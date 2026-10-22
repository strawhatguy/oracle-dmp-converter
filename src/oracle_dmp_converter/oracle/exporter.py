"""Streaming Oracle table export to pluggable output formats."""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import ROUND_HALF_UP, Decimal, localcontext
from pathlib import Path
from typing import Any

import oracledb
import pyarrow as pa

from oracle_dmp_converter.config import ColumnOverride
from oracle_dmp_converter.models import ColumnMetadata, OutputFormat
from oracle_dmp_converter.oracle.format_writer import FormatWriter, make_writer
from oracle_dmp_converter.oracle.identifiers import oracle_identifier, oracle_qualified_name
from oracle_dmp_converter.oracle.types import export_expression, oracle_to_arrow_token

LOGGER = logging.getLogger(__name__)

# Oracle NUMBER holds magnitudes below 1E126.  A malformed stored NUMBER (seen
# in a 19c dump as a single 0x30 byte, ORA-03090 in Oracle itself) decodes in
# python-oracledb as -1E+126, so anything at or past the bound is not a value.
_NUMBER_LIMIT = Decimal("1E126")


@dataclass(frozen=True)
class ExportResult:
    """Result of a single table export operation.

    Attributes:
        path: Absolute path to the written output file.
        rows: Total number of rows written.
    """

    path: Path
    rows: int


def arrow_type_for_column(
    column: ColumnMetadata,
    override: ColumnOverride | None = None,
) -> pa.DataType:
    """Resolve the Arrow :class:`~pyarrow.DataType` for an Oracle column.

    Delegates to :func:`~oracle_dmp_converter.oracle.types.oracle_to_arrow_token`
    and maps the returned token string to a concrete Arrow type.  Falls back
    to ``pa.string()`` for any unrecognised token.

    Args:
        column: Column metadata from ``ALL_TAB_COLUMNS``.
        override: Optional per-column override; a ``parquet_type`` field
            in the override takes priority over the default mapping.

    Returns:
        The Arrow :class:`~pyarrow.DataType` for the column.
    """
    type_name = oracle_to_arrow_token(column, override)
    if type_name == "int64":
        return pa.int64()
    if type_name == "double":
        return pa.float64()
    if type_name == "string":
        return pa.string()
    if type_name == "binary":
        return pa.binary()
    if type_name == "timestamp_us":
        return pa.timestamp("us")
    if type_name.startswith("decimal128(") and type_name.endswith(")"):
        precision, scale = type_name[len("decimal128(") : -1].split(",", 1)
        return pa.decimal128(int(precision), int(scale))
    if type_name.startswith("decimal256(") and type_name.endswith(")"):
        precision, scale = type_name[len("decimal256(") : -1].split(",", 1)
        return pa.decimal256(int(precision), int(scale))
    return pa.string()


def arrow_schema_for_columns(
    columns: tuple[ColumnMetadata, ...],
    overrides: dict[str, ColumnOverride] | None = None,
) -> pa.Schema:
    """Build a PyArrow :class:`~pyarrow.Schema` for a tuple of Oracle columns.

    Args:
        columns: Ordered column metadata from ``ALL_TAB_COLUMNS``.
        overrides: Optional mapping of column name → override; keys are
            matched case-sensitively against ``column.name``.

    Returns:
        A :class:`~pyarrow.Schema` with one field per column, in ordinal order.
    """
    overrides = overrides or {}
    fields = [
        pa.field(
            column.name,
            arrow_type_for_column(column, overrides.get(column.name)),
            metadata=_field_metadata_for(column),
        )
        for column in columns
    ]
    return pa.schema(fields)


def _field_metadata_for(column: ColumnMetadata) -> dict[bytes, bytes] | None:
    """Return Arrow field metadata for *column*, or ``None`` if there is none.

    Currently exposes:

    - ``oracle_data_type`` — the original Oracle type string.
    - ``oracle_comment`` — the ``ALL_COL_COMMENTS`` comment when present.

    These travel into the Parquet file's column metadata so downstream
    consumers can recover Oracle semantics without re-querying the
    catalog.
    """
    metadata: dict[bytes, bytes] = {
        b"oracle_data_type": column.data_type.encode("utf-8"),
    }
    if column.comment:
        metadata[b"oracle_comment"] = column.comment.encode("utf-8")
    return metadata


def _decode_utf8(value: bytes) -> str:
    """Decode *value* as UTF-8, warning once if invalid bytes are replaced.

    ``errors="replace"`` keeps the export from crashing on non-UTF-8 bytes,
    but it silently substitutes U+FFFD for the offending sequence.  Attempt a
    strict decode first so a clean warning is logged when data is actually
    lost, rather than corrupting the output without a trace.
    """
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        LOGGER.warning(
            "Non-UTF-8 bytes encountered while decoding a string value; "
            "invalid sequences replaced with U+FFFD (data may be lost)"
        )
        return value.decode("utf-8", errors="replace")


def _read_lob(value: Any) -> Any:
    """Read a LOB value into a plain Python object if needed.

    ``oracledb`` returns CLOB/BLOB columns as :class:`oracledb.LOB` objects
    with a ``read()`` method.  This helper calls ``read()`` when present,
    returning the raw bytes/str, and passes any other value through unchanged.

    ``oracledb.DbObject`` values (returned for OBJECT / VARRAY / nested
    table columns) also expose ``read()`` indirectly — but calling it
    raises.  Detect them explicitly and emit a JSON-ish string instead
    of letting them slip through to ``str(value)`` and produce repr
    noise like ``<oracledb.DbObject ADDRESS_T at 0x…>``.

    Args:
        value: Row value as returned by ``oracledb``.

    Returns:
        The materialised value (``str`` or ``bytes`` for LOBs, JSON-ish
        text for DbObject, or the original value unchanged otherwise).
    """
    if isinstance(value, oracledb.DbObject):
        return _db_object_to_text(value)
    if hasattr(value, "read") and callable(value.read):
        return value.read()
    return value


def _db_object_to_text(obj: oracledb.DbObject) -> str:
    """Serialise an :class:`oracledb.DbObject` to JSON-ish text.

    ``oracledb.DbObject`` represents Oracle OBJECT, VARRAY, and nested
    table values.  The driver exposes attribute access for OBJECT types
    (via ``obj.type.attributes``) and iteration for collection types.
    We recurse through whichever structure applies and emit a stable
    JSON-style string so downstream readers see meaningful data instead
    of a Python repr.
    """

    def _walk(node: Any) -> Any:
        if isinstance(node, oracledb.DbObject):
            type_info = node.type
            if type_info.iscollection:
                return [_walk(item) for item in node.aslist()]
            return {attr.name: _walk(getattr(node, attr.name)) for attr in type_info.attributes}
        if isinstance(node, (bytes, bytearray)):
            return _decode_utf8(bytes(node))
        if isinstance(node, (datetime, date)):
            return node.isoformat()
        if isinstance(node, Decimal):
            return str(node)
        return node

    return json.dumps(_walk(obj), default=str, sort_keys=True)


def _coerce_value(value: Any, arrow_type: pa.DataType) -> Any:
    """Coerce a raw Oracle row value to the Python type expected by PyArrow.

    Handles LOB materialisation, string/bytes coercion, integer and float
    casting, ``Decimal`` quantisation for ``decimal128`` columns, and
    ``date`` → ``datetime`` promotion for timestamp columns.

    Args:
        value: Raw value from ``oracledb`` (post-LOB-read via :func:`_read_lob`
               is applied internally).
        arrow_type: Target Arrow type for the column.

    Returns:
        A Python object compatible with ``pa.array([value], type=arrow_type)``,
        or ``None`` if *value* is ``None``.
    """
    value = _read_lob(value)
    if value is None:
        return None
    if pa.types.is_string(arrow_type):
        if isinstance(value, bytes):
            return _decode_utf8(value)
        return str(value)
    if pa.types.is_binary(arrow_type):
        if isinstance(value, str):
            return value.encode("utf-8")
        return bytes(value)
    if pa.types.is_integer(arrow_type):
        return int(value)
    if pa.types.is_floating(arrow_type):
        return float(value)
    if pa.types.is_decimal(arrow_type):
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        scale = arrow_type.scale
        if scale >= 0:
            quantizer = Decimal(1).scaleb(-scale)
            # Widen the Decimal context to at least the Arrow column's
            # precision so values up to NUMBER(38) don't trip
            # InvalidOperation against Python's default 28-digit precision.
            with localcontext() as ctx:
                ctx.prec = max(ctx.prec, arrow_type.precision + 1)
                return decimal_value.quantize(quantizer, rounding=ROUND_HALF_UP)
        return decimal_value
    if pa.types.is_timestamp(arrow_type):
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, time.min)
    return value


def _rows_to_table(
    rows: list[tuple[Any, ...]],
    schema: pa.Schema,
    unreadable: Counter[str] | None = None,
) -> pa.Table:
    """Convert a list of raw Oracle rows to a :class:`~pyarrow.Table`.

    Each column is built as a :class:`~pyarrow.Array` by calling
    :func:`_coerce_value` on every row element so that Oracle-specific types
    (LOBs, ``Decimal``, ``date``) are properly normalised before PyArrow sees
    them.

    Args:
        rows: List of row tuples as returned by ``cursor.fetchmany()``.
        schema: Arrow schema describing the expected columns and types.
        unreadable: Per-column count of malformed NUMBER values, which are
            written as null instead of failing the whole table.

    Returns:
        A :class:`~pyarrow.Table` with *schema* and one row per element in
        *rows*.
    """
    arrays = []
    for index, field in enumerate(schema):
        values = []
        for row in rows:
            value = row[index]
            if isinstance(value, Decimal) and (
                not value.is_finite() or abs(value) >= _NUMBER_LIMIT
            ):
                if unreadable is not None:
                    unreadable[field.name] += 1
                value = None
            values.append(_coerce_value(value, field.type))
        arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def _fetch_number_as_decimal(cursor: oracledb.Cursor, metadata: Any) -> Any:
    """Output type handler making Oracle NUMBER columns fetch as ``Decimal`` (#8).

    By default ``oracledb`` fetches NUMBER as a Python ``float``, which loses
    precision past 2**53 — so a wide value like ``NUMBER(38,-10)`` would already
    be corrupted before :func:`_coerce_value` ever sees it, regardless of the
    ``decimal256`` Arrow type.  Binding the column to ``Decimal`` keeps the full
    precision end to end.  Non-NUMBER columns return ``None`` to fall back to the
    driver's default handling.
    """
    if metadata.type_code is oracledb.DB_TYPE_NUMBER:
        return cursor.var(Decimal, arraysize=cursor.arraysize)
    return None


def export_table(
    conn: oracledb.Connection,
    *,
    schema_name: str,
    table_name: str,
    columns: tuple[ColumnMetadata, ...],
    output_path: Path,
    output_format: OutputFormat = OutputFormat.PARQUET,
    column_overrides: dict[str, ColumnOverride] | None = None,
    batch_size: int = 10_000,
    partition_name: str | None = None,
    subpartition_name: str | None = None,
) -> ExportResult:
    """Export *table_name* from *conn* to *output_path* in *output_format*.

    Rows are streamed in batches of *batch_size* to keep memory bounded.
    An empty output file is always produced even when the table has no rows.

    When *partition_name* is provided, only rows belonging to that Oracle
    partition are exported via a ``PARTITION (name)`` clause in the
    ``FROM`` expression.  This is used by the batch-import path where the
    staging table contains all partitions' rows after a single ``impdp``
    call and each chunk must still produce a partition-scoped output file.

    When *subpartition_name* is provided, a bare ``SUBPARTITION (name)``
    clause is used instead — subpartition names are unique within a table,
    so the parent ``PARTITION (...)`` qualifier is unnecessary.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    column_overrides = column_overrides or {}
    arrow_schema = arrow_schema_for_columns(columns, column_overrides)
    select_list = ", ".join(
        f"{export_expression(column, column_overrides.get(column.name))} "
        f"AS {oracle_identifier(column.name)}"
        for column in columns
    )
    table_ref = oracle_qualified_name(schema_name, table_name)
    if subpartition_name:
        table_ref = f"{table_ref} SUBPARTITION ({oracle_identifier(subpartition_name)})"
    elif partition_name:
        table_ref = f"{table_ref} PARTITION ({oracle_identifier(partition_name)})"
    sql = f"SELECT {select_list} FROM {table_ref}"
    LOGGER.debug("export_table SQL: %s", sql)
    total_rows = 0
    writer: FormatWriter = make_writer(output_format, output_path, arrow_schema)
    has_rows = False
    unreadable: Counter[str] = Counter()
    try:
        with conn.cursor() as cursor:
            cursor.arraysize = batch_size
            # Fetch NUMBER as Decimal so wide values (NUMBER(38,-10),
            # NUMBER(39+) → decimal256) round-trip exactly instead of through
            # a lossy float.  See :func:`_fetch_number_as_decimal`.
            cursor.outputtypehandler = _fetch_number_as_decimal
            cursor.execute(sql)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                table = _rows_to_table(rows, arrow_schema, unreadable)
                writer.write_batch(table)
                has_rows = True
                total_rows += len(rows)
        if not has_rows:
            writer.write_empty(arrow_schema)
    finally:
        writer.close()
    for column, count in unreadable.items():
        LOGGER.warning(
            "%s.%s.%s: %d unreadable NUMBER value(s) written as null (malformed in the dump)",
            schema_name,
            table_name,
            column,
            count,
        )
    return ExportResult(path=output_path, rows=total_rows)
