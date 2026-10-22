"""Unit tests for oracle/exporter.py internals."""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oracle_dmp_converter.config import ColumnOverride
from oracle_dmp_converter.models import ColumnMetadata, OutputFormat
from oracle_dmp_converter.oracle import exporter as exporter_module
from oracle_dmp_converter.oracle.exporter import (
    _coerce_value,
    _db_object_to_text,
    _decode_utf8,
    _fetch_number_as_decimal,
    _field_metadata_for,
    _read_lob,
    _rows_to_table,
    arrow_schema_for_columns,
    arrow_type_for_column,
    export_table,
)
from oracle_dmp_converter.oracle.format_writer import AvroFormatWriter


def _col(
    data_type: str,
    precision: int | None = None,
    scale: int | None = None,
    *,
    name: str = "C",
) -> ColumnMetadata:
    return ColumnMetadata(
        name=name,
        data_type=data_type,
        ordinal=1,
        data_precision=precision,
        data_scale=scale,
    )


# ---------------------------------------------------------------------------
# arrow_type_for_column
# ---------------------------------------------------------------------------


class TestArrowTypeForColumn:
    def test_number_integer(self) -> None:
        assert arrow_type_for_column(_col("NUMBER", 10, 0)) == pa.int64()

    def test_number_decimal(self) -> None:
        assert arrow_type_for_column(_col("NUMBER", 20, 4)) == pa.decimal128(20, 4)

    def test_number_decimal256(self) -> None:
        assert arrow_type_for_column(_col("NUMBER", 39, 2)) == pa.decimal256(39, 2)

    def test_number_unconstrained(self) -> None:
        # Unbounded NUMBER now maps to the widest fixed-precision decimal
        # so values past 2^53 round-trip without silent precision loss.
        assert arrow_type_for_column(_col("NUMBER")) == pa.decimal128(38, 0)

    def test_float_maps_to_double(self) -> None:
        assert arrow_type_for_column(_col("BINARY_DOUBLE")) == pa.float64()

    def test_varchar2(self) -> None:
        assert arrow_type_for_column(_col("VARCHAR2")) == pa.string()

    def test_raw(self) -> None:
        assert arrow_type_for_column(_col("RAW")) == pa.binary()

    def test_date(self) -> None:
        assert arrow_type_for_column(_col("DATE")) == pa.timestamp("us")

    def test_timestamp(self) -> None:
        assert arrow_type_for_column(_col("TIMESTAMP")) == pa.timestamp("us")

    def test_override_takes_precedence(self) -> None:
        override = ColumnOverride(parquet_type="string")
        assert arrow_type_for_column(_col("NUMBER", 10, 0), override) == pa.string()

    def test_unknown_token_falls_back_to_string(self) -> None:
        """arrow_type_for_column returns pa.string() for any unrecognised token."""
        with patch(
            "oracle_dmp_converter.oracle.exporter.oracle_to_arrow_token",
            return_value="xyz_unsupported",
        ):
            result = arrow_type_for_column(_col("GEOMETRY"))
        assert result == pa.string()


# ---------------------------------------------------------------------------
# arrow_schema_for_columns
# ---------------------------------------------------------------------------


def test_arrow_schema_for_columns_basic() -> None:
    columns = (
        ColumnMetadata("ID", "NUMBER", 1, data_precision=10, data_scale=0),
        ColumnMetadata("NAME", "VARCHAR2", 2),
    )
    schema = arrow_schema_for_columns(columns)
    assert schema.field("ID").type == pa.int64()
    assert schema.field("NAME").type == pa.string()


def test_arrow_schema_for_columns_with_override() -> None:
    columns = (ColumnMetadata("GEOM", "SDO_GEOMETRY", 1),)
    override = ColumnOverride(parquet_type="string")
    schema = arrow_schema_for_columns(columns, {"GEOM": override})
    assert schema.field("GEOM").type == pa.string()


# ---------------------------------------------------------------------------
# _coerce_value
# ---------------------------------------------------------------------------


class TestCoerceValue:
    def test_null_returns_none(self) -> None:
        assert _coerce_value(None, pa.string()) is None

    def test_bytes_to_string(self) -> None:
        assert _coerce_value(b"hello", pa.string()) == "hello"

    def test_non_string_to_string(self) -> None:
        assert _coerce_value(42, pa.string()) == "42"

    def test_string_to_binary(self) -> None:
        assert _coerce_value("hi", pa.binary()) == b"hi"

    def test_integer_coercion(self) -> None:
        assert _coerce_value(Decimal("7"), pa.int64()) == 7

    def test_float_coercion(self) -> None:
        assert _coerce_value(Decimal("3.14"), pa.float64()) == pytest.approx(3.14)

    def test_decimal_rescaling(self) -> None:
        assert _coerce_value(Decimal("25.9000000000"), pa.decimal128(12, 2)) == Decimal("25.90")

    def test_decimal256_rescaling(self) -> None:
        assert _coerce_value(Decimal("25.9000000000"), pa.decimal256(40, 2)) == Decimal("25.90")

    def test_date_to_datetime(self) -> None:
        result = _coerce_value(date(2024, 1, 15), pa.timestamp("us"))
        assert result == datetime(2024, 1, 15, 0, 0, 0)

    def test_datetime_passthrough(self) -> None:
        dt = datetime(2024, 6, 1, 12, 0, 0)
        assert _coerce_value(dt, pa.timestamp("us")) == dt


# ---------------------------------------------------------------------------
# _rows_to_table
# ---------------------------------------------------------------------------


def test_rows_to_table_basic() -> None:
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    rows = [(1, "Alice"), (2, "Bob")]
    table = _rows_to_table(rows, schema)
    assert table.num_rows == 2
    assert table.column("id").to_pylist() == [1, 2]
    assert table.column("name").to_pylist() == ["Alice", "Bob"]


def test_rows_to_table_empty() -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    table = _rows_to_table([], schema)
    assert table.num_rows == 0


# ---------------------------------------------------------------------------
# export_table — SQL generation (partition_name)
# ---------------------------------------------------------------------------


def _make_mock_conn(executed_sqls: list[str]) -> MagicMock:
    """Return a mock oracledb.Connection that records executed SQL statements."""
    mock_cursor = MagicMock()
    mock_cursor.fetchmany.return_value = []  # empty table → no rows

    @contextmanager
    def _cursor_ctx():
        yield mock_cursor

    mock_conn = MagicMock()
    mock_conn.cursor = _cursor_ctx

    def _capture_execute(sql: str) -> None:
        executed_sqls.append(sql)

    mock_cursor.execute.side_effect = _capture_execute
    return mock_conn


def _simple_columns() -> tuple[ColumnMetadata, ...]:
    return (ColumnMetadata("ID", "NUMBER", 1, data_precision=10, data_scale=0),)


def test_export_table_sql_no_partition(tmp_path: Path) -> None:
    """Without partition_name the FROM clause has no PARTITION token."""
    sqls: list[str] = []
    conn = _make_mock_conn(sqls)
    mock_writer = MagicMock()
    mock_writer.write_empty = MagicMock()
    mock_writer.close = MagicMock()

    with patch("oracle_dmp_converter.oracle.exporter.make_writer", return_value=mock_writer):
        export_table(
            conn,
            schema_name="DMP_FINANCE",
            table_name="TRANSACTIONS",
            columns=_simple_columns(),
            output_path=tmp_path / "out.parquet",
            output_format=OutputFormat.PARQUET,
        )

    assert len(sqls) == 1
    assert "PARTITION" not in sqls[0].upper()
    assert "DMP_FINANCE" in sqls[0]
    assert "TRANSACTIONS" in sqls[0]


def test_export_table_sql_with_partition(tmp_path: Path) -> None:
    """With partition_name the FROM clause includes PARTITION (name)."""
    sqls: list[str] = []
    conn = _make_mock_conn(sqls)
    mock_writer = MagicMock()
    mock_writer.write_empty = MagicMock()
    mock_writer.close = MagicMock()

    with patch("oracle_dmp_converter.oracle.exporter.make_writer", return_value=mock_writer):
        export_table(
            conn,
            schema_name="DMP_FINANCE",
            table_name="TRANSACTIONS",
            columns=_simple_columns(),
            output_path=tmp_path / "out.parquet",
            output_format=OutputFormat.PARQUET,
            partition_name="P_2024_Q1",
        )

    assert len(sqls) == 1
    sql_upper = sqls[0].upper()
    assert "PARTITION" in sql_upper
    assert "P_2024_Q1" in sqls[0]
    # Verify the PARTITION clause appears after the table reference
    from_idx = sql_upper.index("FROM")
    assert sql_upper.index("PARTITION") > from_idx


def test_export_table_sql_with_subpartition(tmp_path: Path) -> None:
    """With subpartition_name the FROM clause uses bare SUBPARTITION (name)."""
    sqls: list[str] = []
    conn = _make_mock_conn(sqls)
    mock_writer = MagicMock()
    mock_writer.write_empty = MagicMock()
    mock_writer.close = MagicMock()

    with patch("oracle_dmp_converter.oracle.exporter.make_writer", return_value=mock_writer):
        export_table(
            conn,
            schema_name="DMP_FINANCE",
            table_name="TXN_DETAILS",
            columns=_simple_columns(),
            output_path=tmp_path / "out.parquet",
            output_format=OutputFormat.PARQUET,
            partition_name="P_2024",
            subpartition_name="P_2024_SP3",
        )

    assert len(sqls) == 1
    sql_upper = sqls[0].upper()
    assert "SUBPARTITION" in sql_upper
    assert "P_2024_SP3" in sqls[0]
    # Bare SUBPARTITION clause: parent PARTITION (P_2024) must not appear.
    assert " PARTITION (" not in sql_upper


# ---------------------------------------------------------------------------
# _read_lob
# ---------------------------------------------------------------------------


def test_read_lob_materialises_lob_object() -> None:
    """_read_lob calls .read() on LOB-like objects."""
    lob = MagicMock()
    lob.read.return_value = "clob content"
    assert _read_lob(lob) == "clob content"
    lob.read.assert_called_once()


def test_read_lob_passes_through_plain_values() -> None:
    """Non-LOB values are returned unchanged."""
    assert _read_lob("plain string") == "plain string"
    assert _read_lob(42) == 42
    assert _read_lob(None) is None


# ---------------------------------------------------------------------------
# _coerce_value — remaining uncovered branches
# ---------------------------------------------------------------------------


def test_coerce_bytes_passthrough_for_binary_type() -> None:
    """bytes values are returned as bytes for binary columns (not str → encode)."""
    assert _coerce_value(b"raw data", pa.binary()) == b"raw data"


def test_coerce_passthrough_for_non_datetime_timestamp() -> None:
    """Integer in timestamp column: neither datetime nor date, falls through."""
    result = _coerce_value(1_000_000, pa.timestamp("us"))
    assert result == 1_000_000


# ---------------------------------------------------------------------------
# export_table — non-empty result path (lines 229-232)
# ---------------------------------------------------------------------------


def test_export_table_calls_write_batch_for_rows(tmp_path: Path) -> None:
    """export_table should call write_batch (not write_empty) when rows exist."""
    mock_cursor = MagicMock()
    mock_cursor.fetchmany.side_effect = [[(1,), (2,)], []]

    @contextmanager
    def _cursor_ctx():
        yield mock_cursor

    mock_conn = MagicMock()
    mock_conn.cursor = _cursor_ctx
    mock_cursor.execute.side_effect = lambda sql: None

    mock_writer = MagicMock()
    with patch("oracle_dmp_converter.oracle.exporter.make_writer", return_value=mock_writer):
        result = export_table(
            mock_conn,
            schema_name="DMP_FINANCE",
            table_name="TRANSACTIONS",
            columns=_simple_columns(),
            output_path=tmp_path / "out.parquet",
            output_format=OutputFormat.PARQUET,
        )

    mock_writer.write_batch.assert_called_once()
    mock_writer.write_empty.assert_not_called()
    assert result.rows == 2


def test_export_table_writes_malformed_numbers_as_null(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A malformed stored NUMBER decodes as -1E+126; it becomes null with a warning."""
    columns = (
        ColumnMetadata("ID", "NUMBER", 1, data_precision=12, data_scale=0),
        ColumnMetadata("AMOUNT", "NUMBER", 2, data_precision=17, data_scale=5),
    )
    bad = Decimal("-1E+126")
    mock_cursor = MagicMock()
    mock_cursor.fetchmany.side_effect = [[(bad, bad), (Decimal(7), Decimal("1.5"))], []]

    @contextmanager
    def _cursor_ctx():
        yield mock_cursor

    mock_conn = MagicMock()
    mock_conn.cursor = _cursor_ctx

    out = tmp_path / "out.parquet"
    with caplog.at_level("WARNING"):
        result = export_table(
            mock_conn,
            schema_name="DMP_SRC",
            table_name="T",
            columns=columns,
            output_path=out,
        )

    assert result.rows == 2
    table = pq.read_table(out)
    assert table.column("ID").to_pylist() == [None, 7]
    assert table.column("AMOUNT").to_pylist() == [None, Decimal("1.50000")]
    assert "DMP_SRC.T.ID: 1 unreadable value(s) written as null" in caplog.text
    assert "DMP_SRC.T.AMOUNT: 1 unreadable" in caplog.text


def test_rows_to_table_writes_undecodable_clob_as_null() -> None:
    """A corrupt CLOB whose read() can't decode UTF-16 becomes null and is counted."""
    lob = MagicMock()
    lob.read.side_effect = UnicodeDecodeError("utf-16-be", b"0", 0, 1, "truncated data")
    schema = pa.schema([pa.field("BODY", pa.string())])
    unreadable: Counter[str] = Counter()

    table = _rows_to_table([(lob,), ("ok",)], schema, unreadable)

    assert table.column("BODY").to_pylist() == [None, "ok"]
    assert unreadable == {"BODY": 1}


# ---------------------------------------------------------------------------
# DbObject serialisation + Arrow field metadata
# ---------------------------------------------------------------------------


class _FakeAttr:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeType:
    def __init__(self, *, iscollection: bool, attr_names: tuple[str, ...] = ()) -> None:
        self.iscollection = iscollection
        self.attributes = tuple(_FakeAttr(n) for n in attr_names)


class _FakeDbObject:
    """Minimal stand-in for ``oracledb.DbObject`` for unit testing."""

    def __init__(self, *, attrs: dict | None = None, items: list | None = None) -> None:
        if items is not None:
            self.type = _FakeType(iscollection=True)
            self._items = items
        else:
            attrs = attrs or {}
            self.type = _FakeType(iscollection=False, attr_names=tuple(attrs.keys()))
            for name, value in attrs.items():
                setattr(self, name, value)

    def aslist(self) -> list:
        return self._items


class TestDbObjectToText:
    def test_object_type_serialises_attrs_as_json(self) -> None:
        # Patch the isinstance check inside _db_object_to_text so it
        # treats our fake as a real DbObject.  Easier than importing
        # the real type, which the test doesn't have a live container for.
        with patch.object(exporter_module, "oracledb") as mock_oracledb:
            mock_oracledb.DbObject = _FakeDbObject
            result = _db_object_to_text(_FakeDbObject(attrs={"street": "1 Main", "zip": "12345"}))
        assert '"street": "1 Main"' in result
        assert '"zip": "12345"' in result

    def test_collection_serialises_as_list(self) -> None:
        with patch.object(exporter_module, "oracledb") as mock_oracledb:
            mock_oracledb.DbObject = _FakeDbObject
            result = _db_object_to_text(_FakeDbObject(items=["alpha", "beta"]))
        assert result == '["alpha", "beta"]'

    def test_nested_object_inside_collection(self) -> None:
        inner = _FakeDbObject(attrs={"k": "v"})
        outer = _FakeDbObject(items=[inner])
        with patch.object(exporter_module, "oracledb") as mock_oracledb:
            mock_oracledb.DbObject = _FakeDbObject
            result = _db_object_to_text(outer)
        assert result == '[{"k": "v"}]'

    def test_datetime_attr_uses_isoformat(self) -> None:
        with patch.object(exporter_module, "oracledb") as mock_oracledb:
            mock_oracledb.DbObject = _FakeDbObject
            result = _db_object_to_text(
                _FakeDbObject(attrs={"created": datetime(2024, 1, 15, 10, 30)})
            )
        assert "2024-01-15T10:30:00" in result

    def test_decimal_attr_becomes_string(self) -> None:
        with patch.object(exporter_module, "oracledb") as mock_oracledb:
            mock_oracledb.DbObject = _FakeDbObject
            result = _db_object_to_text(_FakeDbObject(attrs={"amount": Decimal("12.50")}))
        assert '"amount": "12.50"' in result

    def test_bytes_attr_is_decoded(self) -> None:
        with patch.object(exporter_module, "oracledb") as mock_oracledb:
            mock_oracledb.DbObject = _FakeDbObject
            result = _db_object_to_text(_FakeDbObject(attrs={"blob": b"hello"}))
        assert '"blob": "hello"' in result


# ---------------------------------------------------------------------------
# NUMBER scale edge cases must actually WRITE (regression: negative scale and
# scale>precision used to crash the Parquet writer and the Avro schema parser).
# ---------------------------------------------------------------------------


class TestNumberEdgeScalesRoundTrip:
    @pytest.mark.parametrize(
        ("precision", "scale", "value", "expected"),
        [
            (10, -2, Decimal("12300"), 12300),  # int64
            (20, -2, Decimal("12300"), Decimal("12300")),  # decimal128(22,0)
            (2, 5, Decimal("0.00012"), Decimal("0.00012")),  # decimal128(5,5)
            (5, 7, Decimal("0.0000012"), Decimal("0.0000012")),  # decimal128(7,7)
            (39, 2, Decimal("123.45"), Decimal("123.45")),  # decimal256(39,2)
            (40, 0, Decimal("1234567890"), Decimal("1234567890")),  # decimal256(40,0)
        ],
    )
    def test_parquet_write_and_read_back(
        self,
        tmp_path: Path,
        precision: int,
        scale: int,
        value: Decimal,
        expected: object,
    ) -> None:
        columns = (_col("NUMBER", precision, scale, name="N"),)
        schema = arrow_schema_for_columns(columns)
        table = _rows_to_table([(value,)], schema)
        out = tmp_path / "n.parquet"
        pq.write_table(table, out)  # must not raise
        back = pq.read_table(out)
        assert back.column("N").to_pylist()[0] == expected

    @pytest.mark.parametrize(
        ("precision", "scale", "value"),
        [
            (20, -2, Decimal("12300")),
            (2, 5, Decimal("0.00012")),
        ],
    )
    def test_avro_write_does_not_raise(
        self, tmp_path: Path, precision: int, scale: int, value: Decimal
    ) -> None:
        columns = (_col("NUMBER", precision, scale, name="N"),)
        schema = arrow_schema_for_columns(columns)
        table = _rows_to_table([(value,)], schema)
        writer = AvroFormatWriter(tmp_path / "n.avro", schema)  # parse must not raise
        writer.write_batch(table)  # write must not raise
        writer.close()
        assert (tmp_path / "n.avro").exists()


# ---------------------------------------------------------------------------
# _decode_utf8 — warns (rather than silently corrupting) on invalid bytes
# ---------------------------------------------------------------------------


class TestDecodeUtf8:
    def test_clean_utf8_decodes_without_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level("WARNING"):
            assert _decode_utf8("héllo".encode()) == "héllo"
        assert not caplog.records

    def test_invalid_bytes_warn_and_replace(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level("WARNING"):
            result = _decode_utf8(b"ab\xffcd")
        assert "�" in result
        assert any("Non-UTF-8" in r.message for r in caplog.records)


class TestFieldMetadataFor:
    def test_includes_oracle_data_type(self) -> None:
        col = ColumnMetadata(name="EMAIL", data_type="VARCHAR2", ordinal=1)
        metadata = _field_metadata_for(col)
        assert metadata == {b"oracle_data_type": b"VARCHAR2"}

    def test_includes_comment_when_present(self) -> None:
        col = ColumnMetadata(
            name="EMAIL",
            data_type="VARCHAR2",
            ordinal=1,
            comment="Unique work email",
        )
        metadata = _field_metadata_for(col)
        assert metadata is not None
        assert metadata[b"oracle_comment"] == b"Unique work email"

    def test_arrow_schema_attaches_metadata_to_field(self) -> None:
        col = ColumnMetadata(
            name="EMAIL",
            data_type="VARCHAR2",
            ordinal=1,
            comment="work email",
        )
        schema = arrow_schema_for_columns((col,))
        field = schema.field("EMAIL")
        assert field.metadata is not None
        assert field.metadata.get(b"oracle_comment") == b"work email"


class TestFetchNumberAsDecimal:
    """The output type handler that makes NUMBER columns fetch as Decimal (#8)."""

    def test_number_column_fetches_as_decimal(self) -> None:
        cursor = MagicMock()
        cursor.arraysize = 50
        metadata = MagicMock(type_code=oracledb.DB_TYPE_NUMBER)
        _fetch_number_as_decimal(cursor, metadata)
        cursor.var.assert_called_once_with(Decimal, arraysize=50)

    def test_non_number_column_left_to_default(self) -> None:
        cursor = MagicMock()
        metadata = MagicMock(type_code=oracledb.DB_TYPE_VARCHAR)
        assert _fetch_number_as_decimal(cursor, metadata) is None
        cursor.var.assert_not_called()
