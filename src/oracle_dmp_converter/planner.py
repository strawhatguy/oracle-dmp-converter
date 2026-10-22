"""Conversion planning for staged Oracle imports."""

from __future__ import annotations

import logging
import math
from collections.abc import Iterable
from dataclasses import replace

from oracle_dmp_converter.config import ConverterConfig, table_override
from oracle_dmp_converter.models import (
    ChunkPlan,
    ColumnMetadata,
    DumpFormat,
    TableMetadata,
    TablePlan,
    TableStrategy,
)
from oracle_dmp_converter.oracle.identifiers import oracle_identifier
from oracle_dmp_converter.oracle.types import UNSUPPORTED_COLUMN_TYPES

LOGGER = logging.getLogger(__name__)

# Oracle Free caps user data at 12 GB.  Dump bytes load at roughly 1-1.2x in
# the staging database, so keep each import batch to 9 GiB of dump data and
# split bigger tables into ~6 GiB hash slices.
MAX_BATCH_BYTES = 9 * 1024**3
SLICE_TARGET_BYTES = 6 * 1024**3

# Types ORA_HASH accepts and that render without quoting or conversion.
_HASHABLE_TYPES = frozenset(
    {
        "NUMBER",
        "FLOAT",
        "BINARY_FLOAT",
        "BINARY_DOUBLE",
        "VARCHAR2",
        "NVARCHAR2",
        "CHAR",
        "NCHAR",
        "DATE",
        "RAW",
    }  # fmt: skip
)


def _slice_column(table: TableMetadata) -> str | None:
    """Return the column to hash rows into slices on, or ``None`` if none fits.

    ponytail: first hashable column, usually the ID.  Slices are lopsided if
    that column has few distinct values; pick the column by config if that bites.
    """
    for column in sorted(table.columns, key=lambda c: c.ordinal):
        if column.hidden or column.normalized_type not in _HASHABLE_TYPES:
            continue
        # QUERY= wraps the clause in double quotes, so only bare identifiers fit.
        if oracle_identifier(column.name) == column.name:
            return column.name
    return None


def _slice_plan(plan: TablePlan, table: TableMetadata) -> TablePlan:
    """Split an oversized whole-table plan into hash slices, if possible."""
    size = table.estimated_bytes or 0
    if size <= MAX_BATCH_BYTES:
        return plan
    column = _slice_column(table)
    if column is None:
        return replace(
            plan,
            warnings=(
                *plan.warnings,
                f"{size / 1024**3:.1f} GiB exceeds the staging limit but has no column to slice on",
            ),
        )
    count = math.ceil(size / SLICE_TARGET_BYTES)
    LOGGER.info(
        "%s.%s: %.1f GiB, splitting into %d slices on %s",
        table.schema,
        table.name,
        size / 1024**3,
        count,
        column,
    )
    # NVL keeps rows whose column is NULL (ORA_HASH(NULL) is NULL) in slice 1.
    chunks = tuple(
        ChunkPlan(
            name=f"slice-{k + 1:03d}-of-{count:03d}",
            strategy=TableStrategy.SLICE,
            where=f"WHERE NVL(ORA_HASH({column}, {count - 1}), 0) = {k}",
        )
        for k in range(count)
    )
    return replace(plan, strategy=TableStrategy.SLICE, chunks=chunks)


# Oracle reports owner-qualified type names for some built-in types that
# the converter handles natively (XMLTYPE is owned by PUBLIC, SDO_GEOMETRY
# by MDSYS).  These should *not* trigger the "user-defined type" path.
_BUILTIN_OWNED_TYPES = frozenset({"XMLTYPE", "SDO_GEOMETRY"})


def _unsupported_column_reason(column: ColumnMetadata) -> str | None:
    """Return a human-readable reason if *column* can't be safely exported.

    ``None`` means the column is fine.  Otherwise the returned string is
    suitable for use as ``TablePlan.reason``.
    """
    normalized = column.normalized_type
    if normalized in UNSUPPORTED_COLUMN_TYPES:
        return f"column {column.name!r} has unsupported type {normalized}"
    if normalized in _BUILTIN_OWNED_TYPES:
        # Owner-qualified built-ins (PUBLIC.XMLTYPE, MDSYS.SDO_GEOMETRY)
        # have ``data_type_owner`` set but are handled natively via
        # type-specific export expressions — let them through.
        return None
    # Genuine user-defined OBJECT / VARRAY / nested-table columns carry a
    # ``data_type_owner`` value pointing at the type's owning schema.
    # The converter cannot meaningfully serialise these via a normal
    # SELECT — oracledb returns DbObject handles that ``str()`` to repr
    # noise.  Mark the whole table UNSUPPORTED rather than emit garbage.
    if column.data_type_owner:
        return (
            f"column {column.name!r} has user-defined type "
            f"{column.data_type_owner}.{column.data_type}"
        )
    return None


def _table_unsupported_reason(table: TableMetadata) -> str | None:
    """Return a human-readable reason if *table* can't be exported at all.

    External tables: the LOCATION file isn't bind-mounted into the
    staging container, so the staged SELECT would raise KUP-04040.
    Global temporary tables: rows are session-scoped and never persist
    through Data Pump export/import, so the result would always be 0.
    """
    if table.table_type == "EXTERNAL":
        return "external table — LOCATION file not available in staging container"
    if table.table_type == "GTT":
        return (
            "global temporary table — data is session-scoped and does not "
            "round-trip through Data Pump export"
        )
    for column in table.columns:
        reason = _unsupported_column_reason(column)
        if reason is not None:
            return reason
    return None


def plan_table(
    table: TableMetadata,
    config: ConverterConfig,
    dump_format: DumpFormat = DumpFormat.DATAPUMP,
) -> TablePlan:
    """Build a :class:`TablePlan` for a single Oracle table.

    Strategy selection follows this priority order:

    1. If the config contains an explicit ``"whole"`` strategy override, return
       a single whole-table chunk regardless of partitions.
    2. If the config contains any other strategy override, return
       ``UNSUPPORTED`` with a descriptive reason (only ``"whole"`` is
       implemented; ``"range"`` and hash are handled elsewhere).
    3. If the table has no partitions, return a whole-table chunk.
    4. Otherwise return one ``PARTITION`` chunk per partition (or per
       subpartition when the partition is composite).  Applies to both
       modern and legacy dumps — legacy ``imp`` accepts partition and
       subpartition names in its ``TABLES=`` parameter via the
       ``schema.table:NAME`` syntax.

    Args:
        table: Metadata for the table being planned.
        config: Active converter configuration, used to look up any per-table
            override.
        dump_format: Format of the source dump.  Partition strategy is
            independent of it; only modern dumps slice oversized tables.

    Returns:
        A :class:`TablePlan` with an appropriate strategy and chunk list.
    """
    override = table_override(config, table.schema, table.name)

    unsupported = _table_unsupported_reason(table)
    if unsupported is not None:
        LOGGER.debug("%s.%s: strategy=unsupported (%s)", table.schema, table.name, unsupported)
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.UNSUPPORTED,
            reason=unsupported,
        )

    if override and override.strategy == "whole":
        LOGGER.debug("%s.%s: strategy=whole (config override)", table.schema, table.name)
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.WHOLE_TABLE,
            chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
        )

    if override and override.strategy is not None:
        LOGGER.debug(
            "%s.%s: strategy=unsupported (unrecognised config override %r)",
            table.schema,
            table.name,
            override.strategy,
        )
        return TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.UNSUPPORTED,
            reason=(
                f"strategy {override.strategy!r} is not supported; use 'whole' to override "
                "the default strategy for this table"
            ),
        )

    if not table.partitions:
        LOGGER.debug("%s.%s: strategy=whole (no partitions)", table.schema, table.name)
        plan = TablePlan(
            schema=table.schema,
            table=table.name,
            strategy=TableStrategy.WHOLE_TABLE,
            chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
        )
        if dump_format is DumpFormat.LEGACY:  # legacy imp has no QUERY= to slice with
            return plan
        return _slice_plan(plan, table)

    LOGGER.debug(
        "%s.%s: strategy=partition (%d partitions)", table.schema, table.name, len(table.partitions)
    )
    chunks: list[ChunkPlan] = []
    for partition in table.partitions:
        if partition.subpartitions:
            # Composite-partitioned table: emit one chunk per physical
            # subpartition so import/export operations match the underlying
            # storage granularity.
            for sub in partition.subpartitions:
                chunks.append(
                    ChunkPlan(
                        name=(
                            f"subpartition-{partition.position:05d}-{sub.position:05d}"
                            f"-{partition.name}-{sub.name}"
                        ),
                        strategy=TableStrategy.PARTITION,
                        partition_name=partition.name,
                        subpartition_name=sub.name,
                    )
                )
        else:
            chunks.append(
                ChunkPlan(
                    name=f"partition-{partition.position:05d}-{partition.name}",
                    strategy=TableStrategy.PARTITION,
                    partition_name=partition.name,
                )
            )
    return TablePlan(
        schema=table.schema,
        table=table.name,
        strategy=TableStrategy.PARTITION,
        chunks=tuple(chunks),
    )


def plan_tables(
    tables: Iterable[TableMetadata],
    config: ConverterConfig,
    dump_format: DumpFormat = DumpFormat.DATAPUMP,
) -> tuple[TablePlan, ...]:
    """Build a :class:`TablePlan` for every table in an iterable.

    Delegates to :func:`plan_table` for each element and collects the results
    into an immutable tuple.

    Args:
        tables: Iterable of :class:`TableMetadata` instances to plan.
        config: Active converter configuration.
        dump_format: Format of the source dump file.

    Returns:
        Tuple of :class:`TablePlan` instances in the same order as *tables*.
    """
    plans: list[TablePlan] = []
    for table in tables:
        plan = plan_table(table, config, dump_format)
        if table.estimated_bytes:
            # convert_plan packs import batches by this size.
            plan = replace(plan, extra={**plan.extra, "estimated_bytes": table.estimated_bytes})
        plans.append(plan)
    return tuple(plans)
