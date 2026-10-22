"""Data Pump parameter file rendering for modern expdp/impdp operations."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from oracle_dmp_converter.oracle.conn import OracleCredentials
from oracle_dmp_converter.oracle.identifiers import oracle_identifier

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExportJob:
    connection: OracleCredentials
    directory: str
    dumpfile: str
    logfile: str
    full: bool = True
    include_schemas: tuple[str, ...] = ()


@dataclass(frozen=True)
class ImportJob:
    connection: OracleCredentials
    directory: str
    dumpfiles: tuple[str, ...]
    logfile: str
    source_schema: str
    table: str
    remap_schema: tuple[str, str] | None = None
    content: str | None = None
    partition_name: str | None = None
    table_exists_action: str = "TRUNCATE"
    exclude: tuple[str, ...] = field(
        default=("INDEX", "CONSTRAINT", "REF_CONSTRAINT", "TRIGGER", "STATISTICS", "GRANT")
    )
    transform: tuple[str, ...] = field(
        default=("DISABLE_ARCHIVE_LOGGING:Y", "SEGMENT_ATTRIBUTES:N")
    )


@dataclass(frozen=True)
class BatchImportJob:
    """Parameter specification for a single ``impdp`` call that imports multiple tables.

    Each entry in *table_specs* is a ``(source_schema, table, partition_name)`` triple
    where *partition_name* is ``None`` for whole-table imports.  All specs are combined
    into a single ``TABLES=`` line so that Oracle starts one import process for the
    entire batch rather than one per table.

    *remap_schemas* is a tuple of ``(source_schema, stage_schema)`` pairs; one
    ``REMAP_SCHEMA=`` line is written per pair, allowing tables from different source
    schemas to be imported in the same job.
    """

    connection: OracleCredentials
    directory: str
    dumpfiles: tuple[str, ...]
    logfile: str
    # Each entry: (source_schema, table, partition_name_or_None)
    table_specs: tuple[tuple[str, str, str | None], ...]
    remap_schemas: tuple[tuple[str, str], ...] = ()
    # Each entry: (source_schema, table, where_clause) rendered as one QUERY= line.
    queries: tuple[tuple[str, str, str], ...] = ()
    content: str | None = None
    # APPEND rather than TRUNCATE: staging tables are guaranteed empty after
    # inspect's metadata-only import, so APPEND is equivalent to TRUNCATE+load
    # for the happy path.  Critically, APPEND avoids ORA-39120 (table can't be
    # truncated) when a reference-partitioned child table or any other enabled
    # FK references the table being loaded.  Reference partitioning in
    # particular *cannot* be disabled (ORA-14650), so TRUNCATE is permanently
    # blocked for those tables and APPEND is the only workable option.
    table_exists_action: str = "APPEND"
    exclude: tuple[str, ...] = field(
        default=("INDEX", "CONSTRAINT", "REF_CONSTRAINT", "TRIGGER", "STATISTICS", "GRANT")
    )
    transform: tuple[str, ...] = field(
        default=("DISABLE_ARCHIVE_LOGGING:Y", "SEGMENT_ATTRIBUTES:N")
    )


@dataclass(frozen=True)
class BulkMetadataImportJob:
    """Parameter specification for an ``impdp`` call that imports all table DDL
    for one schema without a ``TABLES=`` restriction.

    Used during the inspect phase to load all table metadata in a single Oracle
    tool invocation.  Post-import adjustments (trigger disabling, VPD policy
    dropping, BYTE→CHAR column modification) are applied afterwards via direct
    SQL before any per-chunk data imports begin.
    """

    connection: OracleCredentials
    directory: str
    dumpfiles: tuple[str, ...]
    logfile: str
    remap_schema: tuple[str, str]
    schemas: tuple[str, ...] = ()
    content: str = "METADATA_ONLY"
    table_exists_action: str = "REPLACE"
    exclude: tuple[str, ...] = field(
        default=(
            "INDEX",
            "CONSTRAINT",
            "REF_CONSTRAINT",
            "TRIGGER",
            "STATISTICS",
            "GRANT",
            "USER",
            "TABLESPACE_QUOTA",
            "VIEW",
            "PACKAGE",
            "PACKAGE_BODY",
            "FUNCTION",
            "PROCEDURE",
            "MATERIALIZED_VIEW",
            # MV logs need the PKs excluded above (ORA-12014), and their
            # SYNC_UP_LOG post-table actions name the un-remapped schema (ORA-01435).
            "MATERIALIZED_VIEW_LOG",
            "POST_TABLE_ACTION",
        )
    )
    transform: tuple[str, ...] = field(
        default=("DISABLE_ARCHIVE_LOGGING:Y", "SEGMENT_ATTRIBUTES:N")
    )


@dataclass(frozen=True)
class SqlFileJob:
    connection: OracleCredentials
    directory: str
    dumpfiles: tuple[str, ...]
    logfile: str
    sqlfile: str
    full: bool = True
    include: tuple[str, ...] = ("TABLE",)


def _schema_include_expression(schemas: tuple[str, ...]) -> str:
    quoted = ", ".join(f"'{schema.replace("'", "''")}'" for schema in schemas)
    return f"IN ({quoted})"


def _table_spec(schema: str, table: str, partition_name: str | None = None) -> str:
    spec = f"{oracle_identifier(schema)}.{oracle_identifier(table)}"
    if partition_name:
        spec += f":{oracle_identifier(partition_name)}"
    return spec


def render_export_parfile(job: ExportJob) -> str:
    lines = [
        f"USERID={job.connection.userid}",
        f"DIRECTORY={job.directory}",
        f"DUMPFILE={job.dumpfile}",
        f"LOGFILE={job.logfile}",
    ]
    if job.full:
        lines.append("FULL=Y")
    if job.include_schemas:
        lines.append(f'INCLUDE=SCHEMA:"{_schema_include_expression(job.include_schemas)}"')
    return "\n".join(lines) + "\n"


def render_import_parfile(job: ImportJob) -> str:
    lines = [
        f"USERID={job.connection.userid}",
        f"DIRECTORY={job.directory}",
        f"DUMPFILE={','.join(job.dumpfiles)}",
        f"LOGFILE={job.logfile}",
        f"TABLES={_table_spec(job.source_schema, job.table, job.partition_name)}",
        f"TABLE_EXISTS_ACTION={job.table_exists_action}",
    ]
    for transform in job.transform:
        lines.append(f"TRANSFORM={transform}")
    if job.content:
        lines.append(f"CONTENT={job.content}")
    if job.remap_schema:
        source, target = job.remap_schema
        lines.append(f"REMAP_SCHEMA={oracle_identifier(source)}:{oracle_identifier(target)}")
    for object_type in job.exclude:
        lines.append(f"EXCLUDE={object_type}")
    return "\n".join(lines) + "\n"


def render_batch_import_parfile(job: BatchImportJob) -> str:
    tables_value = ", ".join(
        _table_spec(schema, table, partition) for schema, table, partition in job.table_specs
    )
    lines = [
        f"USERID={job.connection.userid}",
        f"DIRECTORY={job.directory}",
        f"DUMPFILE={','.join(job.dumpfiles)}",
        f"LOGFILE={job.logfile}",
        f"TABLES={tables_value}",
        f"TABLE_EXISTS_ACTION={job.table_exists_action}",
    ]
    for transform in job.transform:
        lines.append(f"TRANSFORM={transform}")
    if job.content:
        lines.append(f"CONTENT={job.content}")
    for source, target in job.remap_schemas:
        lines.append(f"REMAP_SCHEMA={oracle_identifier(source)}:{oracle_identifier(target)}")
    for schema, table, where in job.queries:
        lines.append(f'QUERY={_table_spec(schema, table)}:"{where}"')
    for object_type in job.exclude:
        lines.append(f"EXCLUDE={object_type}")
    return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class MasterOnlyJob:
    """``impdp MASTER_ONLY=YES``: load the dump's master table as ``SYSTEM.<job_name>``.

    The master table records every object in the dump, including each table's
    data size (``DUMP_LENGTH``) and row count, without importing anything else.
    """

    connection: OracleCredentials
    directory: str
    dumpfiles: tuple[str, ...]
    logfile: str
    job_name: str


def render_master_only_parfile(job: MasterOnlyJob) -> str:
    lines = [
        f"USERID={job.connection.userid}",
        f"DIRECTORY={job.directory}",
        f"DUMPFILE={','.join(job.dumpfiles)}",
        f"LOGFILE={job.logfile}",
        "MASTER_ONLY=YES",
        f"JOB_NAME={job.job_name}",
    ]
    return "\n".join(lines) + "\n"


def render_sqlfile_parfile(job: SqlFileJob) -> str:
    lines = [
        f"USERID={job.connection.userid}",
        f"DIRECTORY={job.directory}",
        f"DUMPFILE={','.join(job.dumpfiles)}",
        f"LOGFILE={job.logfile}",
        f"SQLFILE={job.sqlfile}",
    ]
    if job.full:
        lines.append("FULL=Y")
    for object_type in job.include:
        lines.append(f"INCLUDE={object_type}")
    return "\n".join(lines) + "\n"


def render_bulk_metadata_import_parfile(job: BulkMetadataImportJob) -> str:
    """Render a parameter file for a schema-wide ``CONTENT=METADATA_ONLY`` import.

    Unlike :func:`render_import_parfile`, no ``TABLES=`` line is emitted so
    that ``impdp`` imports DDL for all tables in the source schema at once.
    """
    source, target = job.remap_schema
    lines = [
        f"USERID={job.connection.userid}",
        f"DIRECTORY={job.directory}",
        f"DUMPFILE={','.join(job.dumpfiles)}",
        f"LOGFILE={job.logfile}",
        f"TABLE_EXISTS_ACTION={job.table_exists_action}",
        f"CONTENT={job.content}",
        f"REMAP_SCHEMA={oracle_identifier(source)}:{oracle_identifier(target)}",
    ]
    if job.schemas:
        lines.append(f"SCHEMAS={','.join(oracle_identifier(s) for s in job.schemas)}")
    for transform in job.transform:
        lines.append(f"TRANSFORM={transform}")
    for object_type in job.exclude:
        lines.append(f"EXCLUDE={object_type}")
    return "\n".join(lines) + "\n"
