"""Data Pump workflow implementation for modern expdp/impdp dumps."""

from __future__ import annotations

import logging
import re
from pathlib import Path

from oracle_dmp_converter.datapump._workflow_base import DumpWorkflow
from oracle_dmp_converter.datapump.modern.parfile import (
    BatchImportJob,
    BulkMetadataImportJob,
    ImportJob,
    MasterOnlyJob,
    SqlFileJob,
)
from oracle_dmp_converter.datapump.modern.runner import DataPumpRunner
from oracle_dmp_converter.datapump.modern.sqlfile import (
    parse_sqlfile_tables,
    parse_sqlfile_tablespaces,
)
from oracle_dmp_converter.models import DumpFormat
from oracle_dmp_converter.oracle.conn import OracleCredentials

LOGGER = logging.getLogger(__name__)

_SQLFILE_NAME = "discovery-impdp-sqlfile.sql"


def _log_name(name: str) -> str:
    """Make *name* safe as a Data Pump log file name.

    Data Pump expands ``$`` in file names as an environment variable, so table
    names like ``MLOG$_ADDRESS`` fail with ORA-07225.
    """
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)


class DataPumpWorkflow(DumpWorkflow):
    """Workflow for modern Oracle Data Pump (expdp/impdp) dump files."""

    def __init__(
        self,
        *,
        credentials: OracleCredentials,
        directory: str,
        directory_path: str,
        dumpfiles: tuple[str, ...],
        discovery_runner: DataPumpRunner,
        discovery_dir: Path,
        inspect_runner: DataPumpRunner,
        convert_runner: DataPumpRunner,
        discovery_directory: str,
        inspect_directory: str,
        convert_directory: str,
    ) -> None:
        """Store all configuration needed to drive modern Data Pump operations.

        Args:
            credentials: Oracle credentials written to the parfile ``USERID``
                field for every ``impdp`` invocation.
            directory: Oracle DIRECTORY object name (e.g. ``"DUMP_DIR"``) for
                the dump files.
            directory_path: Absolute OS path inside the container that
                *directory* maps to.
            dumpfiles: Tuple of dump file base-names (without directory path).
            discovery_runner: :class:`DataPumpRunner` used exclusively for the
                ``SQLFILE=`` discovery invocation; its parfiles are written to
                ``discovery_dir / "parfiles"``.
            discovery_dir: Local directory where discovery artifacts
                (parfiles, ``.log``, ``.sql``) are written.  Created
                automatically if it does not already exist.
            inspect_runner: :class:`DataPumpRunner` used for read-only
                ``CONTENT=METADATA_ONLY`` imports during the inspect phase.
            convert_runner: :class:`DataPumpRunner` used for data-importing
                operations.
            discovery_directory: Oracle DIRECTORY object name that maps to the
                ``work_dir/discovery/`` host path.  Used as the ``LOGFILE=``
                and ``SQLFILE=`` directory for discovery invocations.
            inspect_directory: Oracle DIRECTORY object name that maps to the
                ``work_dir/inspect/`` host path.  Used as the ``LOGFILE=``
                directory for inspect-phase imports.
            convert_directory: Oracle DIRECTORY object name that maps to the
                ``work_dir/convert/`` host path.  Used as the ``LOGFILE=``
                directory for convert-phase imports.
        """
        self._credentials = credentials
        self._directory = directory
        self._directory_path = directory_path.rstrip("/")
        self._dumpfiles = dumpfiles
        self._discovery_runner = discovery_runner
        self._discovery_dir = discovery_dir
        discovery_dir.mkdir(parents=True, exist_ok=True)
        self._inspect_runner = inspect_runner
        self._convert_runner = convert_runner
        self._discovery_directory = discovery_directory
        self._inspect_directory = inspect_directory
        self._convert_directory = convert_directory

    # ------------------------------------------------------------------
    # DumpWorkflow interface
    # ------------------------------------------------------------------

    @property
    def dump_format(self) -> DumpFormat:
        return DumpFormat.DATAPUMP

    def discover_tables(self) -> tuple[tuple[str, str], ...]:
        """Discover schema/table pairs via ``impdp SQLFILE=``.

        Runs a ``SQLFILE=`` job so that Data Pump writes CREATE TABLE DDL
        directly to :attr:`_discovery_dir` on the host (via the Oracle
        DIRECTORY object :attr:`_discovery_directory`), then reads and parses
        that file.  Oracle also writes its own log file to the same directory.
        """
        sqlfile_name = _SQLFILE_NAME
        job = SqlFileJob(
            connection=self._credentials,
            directory=self._directory,
            dumpfiles=self._dumpfiles,
            logfile=f"{self._discovery_directory}:discovery-impdp-sqlfile.log",
            sqlfile=f"{self._discovery_directory}:{sqlfile_name}",
        )
        LOGGER.info("Running impdp SQLFILE discovery (sqlfile=%s)", sqlfile_name)
        self._discovery_runner.run_sqlfile(job)

        sqlfile_path = self._discovery_dir / _SQLFILE_NAME
        sql_text = sqlfile_path.read_text() if sqlfile_path.exists() else ""

        tables = parse_sqlfile_tables(sql_text)
        LOGGER.info("SQLFILE discovery found %d tables", len(tables))
        return tables

    def required_tablespaces(self) -> frozenset[str]:
        """Return custom tablespaces referenced in the SQLFILE DDL.

        Reads the DDL file written by the most recent ``impdp SQLFILE=``
        discovery run (if it exists) and extracts non-system tablespace names.
        Returns an empty :class:`frozenset` when discovery has not yet run.
        """
        sqlfile_path = self._discovery_dir / _SQLFILE_NAME
        if not sqlfile_path.exists():
            return frozenset()
        return parse_sqlfile_tablespaces(sqlfile_path.read_text())

    def import_all_metadata(self, source_schema: str, stage_schema: str) -> None:
        """Import DDL for all tables in *source_schema* via a single ``impdp`` call.

        Uses ``CONTENT=METADATA_ONLY`` and ``TABLE_EXISTS_ACTION=REPLACE`` without
        a ``TABLES=`` restriction so every table in the schema is created at once.
        Partition key columns are skipped during BYTE→CHAR adjustment via
        ``NOT EXISTS`` subqueries in ``_apply_byte_to_char``.
        """
        LOGGER.debug("Bulk importing metadata for %s -> %s", source_schema, stage_schema)
        job = BulkMetadataImportJob(
            connection=self._credentials,
            directory=self._directory,
            dumpfiles=self._dumpfiles,
            logfile=f"{self._inspect_directory}:impdp-bulk-meta-{source_schema}.log"[:200],
            remap_schema=(source_schema, stage_schema),
            schemas=(source_schema,),
        )
        self._inspect_runner.run_bulk_metadata_impdp(job)

    def load_master_table(self, job_name: str) -> None:
        job = MasterOnlyJob(
            connection=self._credentials,
            directory=self._directory,
            dumpfiles=self._dumpfiles,
            logfile=f"{self._inspect_directory}:impdp-master-only.log",
            job_name=job_name,
        )
        self._inspect_runner.run_master_only(job)

    def import_metadata(self, source_schema: str, stage_schema: str, table: str) -> None:
        """Import table DDL only (``CONTENT=METADATA_ONLY``) into the staging schema.

        Uses ``TABLE_EXISTS_ACTION=REPLACE`` so that re-running inspect against
        an already-prepared staging schema re-creates the DDL cleanly.
        """
        LOGGER.debug("Importing metadata for %s.%s -> %s", source_schema, table, stage_schema)
        job = ImportJob(
            connection=self._credentials,
            directory=self._directory,
            dumpfiles=self._dumpfiles,
            logfile=f"{self._inspect_directory}:{_log_name(f'metadata-{source_schema}-{table}.log')}"[
                :200
            ],
            source_schema=source_schema,
            table=table,
            remap_schema=(source_schema, stage_schema),
            content="METADATA_ONLY",
            table_exists_action="REPLACE",
            exclude=("INDEX", "REF_CONSTRAINT", "TRIGGER", "GRANT"),
        )
        self._inspect_runner.run_impdp(job)

    def import_chunk(
        self,
        source_schema: str,
        stage_schema: str,
        table: str,
        chunk_name: str,
        partition_name: str | None,
        subpartition_name: str | None = None,
    ) -> None:
        """Import one chunk of table data into the staging schema.

        Uses ``CONTENT=DATA_ONLY`` and ``TABLE_EXISTS_ACTION=TRUNCATE`` (the
        ``ImportJob`` default) because the staging schema is pre-populated with
        DDL during the inspect phase.

        When *subpartition_name* is set it takes precedence over
        *partition_name* in the ``TABLES=`` filter — impdp accepts a
        subpartition name in the ``schema.table:NAME`` slot because
        subpartition names are unique within a table.
        """
        LOGGER.debug(
            "Importing chunk %s for %s.%s -> %s", chunk_name, source_schema, table, stage_schema
        )
        job = ImportJob(
            connection=self._credentials,
            directory=self._directory,
            dumpfiles=self._dumpfiles,
            logfile=f"{self._convert_directory}:{_log_name(f'impdp-{source_schema}-{table}-{chunk_name}.log')}"[
                :200
            ],
            source_schema=source_schema,
            table=table,
            remap_schema=(source_schema, stage_schema),
            partition_name=subpartition_name or partition_name,
            content="DATA_ONLY",
        )
        self._convert_runner.run_impdp(job)

    def import_chunks_batch(
        self,
        chunks: list[tuple[str, str, str, str, str | None, str | None]],
        queries: tuple[tuple[str, str, str], ...] = (),
    ) -> None:
        """Import multiple chunks in a single ``impdp`` invocation.

        Combines all ``(source_schema, stage_schema, table, chunk_name,
        partition_name, subpartition_name)`` specs into one ``TABLES=`` line
        so Oracle starts a single import process for the entire batch instead
        of one per chunk.  Subpartition names take precedence over partition
        names in the ``TABLES=`` filter.
        """
        if not chunks:
            return
        LOGGER.debug("Batch-importing %d chunks via single impdp call", len(chunks))
        table_specs = tuple(
            (source_schema, table, subpartition_name or partition_name)
            for (
                source_schema,
                _stage_schema,
                table,
                _chunk_name,
                partition_name,
                subpartition_name,
            ) in chunks
        )
        # Deduplicate remap pairs while preserving insertion order.
        seen: dict[str, str] = {}
        for source_schema, stage_schema, _table, _chunk_name, _partition_name, _sub in chunks:
            seen.setdefault(source_schema, stage_schema)
        remap_schemas = tuple(seen.items())
        # Build a short logfile name from the first few table names.
        table_names = [t for _s, _st, t, _c, _p, _sub in chunks[:3]]
        logfile_name = _log_name(f"impdp-batch-{'-'.join(table_names)}.log"[:120])
        job = BatchImportJob(
            connection=self._credentials,
            directory=self._directory,
            dumpfiles=self._dumpfiles,
            logfile=f"{self._convert_directory}:{logfile_name}",
            table_specs=table_specs,
            remap_schemas=remap_schemas,
            queries=queries,
            content="DATA_ONLY",
        )
        self._convert_runner.run_batch_impdp(job)


def make_modern_runners(
    container: object,
    work_dir: Path,
) -> tuple[DataPumpRunner, DataPumpRunner, DataPumpRunner]:
    """Create the discovery, inspect, and convert ``DataPumpRunner`` triple.

    Returns ``(discovery_runner, inspect_runner, convert_runner)``.

    * *discovery_runner* writes parfiles to ``work_dir/discovery/parfiles/``
      and is used exclusively for the ``SQLFILE=`` discovery invocation.
    * *inspect_runner* writes parfiles to ``work_dir/inspect/parfiles/``
      and handles ``CONTENT=METADATA_ONLY`` imports during the inspect phase.
    * *convert_runner* writes parfiles to ``work_dir/convert/parfiles/``
      and handles data-importing operations.
    """
    discovery_runner = DataPumpRunner(  # type: ignore[arg-type]
        container, work_dir / "discovery" / "parfiles", keep_parfiles=True
    )
    inspect_runner = DataPumpRunner(  # type: ignore[arg-type]
        container, work_dir / "inspect" / "parfiles", keep_parfiles=True
    )
    convert_runner = DataPumpRunner(  # type: ignore[arg-type]
        container, work_dir / "convert" / "parfiles", keep_parfiles=True
    )
    return discovery_runner, inspect_runner, convert_runner
