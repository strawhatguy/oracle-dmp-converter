"""Workflow implementation for legacy Oracle exp/imp dump files."""

from __future__ import annotations

import logging
import re
from pathlib import Path

from oracle_dmp_converter.datapump._workflow_base import DumpWorkflow
from oracle_dmp_converter.datapump.legacy.imp_show import (
    parse_imp_indexfile_tables,
    parse_imp_indexfile_tablespaces,
)
from oracle_dmp_converter.datapump.legacy.parfile import (
    LegacyImportJob,
    LegacyIndexFileJob,
)
from oracle_dmp_converter.datapump.legacy.runner import LegacyRunner
from oracle_dmp_converter.errors import DataPumpError
from oracle_dmp_converter.models import DumpFormat
from oracle_dmp_converter.oracle.conn import OracleCredentials
from oracle_dmp_converter.oracle.identifiers import oracle_identifier

LOGGER = logging.getLogger(__name__)

# Error codes that imp may emit but that do not indicate a fatal failure.
# IMP-00003: ORACLE error encountered
# IMP-00017: statement failed with ORACLE error (object already exists, etc.)
# IMP-00041: Warning: object created with compilation warnings
# IMP-00098: ORACLE error (informational, e.g. privilege issue on non-critical object)
# IMP-00403: Warning: object already exists
# ORA-00942: table or view does not exist (object skipped by imp)
# ORA-01435: user does not exist
# ORA-04043: object does not exist
# ORA-04080: trigger does not exist
# ORA-23308: materialized view does not exist
_NON_FATAL_IMP_CODES: frozenset[str] = frozenset(
    {
        "IMP-00003",
        "IMP-00017",
        "IMP-00041",
        "IMP-00098",
        "IMP-00403",
        "ORA-00942",
        "ORA-01435",
        "ORA-04043",
        "ORA-04080",
        "ORA-23308",
    }
)


def _classify_imp_errors(output: str) -> tuple[bool, frozenset[str], frozenset[str]]:
    """Classify IMP/ORA error codes found in *output*.

    Returns ``(has_codes, known_nonfatal, unknown)`` where:

    * *has_codes* is ``True`` when at least one ``IMP-NNNNN`` or
      ``ORA-NNNNN`` token was found in *output*.
    * *known_nonfatal* is the subset of found codes present in
      :data:`_NON_FATAL_IMP_CODES`.
    * *unknown* is the subset of found codes **not** in the known set.

    When *has_codes* is ``False`` the caller should propagate the error
    (no recognisable codes means an unexpected failure).  When
    *has_codes* is ``True`` the caller swallows the error and adjusts the
    log level: ``INFO`` when all codes are known non-fatal, ``WARNING``
    when any unknown code is present.
    """
    found = frozenset(re.findall(r"(IMP-\d+|ORA-\d+)", output))
    if not found:
        return False, frozenset(), frozenset()
    known = found & _NON_FATAL_IMP_CODES
    unknown = found - _NON_FATAL_IMP_CODES
    return True, known, unknown


_INDEXFILE_NAME = "dmpconverter-legacy-discovery.sql"
# Write the indexfile straight into the rw-mounted work-dir discovery directory
# (host: ``<work_dir>/discovery``, container: ``/work/discovery``).  This avoids
# the historical ``/tmp`` fallback dance: imp's output is immediately visible
# to the host without an extra ``docker cp`` / ``cat`` round-trip.
_INDEXFILE_REMOTE = f"/work/discovery/{_INDEXFILE_NAME}"
_DISCOVERY_LOG = "dmpconverter-legacy-discovery.log"


def _legacy_table_spec(table: str, qualifier: str | None = None) -> str:
    """Render a legacy ``imp`` ``TABLES=`` entry, quoting as Oracle requires.

    Legacy ``imp`` upper-cases unquoted identifiers just like SQL, so a
    mixed-case, reserved-word, or special-character table / partition name
    must be double-quoted to match what is stored in the dump — otherwise
    ``imp`` looks for the upper-cased name, finds nothing, and silently
    imports zero rows.  This mirrors the modern Data Pump path's
    ``_table_spec`` (``datapump/modern/parfile.py``), which already quotes
    every component via :func:`oracle_identifier`.
    """
    spec = oracle_identifier(table)
    if qualifier:
        spec += f":{oracle_identifier(qualifier)}"
    return spec


class LegacyDumpWorkflow(DumpWorkflow):
    """Workflow for legacy Oracle exp/imp dump files.

    ``imp INDEXFILE=`` is run lazily on first access and the resulting SQL
    text is cached so that :meth:`discover_tables` and
    :meth:`required_tablespaces` share a single round-trip to the container.
    """

    def __init__(
        self,
        *,
        credentials: OracleCredentials,
        directory_path: str,
        dumpfiles: tuple[str, ...],
        discovery_runner: LegacyRunner,
        discovery_dir: Path,
        inspect_runner: LegacyRunner,
        convert_runner: LegacyRunner,
    ) -> None:
        """Store all configuration needed to drive legacy ``imp`` operations.

        Args:
            credentials: Oracle credentials written to the parfile ``USERID``
                field for every ``imp`` invocation.
            directory_path: Absolute OS path inside the container where the
                dump files reside.
            dumpfiles: Tuple of dump file base-names (without the directory
                path prefix).
            discovery_runner: :class:`LegacyRunner` used exclusively for the
                ``INDEXFILE=`` discovery invocation; its parfiles are written
                to ``discovery_dir / "parfiles"``.
            discovery_dir: Local directory where discovery artifacts
                (parfiles, ``.log``, ``.sql``) are written.  Created
                automatically if it does not already exist.
            inspect_runner: :class:`LegacyRunner` used for read-only
                ``ROWS=N`` metadata imports during the inspect phase.
            convert_runner: :class:`LegacyRunner` used for data-importing
                operations (row imports).
        """
        self._credentials = credentials
        self._directory_path = directory_path.rstrip("/")
        self._dumpfiles = dumpfiles
        self._discovery_runner = discovery_runner
        self._discovery_dir = discovery_dir
        discovery_dir.mkdir(parents=True, exist_ok=True)
        self._inspect_runner = inspect_runner
        self._convert_runner = convert_runner
        # Cached after the first call to _indexfile_sql().
        self._cached_sql: str | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _legacy_files(self) -> tuple[str, ...]:
        """Absolute paths to dump files inside the container."""
        return tuple(f"{self._directory_path}/{f}" for f in self._dumpfiles)

    def _indexfile_sql(self) -> str:
        """Run ``imp INDEXFILE=`` once and cache the DDL text.

        The indexfile is written into the rw-mounted work-dir discovery
        directory (``/work/discovery`` inside the container,
        ``<work_dir>/discovery`` on the host), so the SQL text is readable
        without any extra container round-trip.  If the runner returns an
        empty string we raise :class:`DataPumpError` rather than falling
        back to a ``cat`` shell-out, because an empty indexfile always
        indicates a real failure (missing dump, wrong credentials, imp
        bailed out) that the caller needs to see.

        Saves ``discovery-imp-indexfile.log`` (subprocess stdout+stderr) and
        ``discovery-imp-indexfile.sql`` (the indexfile DDL) into
        :attr:`_discovery_dir`.
        """
        if self._cached_sql is not None:
            return self._cached_sql

        job = LegacyIndexFileJob(
            connection=self._credentials,
            files=self._legacy_files(),
            logfile=_DISCOVERY_LOG,
            indexfile=_INDEXFILE_REMOTE,
            full=True,
        )
        LOGGER.info(
            "Running imp INDEXFILE discovery (files: %s, indexfile=%s)",
            ", ".join(self._dumpfiles),
            _INDEXFILE_REMOTE,
        )
        sql_text, log_output = self._discovery_runner.run_imp_indexfile(job)

        if not sql_text:
            (self._discovery_dir / "discovery-imp-indexfile.log").write_text(log_output)
            raise DataPumpError(
                "imp INDEXFILE discovery produced no SQL output. "
                f"See {self._discovery_dir / 'discovery-imp-indexfile.log'} for details.\n"
                + log_output
            )

        LOGGER.info("imp INDEXFILE discovery complete (%d chars of DDL)", len(sql_text))

        (self._discovery_dir / "discovery-imp-indexfile.log").write_text(log_output)
        (self._discovery_dir / "discovery-imp-indexfile.sql").write_text(sql_text)

        self._cached_sql = sql_text
        return self._cached_sql

    # ------------------------------------------------------------------
    # DumpWorkflow interface
    # ------------------------------------------------------------------

    @property
    def dump_format(self) -> DumpFormat:
        return DumpFormat.LEGACY

    def discover_tables(self) -> tuple[tuple[str, str], ...]:
        """Discover schema/table pairs via ``imp INDEXFILE=``."""
        return parse_imp_indexfile_tables(self._indexfile_sql())

    def required_tablespaces(self) -> frozenset[str]:
        """Return custom tablespaces referenced in the dump DDL.

        These must be pre-created in the staging Oracle instance before
        ``imp`` can land tables that reference them.
        """
        return parse_imp_indexfile_tablespaces(self._indexfile_sql())

    def import_all_metadata(self, source_schema: str, stage_schema: str) -> None:
        """Import DDL for all tables in *source_schema* via a single ``imp ROWS=N`` call.

        Omitting ``tables`` from the job causes the parfile renderer to emit no
        ``TABLES=`` line, so ``imp`` imports metadata for every table in the schema
        at once.
        """
        LOGGER.debug("Bulk importing legacy metadata for %s -> %s", source_schema, stage_schema)
        job = LegacyImportJob(
            connection=self._credentials,
            files=self._legacy_files(),
            logfile=f"imp-bulk-meta-{source_schema}.log"[:120],
            fromuser=source_schema,
            touser=stage_schema,
            tables=(),
            rows=False,
            indexes=False,
            grants=False,
            constraints=False,
        )
        try:
            self._inspect_runner.run_imp(job)
        except DataPumpError as exc:
            has_codes, known, unknown = _classify_imp_errors(str(exc))
            if has_codes:
                if unknown:
                    LOGGER.warning(
                        "Legacy bulk metadata import for %s completed with errors "
                        "containing unknown IMP/ORA codes %s (known non-fatal: %s): %s",
                        source_schema,
                        sorted(unknown),
                        sorted(known),
                        str(exc)[:400],
                    )
                else:
                    LOGGER.info(
                        "Legacy bulk metadata import for %s completed with non-fatal errors "
                        "(IMP/ORA codes in output are all known non-fatal): %s",
                        source_schema,
                        str(exc)[:400],
                    )
                return
            raise

    def import_metadata(self, source_schema: str, stage_schema: str, table: str) -> None:
        """Import table DDL only (``rows=False``) into the staging schema."""
        LOGGER.debug(
            "Importing legacy metadata for %s.%s -> %s", source_schema, table, stage_schema
        )
        job = LegacyImportJob(
            connection=self._credentials,
            files=self._legacy_files(),
            logfile=f"imp-meta-{source_schema}-{table}.log"[:120],
            fromuser=source_schema,
            touser=stage_schema,
            tables=(_legacy_table_spec(table),),
            rows=False,
            indexes=False,
            grants=False,
            constraints=False,
        )
        try:
            self._inspect_runner.run_imp(job)
        except DataPumpError as exc:
            has_codes, known, unknown = _classify_imp_errors(str(exc))
            if has_codes:
                if unknown:
                    LOGGER.warning(
                        "Legacy metadata import for %s.%s completed with errors "
                        "containing unknown IMP/ORA codes %s (known non-fatal: %s): %s",
                        source_schema,
                        table,
                        sorted(unknown),
                        sorted(known),
                        str(exc)[:400],
                    )
                else:
                    LOGGER.info(
                        "Legacy metadata import for %s.%s completed with non-fatal errors "
                        "(IMP/ORA codes in output are all known non-fatal): %s",
                        source_schema,
                        table,
                        str(exc)[:400],
                    )
                return
            raise

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

        Legacy ``imp`` does not support ``QUERY=`` (so arbitrary WHERE-filter
        chunking is impossible), but it *does* accept partition and
        subpartition names directly in the ``TABLES=`` parameter via the
        ``schema.table:NAME`` syntax — both partition and subpartition names
        are valid in the ``:NAME`` slot since subpartition names are unique
        within a table.

        When *subpartition_name* is set it takes precedence over
        *partition_name*; when neither is set the whole table is imported.
        """
        qualifier = subpartition_name or partition_name
        table_spec = _legacy_table_spec(table, qualifier)
        LOGGER.debug(
            "Importing legacy chunk %s for %s.%s%s -> %s",
            chunk_name,
            source_schema,
            table,
            f":{qualifier}" if qualifier else "",
            stage_schema,
        )
        job = LegacyImportJob(
            connection=self._credentials,
            files=self._legacy_files(),
            logfile=f"imp-{source_schema}-{table}-{chunk_name}.log"[:120],
            fromuser=source_schema,
            touser=stage_schema,
            tables=(table_spec,),
            rows=True,
            indexes=False,
            grants=False,
            constraints=False,
            data_only=True,
        )
        self._convert_runner.run_imp(job)

    def import_chunks_batch(
        self,
        chunks: list[tuple[str, str, str, str, str | None, str | None]],
        queries: tuple[tuple[str, str, str], ...] = (),
    ) -> None:
        """Import multiple tables using as few ``imp`` invocations as possible.

        Legacy ``imp`` only supports a single ``FROMUSER``/``TOUSER`` pair per
        invocation, so cross-schema batching is not possible.  Chunks are
        grouped by ``(source_schema, stage_schema)`` and one ``imp`` call is
        issued per distinct schema pair, with each chunk's table+qualifier
        combined into the ``TABLES=`` list (``schema.table:NAME`` style,
        where NAME is a partition or subpartition name).
        """
        if queries:
            raise NotImplementedError("legacy imp cannot filter rows")
        if not chunks:
            return
        # Group table specs by (source_schema, stage_schema), preserving order.
        # Each spec is "TABLE" or "TABLE:qualifier" for partition/subpartition
        # filtering.  Subpartition takes precedence over partition.
        schema_groups: dict[tuple[str, str], list[str]] = {}
        for (
            source_schema,
            stage_schema,
            table,
            _chunk_name,
            partition_name,
            subpartition_name,
        ) in chunks:
            qualifier = subpartition_name or partition_name
            spec = _legacy_table_spec(table, qualifier)
            key = (source_schema, stage_schema)
            schema_groups.setdefault(key, []).append(spec)

        for (source_schema, stage_schema), specs in schema_groups.items():
            unique_specs = tuple(dict.fromkeys(specs))
            LOGGER.debug(
                "Batch-importing %d legacy table-spec(s) for %s -> %s via single imp call",
                len(unique_specs),
                source_schema,
                stage_schema,
            )
            # Use just the table portion of each spec for the log filename.
            short_names = [s.split(":", 1)[0] for s in unique_specs[:3]]
            logfile = f"imp-batch-{source_schema}-{'-'.join(short_names)}.log"[:120]
            job = LegacyImportJob(
                connection=self._credentials,
                files=self._legacy_files(),
                logfile=logfile,
                fromuser=source_schema,
                touser=stage_schema,
                tables=unique_specs,
                rows=True,
                indexes=False,
                grants=False,
                constraints=False,
                data_only=True,
            )
            self._convert_runner.run_imp(job)


def make_legacy_runners(
    container: object,
    work_dir: Path,
) -> tuple[LegacyRunner, LegacyRunner, LegacyRunner]:
    """Create the discovery, inspect, and convert ``LegacyRunner`` triple.

    Returns ``(discovery_runner, inspect_runner, convert_runner)``.

    * *discovery_runner* writes parfiles to ``work_dir/discovery/parfiles/``
      and is used exclusively for the ``INDEXFILE=`` discovery invocation.
    * *inspect_runner* writes parfiles to ``work_dir/inspect/parfiles/``
      and handles ``ROWS=N`` metadata imports during the inspect phase.
    * *convert_runner* writes parfiles to ``work_dir/convert/parfiles/``
      and handles data-importing operations.
    """
    discovery_runner = LegacyRunner(  # type: ignore[arg-type]
        container, work_dir / "discovery" / "parfiles", keep_parfiles=True
    )
    inspect_runner = LegacyRunner(  # type: ignore[arg-type]
        container, work_dir / "inspect" / "parfiles", keep_parfiles=True
    )
    convert_runner = LegacyRunner(  # type: ignore[arg-type]
        container, work_dir / "convert" / "parfiles", keep_parfiles=True
    )
    return discovery_runner, inspect_runner, convert_runner
