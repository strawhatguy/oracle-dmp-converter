"""Low-level Oracle staging executor.

Drives the discover → stage → import → export sequence against a running
Oracle container.  Format-specific branching (modern Data Pump vs legacy
exp/imp) is delegated to :class:`DumpWorkflow`.
"""

# pylint: disable=too-many-lines  # single coherent driver; splitting hurts readability.

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import oracledb

from oracle_dmp_converter.config import ConverterConfig, column_override
from oracle_dmp_converter.core.results import (
    ChunkConversionResult,
    PlanConversionResult,
    TableConversionResult,
)
from oracle_dmp_converter.core.staging import (
    apply_byte_to_char,
    dematerialize_mviews,
    disable_foreign_keys,
    disable_triggers,
    drop_vpd_policies,
    drop_vpd_policy_functions,
)
from oracle_dmp_converter.datapump._ddl_parser import parse_missing_tablespace_from_error
from oracle_dmp_converter.datapump._workflow_base import DumpWorkflow, WorkflowConfig
from oracle_dmp_converter.datapump.legacy.workflow import (
    LegacyDumpWorkflow,
    make_legacy_runners,
)
from oracle_dmp_converter.datapump.modern.workflow import (
    DataPumpWorkflow,
    make_modern_runners,
)
from oracle_dmp_converter.datapump.workflow import create_workflow
from oracle_dmp_converter.errors import DataPumpError
from oracle_dmp_converter.models import (
    ChunkPlan,
    ConversionPlan,
    DumpFormat,
    DumpManifest,
    OutputFormat,
    TableMetadata,
    TablePlan,
    TableStrategy,
)
from oracle_dmp_converter.oracle.conn import (
    OracleCredentials,
    count_rows,
    drop_schema,
    ensure_schema,
    ensure_tablespace,
    execute_ignore,
    grant_quota_unlimited,
    oracle_connection,
    table_exists,
    truncate_table,
)
from oracle_dmp_converter.oracle.exporter import export_table
from oracle_dmp_converter.oracle.identifiers import filesystem_safe_identifier
from oracle_dmp_converter.oracle.metadata import discover_table_metadata
from oracle_dmp_converter.persistence.state import ChunkState, StateStore
from oracle_dmp_converter.planner import MAX_BATCH_BYTES
from oracle_dmp_converter.runtime.admin import OracleAdminConnection
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle

LOGGER = logging.getLogger(__name__)

_STAGE_SCHEMA_PREFIX = "DMP_"

# Most tables combined into a single impdp/imp invocation; batches also stop
# at MAX_BATCH_BYTES of dump data when table sizes are known.
TABLE_IMPORT_BATCH_SIZE = 20

# Job name for the master-table load that reads table sizes during inspect.
_SIZES_JOB = "DMC_TABLE_SIZES"

# Env var name for the optional phase-timing JSONL output.  When set to a
# writable path, the helpers below append one record per timed phase so we
# can attribute wall-clock between pre-clear, import-binary, and export
# phases.  Off by default.
_PHASE_TIMING_ENV = "DMP_CONVERTER_PHASE_TIMING_FILE"


def _emit_phase_timing(phase: str, **fields: object) -> None:
    """Append a JSONL phase-timing record when the env var points at a file.

    Diagnostic hook used to attribute integration-test wall-clock.  Never
    raises: a failed write is swallowed so instrumentation can't break the
    convert path.
    """
    path = os.environ.get(_PHASE_TIMING_ENV)
    if not path:
        return
    record = {"phase": phase, **fields}
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
    except OSError:
        pass


@contextmanager
def _phase_timer(phase: str, **static_fields: object) -> Iterator[None]:
    """Context manager that emits a phase-timing record on exit."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        _emit_phase_timing(phase, **static_fields, took_ms=round(elapsed_ms, 2))


def chunk_output_path(
    *,
    source_schema: str,
    table: str,
    chunk_name: str,
    output_dir: Path,
    output_format: OutputFormat,
) -> Path:
    """Return ``<output_dir>/<schema>/<table>/<chunk>.<ext>`` for one chunk."""
    return (
        output_dir
        / filesystem_safe_identifier(source_schema)
        / filesystem_safe_identifier(table)
        / f"{filesystem_safe_identifier(chunk_name)}.{output_format.value}"
    )


class StagingExecutor:
    """Drive inspect → convert against a running Oracle container.

    All dump-format-specific work (impdp vs imp, SQLFILE vs INDEXFILE,
    parfile syntax) is delegated to :attr:`_workflow`, which is initialised
    by :meth:`inspect_dump` (auto-detect) or :meth:`use_format` (known).
    """

    def __init__(
        self,
        *,
        container: ContainerOracle,
        admin: OracleAdminConnection,
        work_dir: Path,
        dumpfiles: tuple[str, ...],
        directory: str = "DATA_PUMP_DIR",
        directory_path: str = "/opt/oracle/admin/FREE/dpdump",
        discovery_directory: str = "ORACLE_DMC_DISCOVERY",
        inspect_directory: str = "ORACLE_DMC_INSPECT",
        convert_directory: str = "ORACLE_DMC_CONVERT",
        stage_password: str = "StagePwd_123",
        output_format: OutputFormat = OutputFormat.PARQUET,
        config: ConverterConfig | None = None,
    ) -> None:
        self.container = container
        self.admin = admin
        self.work_dir = work_dir
        self.dumpfiles = dumpfiles
        self.directory = directory
        self.directory_path = directory_path.rstrip("/")
        self.discovery_directory = discovery_directory
        self.inspect_directory = inspect_directory
        self.convert_directory = convert_directory
        self.stage_password = stage_password
        self.output_format = output_format
        self.config = config if config is not None else ConverterConfig()
        self._workflow: DumpWorkflow | None = None
        # Names of tablespaces that this executor has already created (or
        # confirmed exist) in the running container.  Used by
        # :meth:`_recover_missing_tablespaces` to avoid a redundant
        # ``CREATE TABLESPACE`` round-trip when an earlier chunk already
        # triggered the same recovery.
        self._created_tablespaces: set[str] = set()
        # Source schemas whose staging counterpart has been prepared
        # (created, granted quotas) during this executor's lifetime.  Mirrored
        # into ``ContainerSession.prepared_schemas`` on shutdown.
        self._prepared_schemas: set[str] = set()
        # Cache of staging-table metadata keyed on (stage_schema, table).
        # ``export_stage_table`` runs once per chunk, so without this a
        # partitioned table re-discovers identical column/constraint
        # metadata for every chunk.  The staging structure is stable for
        # the lifetime of staging; the pre-clear helpers drop the cache
        # defensively so a structure-changing re-import can't serve stale
        # columns.
        self._table_metadata_cache: dict[tuple[str, str], TableMetadata] = {}
        # True after :meth:`inspect_dump` has successfully imported metadata
        # for every discovered schema.  Persisted into
        # ``ContainerSession.metadata_imported`` so a later ``convert`` run
        # can call :meth:`validate_metadata_state` to detect a stale or
        # restarted container before doing any work.
        self.metadata_imported: bool = False

    @property
    def dump_format(self) -> DumpFormat:
        if self._workflow is None:
            raise RuntimeError(
                "dump_format is unavailable; call inspect_dump() or use_format() first"
            )
        return self._workflow.dump_format

    def _credentials(self) -> OracleCredentials:
        return OracleCredentials(
            user=self.admin.user,
            password=self.admin.password,
            service=self.admin.service,
        )

    def _workflow_config(self) -> WorkflowConfig:
        return WorkflowConfig(
            credentials=self._credentials(),
            directory=self.directory,
            directory_path=self.directory_path,
            dumpfiles=self.dumpfiles,
            container=self.container,
            work_dir=self.work_dir,
            discovery_directory=self.discovery_directory,
            inspect_directory=self.inspect_directory,
            convert_directory=self.convert_directory,
        )

    def use_format(self, dump_format: DumpFormat) -> None:
        """Initialise :attr:`_workflow` for a known dump format."""
        cfg = self._workflow_config()
        if dump_format is DumpFormat.LEGACY:
            legacy_discovery, legacy_inspect, legacy_convert = make_legacy_runners(
                cfg.container, cfg.work_dir
            )
            self._workflow = LegacyDumpWorkflow(
                credentials=cfg.credentials,
                directory_path=cfg.directory_path,
                dumpfiles=cfg.dumpfiles,
                discovery_runner=legacy_discovery,
                discovery_dir=cfg.work_dir / "discovery",
                inspect_runner=legacy_inspect,
                convert_runner=legacy_convert,
            )
        else:
            discovery_runner, inspect_runner, convert_runner = make_modern_runners(
                cfg.container, cfg.work_dir
            )
            self._workflow = DataPumpWorkflow(
                credentials=cfg.credentials,
                directory=cfg.directory,
                directory_path=cfg.directory_path,
                dumpfiles=cfg.dumpfiles,
                discovery_runner=discovery_runner,
                discovery_dir=cfg.work_dir / "discovery",
                inspect_runner=inspect_runner,
                convert_runner=convert_runner,
                discovery_directory=cfg.discovery_directory,
                inspect_directory=cfg.inspect_directory,
                convert_directory=cfg.convert_directory,
            )

    def _require_workflow(self) -> DumpWorkflow:
        if self._workflow is None:
            raise RuntimeError("No workflow active; call inspect_dump() or use_format() first")
        return self._workflow

    @staticmethod
    def _stage_schema_for(source_schema: str) -> str:
        return f"{_STAGE_SCHEMA_PREFIX}{source_schema}"

    def _connect(self) -> AbstractContextManager[oracledb.Connection]:
        return oracle_connection(
            host=self.admin.host,
            port=self.admin.port,
            service=self.admin.service,
            user=self.admin.user,
            password=self.admin.password,
        )

    def _required_tablespaces(self) -> frozenset[str]:
        if self._workflow is None:
            return frozenset()
        return self._workflow.required_tablespaces()

    def _recover_missing_tablespaces(self, output: str, source_schema: str) -> bool:
        """Parse *output* for ``ORA-00959`` errors and create any missing tablespaces.

        Extracts every tablespace name from ``ORA-00959: tablespace '...' does
        not exist`` lines, creates each tablespace that has not already been
        created during this executor's lifetime (using OMF), and grants
        ``QUOTA UNLIMITED`` on every reported tablespace to the staging
        schema.

        The created-tablespace cache (:attr:`_created_tablespaces`) means a
        flurry of failing chunks that all hit the same missing tablespace
        only triggers one ``CREATE TABLESPACE`` round-trip per name.

        Args:
            output: Combined stdout+stderr text from a failed impdp/imp run
                (typically the message of a
                :class:`~oracle_dmp_converter.errors.DataPumpError`).
            source_schema: Source schema name; the corresponding staging schema
                receives ``QUOTA UNLIMITED`` on each newly created tablespace.

        Returns:
            ``True`` if one or more missing tablespaces were detected and
            an action (create or grant) was performed; ``False`` if *output*
            contained no ``ORA-00959`` lines.
        """
        missing = parse_missing_tablespace_from_error(output)
        if not missing:
            return False
        stage_schema = self._stage_schema_for(source_schema)
        to_create = sorted(missing - self._created_tablespaces)
        LOGGER.info(
            "Recovering %d missing tablespace(s) for schema %s (%d new, %d cached): %s",
            len(missing),
            source_schema,
            len(to_create),
            len(missing) - len(to_create),
            ", ".join(sorted(missing)),
        )
        with self._connect() as conn:
            for tablespace in to_create:
                ensure_tablespace(conn, tablespace)
                self._created_tablespaces.add(tablespace)
            for tablespace in sorted(missing):
                grant_quota_unlimited(conn, stage_schema, tablespace)
        return True

    def _recover_missing_quota(self, output: str, source_schema: str) -> bool:
        """Parse *output* for ``ORA-01950`` and grant unlimited quota where needed.

        ``ORA-01950: no privileges on tablespace 'X'`` fires when a staging
        schema lacks quota on a tablespace referenced by an inbound DDL.
        We grant ``QUOTA UNLIMITED`` on each named tablespace to the staging
        schema and report whether any action was taken.
        """
        # ORA-01950 messages always carry the tablespace name in single quotes
        # immediately after the ``tablespace`` keyword.  Re-use the same
        # regex shape as ORA-00959 since the surrounding text differs.
        # pylint: disable=import-outside-toplevel
        import re  # noqa: PLC0415 - keep the import local to the recovery path.

        names = {
            m.group(1).upper()
            for m in re.finditer(
                r"ORA-01950:\s+no\s+privileges\s+on\s+tablespace\s+'([^']+)'",
                output,
                re.IGNORECASE,
            )
        }
        if not names:
            return False
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.info(
            "Granting QUOTA UNLIMITED to %s on %d tablespace(s): %s",
            stage_schema,
            len(names),
            ", ".join(sorted(names)),
        )
        with self._connect() as conn:
            for tablespace in sorted(names):
                grant_quota_unlimited(conn, stage_schema, tablespace)
        return True

    def prepare_stage_schema(self, source_schema: str) -> None:
        stage_schema = self._stage_schema_for(source_schema)
        if stage_schema in self._prepared_schemas:
            # Already prepared by an earlier batch in this run (or restored
            # from session.json on reconnect).  Skip the redundant
            # CREATE USER + CREATE TABLESPACE + GRANT QUOTA round-trips
            # and the noisy logs they produce.
            return
        LOGGER.info("Ensuring staging schema %s for source %s", stage_schema, source_schema)
        with self._connect() as conn:
            ensure_schema(conn, stage_schema, self.stage_password)
            for tablespace in self._required_tablespaces():
                if tablespace not in self._created_tablespaces:
                    ensure_tablespace(conn, tablespace)
                    self._created_tablespaces.add(tablespace)
                grant_quota_unlimited(conn, stage_schema, tablespace)
        self._prepared_schemas.add(stage_schema)

    def drop_stage_schema(self, source_schema: str) -> None:
        stage_schema = self._stage_schema_for(source_schema)
        LOGGER.info("Dropping staging schema %s", stage_schema)
        with self._connect() as conn:
            drop_schema(conn, stage_schema)

    def validate_staging_tables(self, table_plans: list[TablePlan]) -> list[TablePlan]:
        """Filter *table_plans* down to those with present staging tables.

        Should be called before convert when the workflow was already
        initialised from a prior :meth:`inspect_dump` call.  Tables whose
        staging counterpart no longer exists (because the metadata import
        skipped them — common when a legacy ``exp`` dump fails to
        serialise IDENTITY / JSON / object-type columns) are dropped from
        the returned list and logged at WARNING.

        Returns:
            The subset of *table_plans* whose staging tables still exist.
            Missing tables are filtered out rather than treated as fatal,
            so a partial dump still produces partial output.
        """
        present: list[TablePlan] = []
        missing: list[str] = []
        with self._connect() as conn:
            for tp in table_plans:
                stage_schema = self._stage_schema_for(tp.schema)
                if table_exists(conn, stage_schema, tp.table):
                    present.append(tp)
                else:
                    missing.append(f"{stage_schema}.{tp.table}")
        if missing:
            LOGGER.warning(
                "Skipping %d staging table(s) absent from the container "
                "(typically because the source dump's metadata for them was "
                "incomplete): %s",
                len(missing),
                ", ".join(missing),
            )
        return present

    def _apply_staging_fixups(self, source_schema: str) -> None:
        """Run all post-import staging fixups in a single connection."""
        stage_schema = self._stage_schema_for(source_schema)
        with self._connect() as conn:
            dematerialize_mviews(conn, stage_schema)
            disable_triggers(conn, stage_schema)
            disable_foreign_keys(conn, stage_schema)
            # Drop the policy functions BEFORE the policies: the function
            # discovery reads ALL_POLICIES, so the policies must still exist
            # when drop_vpd_policy_functions runs.  (Dropping a function while
            # its policy is still attached is allowed, and no query touches the
            # protected table between here and drop_vpd_policies.)
            drop_vpd_policy_functions(conn, stage_schema)
            drop_vpd_policies(conn, stage_schema)
            apply_byte_to_char(conn, stage_schema)

    def inspect_dump(self) -> DumpManifest:
        """Inspect the dump, auto-detecting format, and return a manifest."""
        self._workflow = create_workflow(self._workflow_config())
        schema_tables = self._workflow.discover_tables()

        total = len(schema_tables)
        LOGGER.info("Discovered %d tables in dump", total)

        seen_schemas: set[str] = set()
        for source_schema, _ in schema_tables:
            if source_schema in seen_schemas:
                continue
            seen_schemas.add(source_schema)
            stage_schema = self._stage_schema_for(source_schema)
            LOGGER.info("Importing metadata for schema %s -> %s", source_schema, stage_schema)
            self.prepare_stage_schema(source_schema)
            try:
                self._require_workflow().import_all_metadata(source_schema, stage_schema)
            except DataPumpError as exc:
                if not self._recover_missing_tablespaces(str(exc), source_schema):
                    raise
                LOGGER.info(
                    "Retrying bulk metadata import for %s after tablespace recovery",
                    source_schema,
                )
                self._require_workflow().import_all_metadata(source_schema, stage_schema)
            self._apply_staging_fixups(source_schema)

        sizes = self._dump_table_sizes()
        tables: list[TableMetadata] = []
        for i, (source_schema, table) in enumerate(schema_tables, start=1):
            stage_schema = self._stage_schema_for(source_schema)
            LOGGER.info(
                "Inspecting table %d/%d: %s.%s (staging -> %s)",
                i,
                total,
                source_schema,
                table,
                stage_schema,
            )
            with self._connect() as conn:
                metadata = discover_table_metadata(conn, stage_schema, table)
            # The staging copy holds no rows yet, so prefer the dump's own figures.
            dump_bytes, dump_rows = sizes.get((source_schema, table), (None, None))
            tables.append(
                TableMetadata(
                    schema=source_schema,
                    name=table,
                    columns=metadata.columns,
                    estimated_bytes=dump_bytes or metadata.estimated_bytes,
                    row_count=dump_rows if dump_rows is not None else metadata.row_count,
                    partitions=metadata.partitions,
                    primary_key=metadata.primary_key,
                    unique_keys=metadata.unique_keys,
                )
            )
        self.metadata_imported = True
        return DumpManifest(
            dump_paths=self.dumpfiles,
            tables=tuple(tables),
            dump_format=self._workflow.dump_format,
        )

    def _dump_table_sizes(self) -> dict[tuple[str, str], tuple[int, int]]:
        """Return ``{(schema, table): (dump_bytes, rows)}`` from the dump's master table.

        Only Data Pump dumps carry a master table.  Sizes only steer batching
        and slicing, so any failure here is logged and treated as "unknown".
        """
        workflow = self._require_workflow()
        if workflow.dump_format != DumpFormat.DATAPUMP:
            return {}
        master = f"SYSTEM.{_SIZES_JOB}"
        try:
            with self._connect() as conn:
                execute_ignore(conn, f"DROP TABLE {master} PURGE", {942})
            workflow.load_master_table(_SIZES_JOB)
            with self._connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"SELECT object_schema, object_name, SUM(dump_length), SUM(completed_rows) "
                        f"FROM {master} WHERE object_type = 'TABLE_DATA' AND process_order > 0 "
                        "GROUP BY object_schema, object_name"
                    )
                    rows = cursor.fetchall()
                execute_ignore(conn, f"DROP TABLE {master} PURGE", {942})
        except (DataPumpError, oracledb.DatabaseError) as exc:
            LOGGER.warning("Could not read table sizes from the dump: %s", exc)
            return {}
        LOGGER.info("Read data sizes for %d tables from the dump", len(rows))
        return {
            (schema, table): (int(size or 0), int(count or 0))
            for schema, table, size, count in rows
        }

    def get_prepared_schemas(self) -> frozenset[str]:
        """Return the staging schemas this executor has prepared so far.

        Mirrored into :attr:`ContainerSession.prepared_schemas` when
        :meth:`OracleDMPConverter.stop` runs with ``keep_alive=True`` so a
        later ``convert`` reconnect can short-circuit re-creation.
        """
        return frozenset(self._prepared_schemas)

    def validate_metadata_state(self, plan: ConversionPlan) -> None:
        """Verify the running container still holds the inspect-time state.

        When :attr:`metadata_imported` is ``False`` this returns immediately;
        the caller has signalled that inspect has not run against this
        executor (e.g. ``convert`` invoked against a freshly created container
        with no prior inspect phase, in which case ``convert_plan`` will
        prepare schemas lazily).

        Otherwise we confirm every staging user ``DMP_<schema>`` exists in
        ``dba_users``.  A missing user means the container was restarted
        between inspect and convert; we fail fast with a remediation hint
        ("re-run inspect").

        Missing staging *tables* (with the users present) are NOT checked
        here — ``convert_plan`` calls :meth:`validate_staging_tables`, which
        filters absent tables out of the plan and warns about them.  This
        method only owns the user-level fail-fast so the two passes don't
        each query ``dba_tables`` for every supported table.
        """
        if not self.metadata_imported:
            return
        supported = [tp for tp in plan.tables if tp.strategy != TableStrategy.UNSUPPORTED]
        if not supported:
            return
        missing_users: set[str] = set()
        with self._connect() as conn, conn.cursor() as cursor:
            wanted_users = {self._stage_schema_for(tp.schema) for tp in supported}
            # Bind variables would require dynamic IN-list construction;
            # validate per-name with a single small query each so the
            # connection round-trips stay bounded by |wanted_users|.
            for user in sorted(wanted_users):
                cursor.execute(
                    "SELECT 1 FROM dba_users WHERE username = :u",
                    {"u": user},
                )
                if cursor.fetchone() is None:
                    missing_users.add(user)
        if missing_users:
            # Missing staging *users* really do indicate stale state — the
            # container was likely restarted between inspect and convert.
            raise ValueError(
                "Container state does not match inspect manifest "
                "(missing staging users: "
                + ", ".join(sorted(missing_users))
                + "). Re-run inspect before convert."
            )

    def _import_chunk_with_recovery(
        self,
        *,
        source_schema: str,
        stage_schema: str,
        table: str,
        chunk_name: str,
        partition_name: str | None,
        subpartition_name: str | None = None,
        max_attempts: int = 3,
    ) -> None:
        """Import a single chunk, retrying after tablespace / quota recovery.

        On each failed attempt the recovery helpers inspect the
        :class:`DataPumpError` output:

        * :meth:`_recover_missing_tablespaces` handles ``ORA-00959``
          (missing tablespace) by creating the tablespace via OMF and
          granting unlimited quota to *stage_schema*.
        * :meth:`_recover_missing_quota` handles ``ORA-01950`` (no
          privileges) by granting unlimited quota on the offending
          tablespace.

        If neither recovery applies, or *max_attempts* is exhausted, the
        original :class:`DataPumpError` is re-raised so the caller's state
        tracking still records the failure.
        """
        workflow = self._require_workflow()
        last_exc: DataPumpError | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                workflow.import_chunk(
                    source_schema=source_schema,
                    stage_schema=stage_schema,
                    table=table,
                    chunk_name=chunk_name,
                    partition_name=partition_name,
                    subpartition_name=subpartition_name,
                )
                return
            except DataPumpError as exc:
                last_exc = exc
                output = str(exc)
                recovered = self._recover_missing_tablespaces(output, source_schema)
                recovered |= self._recover_missing_quota(output, source_schema)
                if not recovered:
                    raise
                if attempt == max_attempts:
                    LOGGER.error(
                        "Chunk %s for %s.%s still failing after %d recovery attempt(s)",
                        chunk_name,
                        source_schema,
                        table,
                        attempt,
                    )
                    raise
                LOGGER.info(
                    "Retrying chunk %s for %s.%s (attempt %d/%d) after recovery",
                    chunk_name,
                    source_schema,
                    table,
                    attempt + 1,
                    max_attempts,
                )
        # Defensive: loop exits via return or raise; reachable only if
        # max_attempts <= 0, which is not a supported caller contract.
        assert last_exc is not None
        raise last_exc

    def import_table_chunk(
        self,
        *,
        source_schema: str,
        table: str,
        chunk_name: str,
        partition_name: str | None = None,
        subpartition_name: str | None = None,
    ) -> int:
        stage_schema = self._stage_schema_for(source_schema)
        workflow = self._require_workflow()

        # Legacy ``imp`` with IGNORE=Y appends, so for whole-table chunks we
        # truncate first to keep re-runs idempotent.  Partition / subpartition
        # chunks must NOT truncate — they need previously imported partitions
        # of the same table to remain in place.  The caller is responsible for
        # truncating once at the start of a multi-chunk legacy table (see
        # _truncate_legacy_stage_tables in the batch path).
        if workflow.dump_format == DumpFormat.LEGACY and not (partition_name or subpartition_name):
            with self._connect() as conn:
                truncate_table(conn, stage_schema, table)

        self._import_chunk_with_recovery(
            source_schema=source_schema,
            stage_schema=stage_schema,
            table=table,
            chunk_name=chunk_name,
            partition_name=partition_name,
            subpartition_name=subpartition_name,
        )

        with self._connect() as conn:
            return count_rows(conn, stage_schema, table)

    def _cached_stage_metadata(
        self, conn: oracledb.Connection, stage_schema: str, table: str
    ) -> TableMetadata:
        """Return staging-table metadata, discovering it once per table.

        ``export_stage_table`` is called per chunk; the underlying staging
        structure is identical across a table's chunks, so we memoise the
        8-query discovery on ``(stage_schema, table)``.
        """
        key = (stage_schema, table)
        cached = self._table_metadata_cache.get(key)
        if cached is None:
            cached = discover_table_metadata(conn, stage_schema, table)
            self._table_metadata_cache[key] = cached
        return cached

    def export_stage_table(
        self,
        *,
        source_schema: str,
        table: str,
        chunk_name: str,
        output_dir: Path,
        partition_name: str | None = None,
        subpartition_name: str | None = None,
    ) -> ChunkConversionResult:
        stage_schema = self._stage_schema_for(source_schema)
        output_path = chunk_output_path(
            source_schema=source_schema,
            table=table,
            chunk_name=chunk_name,
            output_dir=output_dir,
            output_format=self.output_format,
        )
        with self._connect() as conn:
            metadata = self._cached_stage_metadata(conn, stage_schema, table)
            imported_rows = count_rows(
                conn,
                stage_schema,
                table,
                partition_name,
                subpartition_name=subpartition_name,
            )
            col_overrides = {
                col.name: ov
                for col in metadata.columns
                if (ov := column_override(self.config, source_schema, table, col.name))
            }
            export_result = export_table(
                conn,
                schema_name=stage_schema,
                table_name=table,
                columns=metadata.columns,
                output_path=output_path,
                output_format=self.output_format,
                column_overrides=col_overrides or None,
                partition_name=partition_name,
                subpartition_name=subpartition_name,
            )
        return ChunkConversionResult(
            name=chunk_name,
            imported_rows=imported_rows,
            output_rows=export_result.rows,
            output_path=export_result.path,
        )

    def convert_chunk_plan(
        self,
        *,
        table_plan: TablePlan,
        chunk: ChunkPlan,
        output_dir: Path,
    ) -> ChunkConversionResult:
        if chunk.where:
            raise ValueError(f"{table_plan.qualified_name} {chunk.name}: slices need convert_plan")
        self.import_table_chunk(
            source_schema=table_plan.schema,
            table=table_plan.table,
            chunk_name=chunk.name,
            partition_name=chunk.partition_name,
            subpartition_name=chunk.subpartition_name,
        )
        return self.export_stage_table(
            source_schema=table_plan.schema,
            table=table_plan.table,
            chunk_name=chunk.name,
            output_dir=output_dir,
            partition_name=chunk.partition_name,
            subpartition_name=chunk.subpartition_name,
        )

    def convert_table_plan(
        self,
        table_plan: TablePlan,
        output_dir: Path,
        state_store: StateStore | None = None,
    ) -> TableConversionResult:
        if table_plan.strategy == TableStrategy.UNSUPPORTED:
            reason = table_plan.reason or "unsupported table conversion strategy"
            raise ValueError(f"{table_plan.qualified_name}: {reason}")

        self.prepare_stage_schema(table_plan.schema)

        # Legacy partitioned/subpartitioned tables import multiple chunks
        # via ``imp ... IGNORE=Y`` (APPEND semantics).  Truncate the staging
        # table once up-front so a re-run starts clean; the per-chunk
        # truncate in ``import_table_chunk`` only fires for whole-table
        # chunks now.
        workflow = self._require_workflow()
        if workflow.dump_format == DumpFormat.LEGACY and any(
            c.partition_name or c.subpartition_name for c in table_plan.chunks
        ):
            stage_schema = self._stage_schema_for(table_plan.schema)
            with self._connect() as conn:
                truncate_table(conn, stage_schema, table_plan.table)

        chunk_results: list[ChunkConversionResult] = []
        for chunk in table_plan.chunks:
            state = state_store.get(table_plan.qualified_name, chunk.name) if state_store else None
            if state and state.status == "completed":
                output_path = chunk_output_path(
                    source_schema=table_plan.schema,
                    table=table_plan.table,
                    chunk_name=chunk.name,
                    output_dir=output_dir,
                    output_format=self.output_format,
                )
                chunk_results.append(
                    ChunkConversionResult(
                        name=chunk.name,
                        imported_rows=state.imported_rows or 0,
                        output_rows=state.output_rows or 0,
                        output_path=output_path,
                    )
                )
                continue

            if state_store:
                state_store.upsert(ChunkState(table_plan.qualified_name, chunk.name, "running"))
            try:
                result = self.convert_chunk_plan(
                    table_plan=table_plan,
                    chunk=chunk,
                    output_dir=output_dir,
                )
                if result.imported_rows != result.output_rows:
                    msg = (
                        f"row count mismatch for {table_plan.qualified_name} {chunk.name}: "
                        f"imported={result.imported_rows}, output={result.output_rows}"
                    )
                    raise ValueError(msg)
                if state_store:
                    state_store.upsert(
                        ChunkState(
                            table_plan.qualified_name,
                            chunk.name,
                            "completed",
                            result.imported_rows,
                            result.output_rows,
                        )
                    )
                chunk_results.append(result)
            except Exception as exc:
                if state_store:
                    state_store.upsert(
                        ChunkState(table_plan.qualified_name, chunk.name, "failed", error=str(exc))
                    )
                raise
        return TableConversionResult(
            source_schema=table_plan.schema,
            table=table_plan.table,
            chunks=tuple(chunk_results),
        )

    def _split_batch_pending(
        self,
        table_plans: list[TablePlan],
        output_dir: Path,
        state_store: StateStore | None,
        prefetched: dict[tuple[str, str], ChunkState] | None = None,
    ) -> tuple[dict[str, list[ChunkConversionResult]], list[tuple[TablePlan, ChunkPlan]]]:
        """Separate already-completed chunks (rehydrated from state) from pending work."""
        chunk_results: dict[str, list[ChunkConversionResult]] = {
            tp.qualified_name: [] for tp in table_plans
        }
        pending: list[tuple[TablePlan, ChunkPlan]] = []
        for tp in table_plans:
            for chunk in tp.chunks:
                state = self._chunk_state(state_store, prefetched, tp.qualified_name, chunk.name)
                if state and state.status == "completed":
                    chunk_results[tp.qualified_name].append(
                        ChunkConversionResult(
                            name=chunk.name,
                            imported_rows=state.imported_rows or 0,
                            output_rows=state.output_rows or 0,
                            output_path=chunk_output_path(
                                source_schema=tp.schema,
                                table=tp.table,
                                chunk_name=chunk.name,
                                output_dir=output_dir,
                                output_format=self.output_format,
                            ),
                        )
                    )
                else:
                    pending.append((tp, chunk))
        return chunk_results, pending

    def _truncate_legacy_stage_tables(self, pending: list[tuple[TablePlan, ChunkPlan]]) -> None:
        """Pre-truncate every unique staging table for a legacy batch import."""
        with self._connect() as conn:
            truncated: set[tuple[str, str]] = set()
            for tp, _ in pending:
                stage_schema = self._stage_schema_for(tp.schema)
                key = (stage_schema, tp.table)
                if key not in truncated:
                    truncate_table(conn, stage_schema, tp.table)
                    self._table_metadata_cache.pop(key, None)
                    truncated.add(key)

    def _clear_modern_stage_tables_for_plan(
        self,
        supported: list[TablePlan],
        state_store: StateStore | None,
        prefetched: dict[tuple[str, str], ChunkState] | None = None,
    ) -> None:
        """Pre-clear every staging table with at least one pending chunk.

        Unlike :meth:`_clear_modern_stage_tables` (which works one batch
        at a time), this walks the full ``supported`` plan and decides
        which tables to clear based on the entire set of pending chunks
        across all batches.  This avoids ORA-02292 deadlock cases where
        a reference-partitioned parent table sits in batch N and its
        child sits in batch N+1: clearing the parent first would fail
        because the child still has rows, and the per-batch
        implementation never gets to delete the child.
        """
        if not supported:
            return
        targets: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for tp in supported:
            if not self._table_has_pending_chunks(tp, state_store, prefetched):
                continue
            key = (self._stage_schema_for(tp.schema), tp.table)
            if key in seen:
                continue
            seen.add(key)
            targets.append(key)
        if not targets:
            return
        self._delete_from_staging_tables(targets)

    def _chunk_state(
        self,
        state_store: StateStore | None,
        prefetched: dict[tuple[str, str], ChunkState] | None,
        qualified_name: str,
        chunk_name: str,
    ) -> ChunkState | None:
        """Resolve a chunk's state from the prefetched snapshot or live store.

        When *prefetched* is provided it is authoritative (one bulk
        ``SELECT`` taken at the start of convert); otherwise fall back to a
        live per-chunk ``state_store.get`` so callers without a snapshot
        still work.
        """
        if prefetched is not None:
            return prefetched.get((qualified_name, chunk_name))
        if state_store is not None:
            return state_store.get(qualified_name, chunk_name)
        return None

    def _table_has_pending_chunks(
        self,
        tp: TablePlan,
        state_store: StateStore | None,
        prefetched: dict[tuple[str, str], ChunkState] | None = None,
    ) -> bool:
        """Return True if any chunk on *tp* is not already ``completed``."""
        if state_store is None and prefetched is None:
            return True
        for chunk in tp.chunks:
            existing = self._chunk_state(state_store, prefetched, tp.qualified_name, chunk.name)
            if existing is None or existing.status != "completed":
                return True
        return False

    def _clear_modern_stage_tables(
        self,
        pending: list[tuple[TablePlan, ChunkPlan]],
    ) -> None:
        """Clear staging-table rows before a modern impdp APPEND batch.

        Modern Data Pump imports use ``TABLE_EXISTS_ACTION=APPEND`` to
        avoid ``ORA-39120`` against reference-partitioned children whose
        FK can't be disabled.  APPEND is correct for the happy path
        (inspect → convert against an empty staging schema), but on a
        re-run — e.g. a test invoking convert multiple times — APPEND
        would stack data on top of itself and eventually exhaust the
        container's resources.

        We pre-DELETE rows from every unique staging table in *pending*,
        ignoring ``ORA-02292`` (child record found) so a reference-
        partitioned parent whose child hasn't been DELETE'd yet just
        gets retried after the child.  Two passes are enough to cover
        any realistic FK graph in the audit fixture.
        """
        seen: set[tuple[str, str]] = set()
        targets: list[tuple[str, str]] = []
        for tp, _ in pending:
            key = (self._stage_schema_for(tp.schema), tp.table)
            if key in seen:
                continue
            seen.add(key)
            targets.append(key)
        if targets:
            self._delete_from_staging_tables(targets)

    def _delete_from_staging_tables(self, targets: list[tuple[str, str]]) -> None:
        """Run ``DELETE FROM`` against *targets* with FK-aware retries.

        Each (stage_schema, table) pair gets a ``DELETE FROM`` statement.
        When DELETE fails with ORA-02292 (child record found), the table
        is deferred to the next pass — the child must be DELETE'd first.
        Up to 3 passes are run, which handles arbitrary linear FK chains
        of length <=3.  Each successful DELETE is committed individually
        so an FK violation later doesn't roll the deletes back.
        """
        LOGGER.info("Pre-clearing %d staging table(s) before APPEND batch", len(targets))
        for key in targets:
            self._table_metadata_cache.pop(key, None)
        with self._connect() as conn:
            for pass_num in range(1, 4):
                remaining: list[tuple[str, str]] = []
                deleted_this_pass = 0
                for stage_schema, table in targets:
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(f'DELETE FROM "{stage_schema}"."{table}"')
                            row_count = cursor.rowcount
                        conn.commit()
                        deleted_this_pass += 1
                        LOGGER.debug(
                            "Pre-cleared %s.%s (%d rows)",
                            stage_schema,
                            table,
                            row_count,
                        )
                    except oracledb.DatabaseError as exc:
                        conn.rollback()
                        code = exc.args[0].code if exc.args else None
                        # ORA-02292: child record found — retry next pass.
                        # ORA-00942: table doesn't exist; ignore.
                        # ORA-14452: cannot delete from external table.
                        if code == 2292:
                            remaining.append((stage_schema, table))
                            continue
                        if code in (942, 14452):
                            LOGGER.debug(
                                "Skipping pre-clear of %s.%s: ORA-%05d",
                                stage_schema,
                                table,
                                code or 0,
                            )
                            continue
                        LOGGER.warning(
                            "Pre-clear of %s.%s failed: %s",
                            stage_schema,
                            table,
                            exc,
                        )
                LOGGER.info(
                    "Pre-clear pass %d: cleared=%d, deferred=%d",
                    pass_num,
                    deleted_this_pass,
                    len(remaining),
                )
                if not remaining:
                    break
                targets = remaining
            else:
                # Three passes exhausted with rows still present.  Some
                # FK chains are deeper than 3 levels, or a row is
                # referenced via a constraint that can never be cleared.
                LOGGER.warning(
                    "Pre-clear gave up after 3 passes; tables still holding rows: %s",
                    ", ".join(f"{s}.{t}" for s, t in remaining),
                )

    def _release_stage_tables(self, pending: list[tuple[TablePlan, ChunkPlan]]) -> None:
        """Truncate this batch's staging tables once their chunks are exported.

        Oracle Free caps user data at 12 GB, so exported rows must not pile up
        across batches.  Every chunk of a table is in the same batch, and slice
        chunks rely on the table being empty before their import.  A table that
        can't be truncated (e.g. a reference-partitioned parent, ORA-02266)
        just keeps its space.
        """
        targets = dict.fromkeys((self._stage_schema_for(tp.schema), tp.table) for tp, _ in pending)
        with self._connect() as conn:
            for stage_schema, table in targets:
                try:
                    truncate_table(conn, stage_schema, table)
                except oracledb.DatabaseError as exc:
                    LOGGER.warning(
                        "Could not free %s.%s after export: %s", stage_schema, table, exc
                    )

    def _mark_pending_failed(
        self,
        pending: list[tuple[TablePlan, ChunkPlan]],
        state_store: StateStore | None,
        exc: BaseException,
    ) -> None:
        if state_store is None:
            return
        for tp, chunk in pending:
            state_store.upsert(ChunkState(tp.qualified_name, chunk.name, "failed", error=str(exc)))

    def _export_one_batched_chunk(
        self,
        tp: TablePlan,
        chunk: ChunkPlan,
        output_dir: Path,
        state_store: StateStore | None,
    ) -> ChunkConversionResult:
        try:
            result = self.export_stage_table(
                source_schema=tp.schema,
                table=tp.table,
                chunk_name=chunk.name,
                output_dir=output_dir,
                partition_name=chunk.partition_name,
                subpartition_name=chunk.subpartition_name,
            )
            if result.imported_rows != result.output_rows:
                msg = (
                    f"row count mismatch for {tp.qualified_name} {chunk.name}: "
                    f"imported={result.imported_rows}, output={result.output_rows}"
                )
                raise ValueError(msg)
            if state_store:
                state_store.upsert(
                    ChunkState(
                        tp.qualified_name,
                        chunk.name,
                        "completed",
                        result.imported_rows,
                        result.output_rows,
                    )
                )
            return result
        except Exception as exc:
            if state_store:
                state_store.upsert(
                    ChunkState(tp.qualified_name, chunk.name, "failed", error=str(exc))
                )
            raise

    def convert_table_batch(
        self,
        table_plans: list[TablePlan],
        output_dir: Path,
        state_store: StateStore | None = None,
        prefetched: dict[tuple[str, str], ChunkState] | None = None,
    ) -> list[TableConversionResult]:
        seen_schemas: set[str] = set()
        for tp in table_plans:
            if tp.schema not in seen_schemas:
                self.prepare_stage_schema(tp.schema)
                seen_schemas.add(tp.schema)

        chunk_results, pending = self._split_batch_pending(
            table_plans, output_dir, state_store, prefetched
        )

        if not pending:
            return [
                TableConversionResult(
                    source_schema=tp.schema,
                    table=tp.table,
                    chunks=tuple(chunk_results[tp.qualified_name]),
                )
                for tp in table_plans
            ]

        if state_store:
            with state_store.deferred_commit():
                for tp, chunk in pending:
                    state_store.upsert(ChunkState(tp.qualified_name, chunk.name, "running"))

        workflow = self._require_workflow()
        if workflow.dump_format == DumpFormat.LEGACY:
            with _phase_timer(
                "preclear_legacy_batch",
                dump_format="legacy",
                chunks=len(pending),
            ):
                self._truncate_legacy_stage_tables(pending)
        # Modern dumps: pre-clear was already done cross-batch in
        # convert_plan to handle reference-partition FK chains; nothing
        # to do here.

        import_specs: list[tuple[str, str, str, str, str | None, str | None]] = [
            (
                tp.schema,
                self._stage_schema_for(tp.schema),
                tp.table,
                chunk.name,
                chunk.partition_name,
                chunk.subpartition_name,
            )
            for tp, chunk in pending
        ]
        queries = tuple((tp.schema, tp.table, chunk.where) for tp, chunk in pending if chunk.where)

        try:
            with _phase_timer(
                "import_batch",
                dump_format=workflow.dump_format.value,
                chunks=len(import_specs),
            ):
                workflow.import_chunks_batch(import_specs, queries)
        except Exception as exc:
            self._mark_pending_failed(pending, state_store, exc)
            raise

        # Batch every per-chunk state upsert into one commit for this batch
        # rather than fsync-ing per chunk.  Resumability granularity drops
        # to per-batch, which is safe because re-import is idempotent.
        export_commit_ctx = (
            state_store.deferred_commit() if state_store is not None else nullcontext()
        )
        with export_commit_ctx:
            for tp, chunk in pending:
                try:
                    with _phase_timer(
                        "export_chunk",
                        dump_format=workflow.dump_format.value,
                        table=tp.qualified_name,
                        chunk=chunk.name,
                        is_subpartition=bool(chunk.subpartition_name),
                    ):
                        result = self._export_one_batched_chunk(tp, chunk, output_dir, state_store)
                    chunk_results[tp.qualified_name].append(result)
                    if chunk.subpartition_name:
                        LOGGER.info(
                            "Converted %s.%s subpartition %s/%s: %d rows",
                            tp.schema,
                            tp.table,
                            chunk.partition_name,
                            chunk.subpartition_name,
                            result.output_rows,
                        )
                    elif chunk.partition_name:
                        LOGGER.info(
                            "Converted %s.%s partition %s: %d rows",
                            tp.schema,
                            tp.table,
                            chunk.partition_name,
                            result.output_rows,
                        )
                except Exception as exc:  # noqa: BLE001
                    # A per-chunk failure (bad type, ORA-00932 on an exotic
                    # column, FK violation on a reference-partitioned import)
                    # is recorded in state by ``_export_one_batched_chunk``
                    # but must NOT abort the whole batch — other tables in
                    # the batch are independent and should still be exported.
                    # The failure is reflected later in the conversion report.
                    LOGGER.warning(
                        "Chunk export failed for %s %s: %s; continuing with rest of batch",
                        tp.qualified_name,
                        chunk.name,
                        exc,
                    )

        self._release_stage_tables(pending)
        return [
            TableConversionResult(
                source_schema=tp.schema,
                table=tp.table,
                chunks=tuple(chunk_results[tp.qualified_name]),
            )
            for tp in table_plans
        ]

    def convert_plan(
        self,
        plan: ConversionPlan,
        output_dir: Path,
        state_store: StateStore | None = None,
    ) -> PlanConversionResult:
        workflow_from_inspect = self._workflow is not None
        if self._workflow is None:
            self.use_format(plan.dump_format)

        supported: list[TablePlan] = []
        for table_plan in plan.tables:
            if table_plan.strategy == TableStrategy.UNSUPPORTED:
                LOGGER.warning(
                    "Skipping %s.%s: %s",
                    table_plan.schema,
                    table_plan.table,
                    table_plan.reason or "unsupported strategy",
                )
            else:
                supported.append(table_plan)

        if workflow_from_inspect:
            supported = self.validate_staging_tables(supported)

        # Take one snapshot of all chunk states up front and reuse it for
        # the pre-clear pending check and every batch's pending split,
        # instead of issuing a per-chunk SQLite read in nested loops.
        prefetched = state_store.snapshot() if state_store is not None else None

        # Pre-clear all supported staging tables that have at least one
        # pending chunk.  Doing this once, cross-batch, handles
        # reference-partitioned FK chains where the parent is in batch N
        # and the child is in batch N+1 — clearing the parent first
        # would fail (ORA-02292) because the child still has rows.
        if self._workflow and self._workflow.dump_format != DumpFormat.LEGACY:
            with _phase_timer(
                "preclear_modern_plan",
                dump_format="datapump",
                tables=len(supported),
            ):
                self._clear_modern_stage_tables_for_plan(supported, state_store, prefetched)

        plan_started_at = datetime.now(UTC)
        # Slices of one table arrive as separate batches, so gather chunks per table.
        results: dict[str, TableConversionResult] = {}
        batches = list(_size_batches(supported))
        for number, (batch, batch_bytes) in enumerate(batches, start=1):
            LOGGER.info(
                "Importing batch %d of %d: %d table(s), %.1f GiB",
                number,
                len(batches),
                len(batch),
                batch_bytes / 1024**3,
            )
            batch_results = self.convert_table_batch(batch, output_dir, state_store, prefetched)
            for tp, table_result in zip(batch, batch_results, strict=True):
                LOGGER.info(
                    "Converted %s.%s: %d rows",
                    tp.schema,
                    tp.table,
                    table_result.rows,
                )
                earlier = results.get(tp.qualified_name)
                results[tp.qualified_name] = (
                    replace(table_result, chunks=earlier.chunks + table_result.chunks)
                    if earlier
                    else table_result
                )
        plan_completed_at = datetime.now(UTC)
        return PlanConversionResult(
            tables=tuple(results.values()),
            started_at=plan_started_at,
            completed_at=plan_completed_at,
        )


def _size_batches(table_plans: list[TablePlan]) -> Iterator[tuple[list[TablePlan], int]]:
    """Yield ``(batch, dump_bytes)`` import batches that fit the staging database.

    A batch closes at :data:`TABLE_IMPORT_BATCH_SIZE` tables or
    :data:`MAX_BATCH_BYTES` of dump data, whichever comes first.  Each slice
    of a ``SLICE`` table becomes its own unit, and two units of one table never
    share a batch (one ``impdp`` job takes a single ``QUERY=`` per table).  A
    unit larger than the budget still goes alone in its own batch.
    """
    units: list[tuple[TablePlan, int]] = []
    for tp in table_plans:
        size = int(tp.extra.get("estimated_bytes") or 0)
        if tp.strategy == TableStrategy.SLICE:
            units.extend(
                (replace(tp, chunks=(chunk,)), size // len(tp.chunks)) for chunk in tp.chunks
            )
        else:
            units.append((tp, size))
    batch: list[TablePlan] = []
    batch_bytes = 0
    for tp, size in units:
        if batch and (
            len(batch) >= TABLE_IMPORT_BATCH_SIZE
            or batch_bytes + size > MAX_BATCH_BYTES
            or any(b.qualified_name == tp.qualified_name for b in batch)
        ):
            yield batch, batch_bytes
            batch, batch_bytes = [], 0
        batch.append(tp)
        batch_bytes += size
    if batch:
        yield batch, batch_bytes
