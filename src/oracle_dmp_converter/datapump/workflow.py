"""Workflow factory and probed-modern wrapper.

The :class:`DumpWorkflow` base and the :class:`WorkflowConfig` parameter struct
live in :mod:`datapump._workflow_base` so the concrete subclasses can import
them without forming a cycle with the :func:`create_workflow` factory here.
"""

from __future__ import annotations

import logging

from oracle_dmp_converter.datapump._workflow_base import DumpWorkflow, WorkflowConfig
from oracle_dmp_converter.datapump.legacy.workflow import (
    LegacyDumpWorkflow,
    make_legacy_runners,
)
from oracle_dmp_converter.datapump.modern.runner import is_legacy_format_error
from oracle_dmp_converter.datapump.modern.workflow import (
    DataPumpWorkflow,
    make_modern_runners,
)
from oracle_dmp_converter.errors import DataPumpError
from oracle_dmp_converter.models import DumpFormat

LOGGER = logging.getLogger(__name__)


__all__ = ["DumpWorkflow", "WorkflowConfig", "create_workflow"]


def create_workflow(cfg: WorkflowConfig) -> DumpWorkflow:
    """Detect the dump format and return the appropriate :class:`DumpWorkflow`.

    Tries :class:`DataPumpWorkflow` first.  If ``impdp`` signals a legacy-format
    error (``ORA-39142`` / ``ORA-39143``) the function transparently constructs
    and returns a :class:`LegacyDumpWorkflow` instead.  Any other ``impdp``
    failure is re-raised as-is.
    """
    discovery_runner, inspect_runner, convert_runner = make_modern_runners(
        cfg.container, cfg.work_dir
    )
    modern_workflow = DataPumpWorkflow(
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

    LOGGER.info("Probing dump format via impdp SQLFILE (files: %s)", ", ".join(cfg.dumpfiles))
    try:
        tables = modern_workflow.discover_tables()
    except DataPumpError as exc:
        if not is_legacy_format_error(str(exc)):
            raise
        LOGGER.info("Legacy exp format detected; switching to imp-based workflow")
        legacy_discovery, legacy_inspect, legacy_convert = make_legacy_runners(
            cfg.container, cfg.work_dir
        )
        return LegacyDumpWorkflow(
            credentials=cfg.credentials,
            directory_path=cfg.directory_path,
            dumpfiles=cfg.dumpfiles,
            discovery_runner=legacy_discovery,
            discovery_dir=cfg.work_dir / "discovery",
            inspect_runner=legacy_inspect,
            convert_runner=legacy_convert,
        )

    LOGGER.info("Modern Data Pump format confirmed; discovered %d tables", len(tables))
    return _ProbedModernWorkflow(modern_workflow, discovered_tables=tables)


class _ProbedModernWorkflow(DumpWorkflow):
    """Wraps a probed ``DataPumpWorkflow`` so a second discovery is unnecessary."""

    def __init__(
        self,
        inner: DumpWorkflow,
        discovered_tables: tuple[tuple[str, str], ...],
    ) -> None:
        self._inner = inner
        self._cached_tables = discovered_tables

    @property
    def dump_format(self) -> DumpFormat:
        return self._inner.dump_format

    def discover_tables(self) -> tuple[tuple[str, str], ...]:
        return self._cached_tables

    def required_tablespaces(self) -> frozenset[str]:
        return self._inner.required_tablespaces()

    def import_all_metadata(self, source_schema: str, stage_schema: str) -> None:
        self._inner.import_all_metadata(source_schema, stage_schema)

    def import_metadata(self, source_schema: str, stage_schema: str, table: str) -> None:
        self._inner.import_metadata(source_schema, stage_schema, table)

    def import_chunk(
        self,
        source_schema: str,
        stage_schema: str,
        table: str,
        chunk_name: str,
        partition_name: str | None,
        subpartition_name: str | None = None,
    ) -> None:
        self._inner.import_chunk(
            source_schema,
            stage_schema,
            table,
            chunk_name,
            partition_name,
            subpartition_name,
        )

    def load_master_table(self, job_name: str) -> None:
        self._inner.load_master_table(job_name)

    def import_chunks_batch(
        self,
        chunks: list[tuple[str, str, str, str, str | None, str | None]],
        queries: tuple[tuple[str, str, str], ...] = (),
    ) -> None:
        self._inner.import_chunks_batch(chunks, queries)
