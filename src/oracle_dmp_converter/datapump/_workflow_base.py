"""Format-agnostic ``DumpWorkflow`` base and shared ``WorkflowConfig``.

Pulled out of :mod:`datapump.workflow` so that the concrete
``LegacyDumpWorkflow`` / ``DataPumpWorkflow`` subclasses can import the base
without forming an import cycle with the :func:`create_workflow` factory.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from oracle_dmp_converter.models import DumpFormat
from oracle_dmp_converter.oracle.conn import OracleCredentials
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle


class DumpWorkflow(ABC):
    """Format-agnostic interface for interacting with an Oracle dump file.

    All format-specific branching (``impdp`` vs ``imp``, ``SQLFILE`` vs
    ``INDEXFILE``, modern parfile syntax vs legacy parfile syntax) is
    encapsulated here so the orchestrator can call a single, consistent API
    regardless of whether the dump was produced by modern Data Pump or
    legacy ``exp``.
    """

    @property
    @abstractmethod
    def dump_format(self) -> DumpFormat:
        """Return the :class:`DumpFormat` this workflow handles."""

    @abstractmethod
    def discover_tables(self) -> tuple[tuple[str, str], ...]:
        """Return ``(schema, table)`` pairs found in the dump."""

    @abstractmethod
    def required_tablespaces(self) -> frozenset[str]:
        """Return custom tablespaces that must exist before import begins."""

    @abstractmethod
    def import_all_metadata(self, source_schema: str, stage_schema: str) -> None:
        """Import DDL for all tables in *source_schema* into *stage_schema*."""

    @abstractmethod
    def import_metadata(self, source_schema: str, stage_schema: str, table: str) -> None:
        """Import the DDL for *table* (no row data) into *stage_schema*."""

    @abstractmethod
    def import_chunk(
        self,
        source_schema: str,
        stage_schema: str,
        table: str,
        chunk_name: str,
        partition_name: str | None,
        subpartition_name: str | None = None,
    ) -> None:
        """Import one logical chunk of *table* data into *stage_schema*."""

    def load_master_table(self, job_name: str) -> None:
        """Load the dump's master table as ``SYSTEM.<job_name>`` (Data Pump only)."""
        raise NotImplementedError(f"{self.dump_format.value} dumps have no master table")

    def import_chunks_batch(
        self,
        chunks: list[tuple[str, str, str, str, str | None, str | None]],
        queries: tuple[tuple[str, str, str], ...] = (),
    ) -> None:
        """Import multiple chunks in a single Oracle tool invocation.

        Each entry is ``(source_schema, stage_schema, table, chunk_name,
        partition_name, subpartition_name)``. The default implementation
        falls back to one :meth:`import_chunk` call per entry; concrete
        workflows override this to combine all specs into a single
        ``impdp``/``imp`` invocation.  *queries* are ``(source_schema, table,
        where_clause)`` row filters, supported only by Data Pump.
        """
        if queries:
            raise NotImplementedError(f"{self.dump_format.value} dumps cannot filter rows")
        for (
            source_schema,
            stage_schema,
            table,
            chunk_name,
            partition_name,
            subpartition_name,
        ) in chunks:
            self.import_chunk(
                source_schema,
                stage_schema,
                table,
                chunk_name,
                partition_name,
                subpartition_name,
            )


@dataclass(frozen=True)
class WorkflowConfig:
    """All parameters required to build either workflow implementation."""

    credentials: OracleCredentials
    directory: str
    directory_path: str
    dumpfiles: tuple[str, ...]
    container: ContainerOracle
    work_dir: Path
    discovery_directory: str
    inspect_directory: str
    convert_directory: str
