"""Data Pump command execution for modern expdp/impdp operations."""

from __future__ import annotations

import logging

from oracle_dmp_converter.datapump._base_runner import _BaseRunner
from oracle_dmp_converter.datapump._exit_policy import EXPDP_POLICY, IMPDP_POLICY
from oracle_dmp_converter.datapump.modern.parfile import (
    BatchImportJob,
    BulkMetadataImportJob,
    ExportJob,
    ImportJob,
    MasterOnlyJob,
    SqlFileJob,
    render_batch_import_parfile,
    render_bulk_metadata_import_parfile,
    render_export_parfile,
    render_import_parfile,
    render_master_only_parfile,
    render_sqlfile_parfile,
)

LOGGER = logging.getLogger(__name__)

# ORA-39142: incompatible version number in dump file.
# ORA-39143: "The file may be an original export dump file."
# Either code is a definitive signal the dump was created by legacy exp, not expdp.
# The exact code varies by Oracle version; 23ai Free emits ORA-39143.
_LEGACY_FORMAT_ERRORS = ("ORA-39142", "ORA-39143")


def is_legacy_format_error(output: str) -> bool:
    """Return True if *output* contains an error identifying a legacy ``exp`` dump."""
    return any(code in output for code in _LEGACY_FORMAT_ERRORS)


class DataPumpRunner(_BaseRunner):
    """Executes modern Data Pump (expdp/impdp) jobs inside a Docker container."""

    def run_expdp(self, job: ExportJob) -> str:
        """Run an ``expdp`` export job and return the combined output.

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit from ``expdp``.

        Args:
            job: Export job parameters used to render the parfile.

        Returns:
            Combined stdout + stderr from the ``expdp`` invocation.
        """
        return self._run_tool(["expdp"], render_export_parfile(job), "expdp", policy=EXPDP_POLICY)

    def run_impdp(self, job: ImportJob) -> str:
        """Run an ``impdp`` import job and return the combined output.

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit from ``impdp``.

        Args:
            job: Import job parameters used to render the parfile.

        Returns:
            Combined stdout + stderr from the ``impdp`` invocation.
        """
        return self._run_tool(["impdp"], render_import_parfile(job), "impdp", policy=IMPDP_POLICY)

    def run_batch_impdp(self, job: BatchImportJob) -> str:
        """Run a single ``impdp`` call that imports multiple tables at once.

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit from ``impdp``.

        Args:
            job: Batch import job parameters; all table specs are combined into
                a single ``TABLES=`` line in the rendered parfile.

        Returns:
            Combined stdout + stderr from the ``impdp`` invocation.
        """
        return self._run_tool(
            ["impdp"], render_batch_import_parfile(job), "impdp-batch", policy=IMPDP_POLICY
        )

    def run_bulk_metadata_impdp(self, job: BulkMetadataImportJob) -> str:
        """Run a schema-wide ``impdp CONTENT=METADATA_ONLY`` job without a ``TABLES=`` filter.

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit from ``impdp``.

        Args:
            job: Bulk metadata import job parameters.

        Returns:
            Combined stdout + stderr from the ``impdp`` invocation.
        """
        return self._run_tool(
            ["impdp"],
            render_bulk_metadata_import_parfile(job),
            "impdp-bulk-meta",
            policy=IMPDP_POLICY,
        )

    def run_master_only(self, job: MasterOnlyJob) -> str:
        """Run ``impdp MASTER_ONLY=YES`` to load the dump's master table."""
        return self._run_tool(
            ["impdp"], render_master_only_parfile(job), "impdp-master", policy=IMPDP_POLICY
        )

    def run_sqlfile(self, job: SqlFileJob) -> str:
        """Run ``impdp SQLFILE=`` to extract DDL into a file inside the container.

        Data Pump writes CREATE TABLE / CREATE INDEX statements to the SQLFILE
        without touching any schema objects.  The file can then be read and
        parsed to discover tables in the dump.

        Raises :class:`~oracle_dmp_converter.errors.DataPumpError` on non-zero
        exit from ``impdp``.

        Args:
            job: SQLFILE job parameters used to render the parfile.

        Returns:
            Combined stdout + stderr from the ``impdp`` invocation.
        """
        return self._run_tool(
            ["impdp"], render_sqlfile_parfile(job), "impdp-sqlfile", policy=IMPDP_POLICY
        )

    def read_remote_file(self, path: str) -> str:
        """Read a file from inside the container, returning its contents or ''."""
        result = self.container.exec(["cat", path], check=False)
        return result.stdout if result.returncode == 0 else ""
