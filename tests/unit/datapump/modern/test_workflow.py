"""Unit tests for datapump/modern/workflow.py."""
# pylint: disable=protected-access

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from oracle_dmp_converter.datapump.modern.workflow import DataPumpWorkflow, make_modern_runners
from oracle_dmp_converter.models import DumpFormat
from oracle_dmp_converter.oracle.conn import OracleCredentials


def _creds() -> OracleCredentials:
    return OracleCredentials(user="system", password="pw", service="FREE")


def _make_workflow(tmp_path: Path, discovery_runner=None) -> DataPumpWorkflow:
    discovery_dir = tmp_path / "discovery"
    discovery_dir.mkdir(parents=True, exist_ok=True)
    return DataPumpWorkflow(
        credentials=_creds(),
        directory="DUMP_DIR",
        directory_path="/dumps",
        dumpfiles=("test.dmp",),
        discovery_runner=discovery_runner or MagicMock(),
        discovery_dir=discovery_dir,
        inspect_runner=MagicMock(),
        convert_runner=MagicMock(),
        discovery_directory="ORACLE_DMC_DISCOVERY",
        inspect_directory="ORACLE_DMC_INSPECT",
        convert_directory="ORACLE_DMC_CONVERT",
    )


class TestDataPumpWorkflowDumpFormat:
    def test_format_is_datapump(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        assert wf.dump_format == DumpFormat.DATAPUMP


class TestRequiredTablespaces:
    def test_returns_empty_when_no_sqlfile(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        # SQLFILE not yet written (discovery not run)
        assert wf.required_tablespaces() == frozenset()

    def test_returns_tablespaces_from_sqlfile(self, tmp_path: Path) -> None:
        discovery_dir = tmp_path / "discovery"
        discovery_dir.mkdir()
        (discovery_dir / "discovery-impdp-sqlfile.sql").write_text(
            'CREATE TABLE "SRC"."T" (ID NUMBER) TABLESPACE "CUSTOM_DATA";\n'
            'CREATE TABLE "SRC"."U" (ID NUMBER) TABLESPACE "CUSTOM_IDX";\n'
        )
        wf = _make_workflow(tmp_path)
        result = wf.required_tablespaces()
        assert result == frozenset({"CUSTOM_DATA", "CUSTOM_IDX"})

    def test_filters_system_tablespaces(self, tmp_path: Path) -> None:
        discovery_dir = tmp_path / "discovery"
        discovery_dir.mkdir()
        (discovery_dir / "discovery-impdp-sqlfile.sql").write_text(
            'CREATE TABLE "SRC"."T" (ID NUMBER) TABLESPACE "USERS";\n'
        )
        wf = _make_workflow(tmp_path)
        assert wf.required_tablespaces() == frozenset()


class TestDiscoverTables:
    def test_reads_sqlfile_and_parses_tables(self, tmp_path: Path) -> None:
        discovery_dir = tmp_path / "discovery"
        discovery_dir.mkdir()
        (discovery_dir / "discovery-impdp-sqlfile.sql").write_text(
            'CREATE TABLE "MYSCHEMA"."ORDERS" (ID NUMBER);\n'
            'CREATE TABLE "MYSCHEMA"."ITEMS" (ID NUMBER);\n'
        )
        mock_runner = MagicMock()
        wf = DataPumpWorkflow(
            credentials=_creds(),
            directory="DUMP_DIR",
            directory_path="/dumps",
            dumpfiles=("test.dmp",),
            discovery_runner=mock_runner,
            discovery_dir=discovery_dir,
            inspect_runner=MagicMock(),
            convert_runner=MagicMock(),
            discovery_directory="ORACLE_DMC_DISCOVERY",
            inspect_directory="ORACLE_DMC_INSPECT",
            convert_directory="ORACLE_DMC_CONVERT",
        )
        tables = wf.discover_tables()
        assert ("MYSCHEMA", "ORDERS") in tables
        assert ("MYSCHEMA", "ITEMS") in tables

    def test_returns_empty_when_no_sqlfile(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        tables = wf.discover_tables()
        assert not tables


class TestImportAllMetadata:
    def test_calls_bulk_metadata_impdp(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_all_metadata("SRC", "STAGE")
        wf._inspect_runner.run_bulk_metadata_impdp.assert_called_once()


class TestImportMetadata:
    def test_calls_impdp(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_metadata("SRC", "STAGE", "ORDERS")
        wf._inspect_runner.run_impdp.assert_called_once()


class TestImportChunk:
    def test_calls_impdp_with_partition(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_chunk("SRC", "STAGE", "ORDERS", "P1", "P1")
        wf._convert_runner.run_impdp.assert_called_once()

    def test_calls_impdp_without_partition(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_chunk("SRC", "STAGE", "ORDERS", "whole", None)
        wf._convert_runner.run_impdp.assert_called_once()


class TestImportChunksBatch:
    def test_empty_batch_is_noop(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_chunks_batch([])
        wf._convert_runner.run_batch_impdp.assert_not_called()

    def test_non_empty_batch_calls_batch_impdp(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_chunks_batch(
            [
                ("SRC", "STAGE", "ORDERS", "whole", None, None),
                ("SRC", "STAGE", "ITEMS", "whole", None, None),
            ]
        )
        wf._convert_runner.run_batch_impdp.assert_called_once()

    def test_deduplicates_remap_schemas(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_chunks_batch(
            [
                ("SRC", "STAGE1", "T1", "whole", None, None),
                ("SRC", "STAGE1", "T2", "whole", None, None),
            ]
        )
        call_args = wf._convert_runner.run_batch_impdp.call_args[0][0]
        assert len(call_args.remap_schemas) == 1

    def test_logfile_name_has_no_dollar(self, tmp_path: Path) -> None:
        wf = _make_workflow(tmp_path)
        wf.import_chunks_batch([("SRC", "STAGE", "MLOG$_ADDRESS", "whole", None, None)])
        job = wf._convert_runner.run_batch_impdp.call_args[0][0]
        assert job.logfile == "ORACLE_DMC_CONVERT:impdp-batch-MLOG__ADDRESS.log"


class TestMakeModernRunners:
    def test_creates_three_runners(self, tmp_path: Path) -> None:
        container = MagicMock()
        disc, insp, conv = make_modern_runners(container, tmp_path)
        assert disc.container is container
        assert insp.container is container
        assert conv.container is container
        assert disc.keep_parfiles is True
        assert insp.keep_parfiles is True
        assert conv.keep_parfiles is True
