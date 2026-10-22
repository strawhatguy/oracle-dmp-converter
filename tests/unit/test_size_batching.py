"""Unit tests for size-aware planning: hash slices, size-packed batches, freeing staging space."""
# pylint: disable=protected-access,missing-function-docstring

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from oracle_dmp_converter.config import ConverterConfig
from oracle_dmp_converter.core.executor import StagingExecutor, _size_batches
from oracle_dmp_converter.core.results import ChunkConversionResult
from oracle_dmp_converter.datapump.modern.parfile import (
    BatchImportJob,
    MasterOnlyJob,
    render_batch_import_parfile,
    render_master_only_parfile,
)
from oracle_dmp_converter.errors import DataPumpError
from oracle_dmp_converter.models import (
    ChunkPlan,
    ColumnMetadata,
    DumpFormat,
    TableMetadata,
    TablePlan,
    TableStrategy,
)
from oracle_dmp_converter.oracle.conn import OracleCredentials
from oracle_dmp_converter.persistence.serialization import (
    chunk_plan_from_dict,
    chunk_plan_to_dict,
)
from oracle_dmp_converter.planner import MAX_BATCH_BYTES, plan_table, plan_tables
from oracle_dmp_converter.runtime.admin import OracleAdminConnection

GIB = 1024**3


def _table(size: int, *columns: ColumnMetadata) -> TableMetadata:
    return TableMetadata(
        schema="SRC",
        name="BIG",
        columns=columns or (ColumnMetadata("ID", "NUMBER", 1),),
        estimated_bytes=size,
    )


def _whole(table: str, size: int) -> TablePlan:
    return TablePlan(
        schema="SRC",
        table=table,
        strategy=TableStrategy.WHOLE_TABLE,
        chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
        extra={"estimated_bytes": size},
    )


def _executor() -> tuple[StagingExecutor, MagicMock]:
    executor = StagingExecutor(
        container=MagicMock(),
        admin=OracleAdminConnection("localhost", 1521, "FREE", "system", "pwd"),
        work_dir=Path("/tmp/work"),
        dumpfiles=("test.dmp",),
        config=ConverterConfig(),
    )
    workflow = MagicMock(dump_format=DumpFormat.DATAPUMP)
    executor._workflow = workflow  # type: ignore[attr-defined]
    return executor, workflow


def _conn_ctx() -> tuple[MagicMock, MagicMock]:
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = lambda s: conn
    ctx.__exit__ = MagicMock(return_value=False)
    return conn, ctx


# --- planner -----------------------------------------------------------------


def test_oversized_table_is_sliced_on_first_hashable_column() -> None:
    table = _table(
        20 * GIB,
        ColumnMetadata("NOTES", "CLOB", 1),
        ColumnMetadata("ID", "NUMBER", 2),
    )
    plan = plan_table(table, ConverterConfig())
    assert plan.strategy == TableStrategy.SLICE
    assert [c.name for c in plan.chunks] == [f"slice-00{k}-of-004" for k in range(1, 5)]
    assert plan.chunks[0].where == "WHERE NVL(ORA_HASH(ID, 3), 0) = 0"
    assert plan.chunks[3].where == "WHERE NVL(ORA_HASH(ID, 3), 0) = 3"


def test_table_within_budget_stays_whole() -> None:
    assert plan_table(_table(MAX_BATCH_BYTES), ConverterConfig()).strategy == (
        TableStrategy.WHOLE_TABLE
    )


def test_oversized_table_without_hashable_column_warns() -> None:
    plan = plan_table(_table(20 * GIB, ColumnMetadata("DOC", "BLOB", 1)), ConverterConfig())
    assert plan.strategy == TableStrategy.WHOLE_TABLE
    assert "no column to slice on" in plan.warnings[0]


def test_legacy_dump_is_never_sliced() -> None:
    plan = plan_table(_table(20 * GIB), ConverterConfig(), dump_format=DumpFormat.LEGACY)
    assert plan.strategy == TableStrategy.WHOLE_TABLE


def test_plan_tables_records_size_for_batching() -> None:
    plan = plan_tables([_table(5 * GIB)], ConverterConfig())[0]
    assert plan.extra["estimated_bytes"] == 5 * GIB


def test_slice_where_round_trips_through_plan_file() -> None:
    chunk = ChunkPlan(name="slice-001-of-002", strategy=TableStrategy.SLICE, where="WHERE 1=1")
    assert chunk_plan_from_dict(chunk_plan_to_dict(chunk)) == chunk


# --- batching ----------------------------------------------------------------


def test_batches_close_at_byte_budget() -> None:
    plans = [_whole("A", 5 * GIB), _whole("B", 3 * GIB), _whole("C", 2 * GIB), _whole("D", 1)]
    batches = [[tp.table for tp in batch] for batch, _ in _size_batches(plans)]
    assert batches == [["A", "B"], ["C", "D"]]


def test_slices_of_one_table_never_share_a_batch() -> None:
    sliced = plan_table(_table(20 * GIB), ConverterConfig())
    sliced = TablePlan(**{**sliced.__dict__, "extra": {"estimated_bytes": 4}})
    batches = list(_size_batches([sliced, _whole("SMALL", 1)]))
    assert [len(batch) for batch, _ in batches] == [1, 1, 1, 2]
    assert batches[-1][0][1].table == "SMALL"


# --- executor ----------------------------------------------------------------


def test_slice_batch_passes_query_and_frees_tables(tmp_path: Path) -> None:
    executor, workflow = _executor()
    sliced = plan_table(_table(20 * GIB), ConverterConfig())
    unit = TablePlan(**{**sliced.__dict__, "chunks": sliced.chunks[:1]})
    _, ctx = _conn_ctx()
    result = ChunkConversionResult(name="x", imported_rows=1, output_rows=1, output_path=tmp_path)
    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=ctx),
        patch("oracle_dmp_converter.core.executor.ensure_schema"),
        patch.object(executor, "export_stage_table", return_value=result),
        patch("oracle_dmp_converter.core.executor.truncate_table") as truncate,
    ):
        executor.convert_table_batch([unit, _whole("SMALL", 1)], tmp_path)
    _, queries = workflow.import_chunks_batch.call_args[0]
    assert queries == (("SRC", "BIG", "WHERE NVL(ORA_HASH(ID, 3), 0) = 0"),)
    assert [c.args[1:] for c in truncate.call_args_list] == [
        ("DMP_SRC", "BIG"),
        ("DMP_SRC", "SMALL"),
    ]


def test_dump_table_sizes_reads_master_table() -> None:
    executor, workflow = _executor()
    conn, ctx = _conn_ctx()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [("SRC", "BIG", 123, 7)]
    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=ctx),
        patch("oracle_dmp_converter.core.executor.execute_ignore"),
    ):
        assert executor._dump_table_sizes() == {("SRC", "BIG"): (123, 7)}
    workflow.load_master_table.assert_called_once_with("DMC_TABLE_SIZES")


def test_dump_table_sizes_failure_means_unknown() -> None:
    executor, workflow = _executor()
    workflow.load_master_table.side_effect = DataPumpError("boom")
    _, ctx = _conn_ctx()
    with (
        patch("oracle_dmp_converter.core.executor.oracle_connection", return_value=ctx),
        patch("oracle_dmp_converter.core.executor.execute_ignore"),
    ):
        assert executor._dump_table_sizes() == {}


def test_legacy_dump_has_no_sizes() -> None:
    executor, workflow = _executor()
    workflow.dump_format = DumpFormat.LEGACY
    assert executor._dump_table_sizes() == {}


# --- parfiles ----------------------------------------------------------------


def test_batch_parfile_renders_query_lines() -> None:
    job = BatchImportJob(
        connection=OracleCredentials(user="system", password="pw", service="FREE"),
        directory="D",
        dumpfiles=("a.dmp",),
        logfile="L:x.log",
        table_specs=(("SRC", "BIG", None),),
        queries=(("SRC", "BIG", "WHERE NVL(ORA_HASH(ID, 3), 0) = 0"),),
    )
    assert 'QUERY=SRC.BIG:"WHERE NVL(ORA_HASH(ID, 3), 0) = 0"' in render_batch_import_parfile(job)


def test_master_only_parfile() -> None:
    job = MasterOnlyJob(
        connection=OracleCredentials(user="system", password="pw", service="FREE"),
        directory="D",
        dumpfiles=("a.dmp", "b.dmp"),
        logfile="L:m.log",
        job_name="J",
    )
    text = render_master_only_parfile(job)
    assert "MASTER_ONLY=YES\nJOB_NAME=J\n" in text
    assert "DUMPFILE=a.dmp,b.dmp" in text
