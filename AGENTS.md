# AGENTS.md

## What this repo is

Python 3.12 CLI tool that converts Oracle Data Pump (`expdp`) and legacy (`exp`) dump files to Parquet, Avro, or CSV. It does **not** parse `.dmp` files directly — it spins up Oracle Database Free in Docker/Podman as a temporary staging reader, then exports via PyArrow/fastavro.

Library entry point: `src/oracle_dmp_converter/converter.py` — `OracleDMPConverter` facade with `start`/`stop`/`inspect`/`plan`/`convert`/`run`. CLI lives in `src/oracle_dmp_converter/cli/` and is a thin Click adapter — Click group with four subcommands: `doctor`, `inspect`, `plan`, `convert`.

---

## Setup

```bash
make setup          # uv sync --all-groups; creates .venv
```

Run commands via `uv run ...` or activate the venv first. Do not use `pip`.

---

## Developer commands

```bash
make format         # ruff format + ruff check --fix (modifies files)
make format-check   # check only, no changes; equivalent: uv run ruff check .
make lint           # uv run pylint src scripts tests
make clean          # removes .venv dist build work parquet sample-data tests/runs caches
```

---

## Testing

Coverage is **always on** and **gated at 92%**: `--cov` is set in `pyproject.toml`
`addopts` and `[tool.coverage.report] fail_under = 92`. Any pytest run that doesn't
cover ≥ 92% of `src` exits non-zero — so when running a *subset* of tests (a single
file, a single test, or a keyword filter), pass `--no-cov` to skip the threshold,
otherwise the partial run "fails" on coverage even when the tests pass.

```bash
# Unit tests — no Docker required (enforces the 92% gate)
uv run python -m pytest tests/unit

# Single test file — add --no-cov so the coverage gate doesn't trip on a partial run
uv run python -m pytest tests/unit/test_planner.py --no-cov

# Single test by name
uv run python -m pytest tests/unit/test_planner.py::test_partitioned_table_plans_partition_chunks --no-cov

# Keyword filter
uv run python -m pytest -k "test_small_table" --no-cov

# Integration tests — require Docker; pull ~1-2 GB Oracle image on first run; several
# minutes. Use --no-cov: they exercise only a slice of src and would trip the gate.
uv run python -m pytest tests/integration --no-cov
```

Integration tests auto-skip when Docker is unavailable (autouse `skip_if_no_docker` fixture in `tests/integration/conftest.py`). No marker needed on individual tests.

`tests/data/` holds real `impdp` log and dump fixtures captured from actual Oracle runs — used by unit tests for DDL parser coverage without a live container.

---

## Architecture notes

### Dump format auto-detection
`create_workflow()` (`src/oracle_dmp_converter/datapump/workflow.py`) first tries modern `impdp SQLFILE=`. On `ORA-39142` or `ORA-39143` it automatically falls back to legacy `imp INDEXFILE=`. A `_ProbedModernWorkflow` wrapper caches the discovered table list so `discover_tables()` is not called twice. Concrete workflows live in the `datapump/modern/` and `datapump/legacy/` subpackages.

### Partitioning is format-independent
Both modern and legacy dumps support partition- and subpartition-level imports. The planner (`plan_table`) emits one chunk per partition (or per subpartition for composite tables) regardless of dump format — legacy `imp` accepts partition/subpartition names via its `TABLES=schema.table:NAME` syntax. The `dump_format` argument to `plan_table`/`plan_tables` is retained only for API symmetry. Oracle Free caps staging at 12 GB, so inspect reads each table's dump size from the Data Pump master table (`impdp MASTER_ONLY=YES`). The planner splits a non-partitioned modern-dump table over `planner.MAX_BATCH_BYTES` into `SLICE` chunks filtered with `impdp QUERY=` on `ORA_HASH` of its first hashable column; `convert_plan` packs batches up to that byte budget (and 20 tables), and `convert_table_batch` truncates each batch's staging tables after export. Legacy `imp` has no `QUERY=`, so legacy tables are never sliced.

### Staging schema pattern
Source schema `SCHEMA` is imported into staging schema `DMP_SCHEMA`. Staging schemas are prepared once per run (cached in `_prepared_schemas`), not per chunk. Chunks are imported in batches via a single `impdp`/`imp` invocation (`import_chunks_batch`) rather than one process per chunk — see `convert_table_batch`/`convert_plan` in `core/executor.py`. Resumability is tracked in a SQLite `StateStore` at `<work_dir>/convert/state.sqlite`.

### Docker/Podman
The tool uses the Docker SDK Python library for container management but runs `docker exec` / `docker cp` via `subprocess`, not the SDK exec API (SDK's chunked HTTP stream never closes EOF). Works with both Docker and Podman.

**Apple Silicon:** set `DMP_CONVERTER_DOCKER_PLATFORM=linux/amd64` — Oracle Free only has an amd64 image.

### Oracle-to-Arrow type mapping (`src/oracle_dmp_converter/oracle/types.py`)
- `NUMBER(p,0)` with `p <= 18` → `int64`; larger or no-scale → `decimal128` or `double`
- `DATE` / `TIMESTAMP` → `timestamp[us]`
- `XMLTYPE`, `SDO_GEOMETRY`, `INTERVAL *`, `ANYDATA`, `TIMESTAMP WITH TIME ZONE` → `string` via `TO_CHAR()`
- Per-column overrides in `config.yaml` can override both the SQL expression and Arrow type

### Output path convention
`<output_dir>/<schema_lower>/<table_lower>/<chunk>.<ext>` where non-alphanumeric chars in names are replaced with `_`. Chunk name patterns (`src/oracle_dmp_converter/planner.py`): `whole`, `partition-00001-P_NORTH`, and `subpartition-00001-00002-P_NORTH-SP_A` for composite-partitioned tables (one chunk per physical subpartition).

---

## Key env vars

| Var | Effect |
|---|---|
| `DMP_CONVERTER_CONTAINER_RUNTIME` | `docker` (default) or `podman` |
| `DMP_CONVERTER_IMAGE` | Override Oracle image tag |
| `DMP_CONVERTER_DOCKER_PLATFORM` | Override `--platform` (set `linux/amd64` on Apple Silicon) |

---

## Style conventions

- Line length: **100** characters (ruff + pylint)
- All models are **frozen dataclasses**; use `dataclasses.replace()` to derive new instances
- `from __future__ import annotations` in all source files
- Pylint disabled: `broad-exception-caught`, `duplicate-code`, `missing-*-docstring`, `too-many-*` — docstrings present but not enforced; "too-many" checks suppressed
- Circular import avoidance: `datapump/workflow.py` uses deferred imports inside function bodies with `# noqa: PLC0415`
- pytest runs with `--import-mode=importlib` (set in `pyproject.toml`)
- Ruff rules: `E`, `F`, `I` (isort), `UP` (pyupgrade), `B` (bugbear)

---

## CI

GitHub Actions, defined in `.github/workflows/`:

- **`ci.yml`** runs on every push and pull request (all branches) with five independent jobs:
  - `format-check` → `make format-check`
  - `lint` → `make lint`
  - `unit-tests` → `uv run pytest tests/unit --cov --cov-report=term-missing` (uploads `htmlcov/` as an artifact); fails if coverage drops below the 92% `fail_under` gate
  - `modern-integration-tests` → `tests/integration/test_data_modern_dump.py --no-cov` (30-min timeout)
  - `legacy-integration-tests` → `tests/integration/test_data_legacy_dump.py --no-cov` (30-min timeout)
- **`release.yml`** runs on published GitHub releases: `uv build`, then uploads the wheel and sdist as release assets.

No `.pre-commit-config.yaml`. The `Makefile` is the local task runner; CI calls the same `make` targets.
