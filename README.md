# Basaltic-Red

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/Rust-1.80+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-58.4.0-red.svg)](https://crates.io/crates/arrow)
[![DataFusion](https://img.shields.io/badge/DataFusion-54.1.0-purple.svg)](https://crates.io/crates/datafusion)

> **Zero-copy Data Lake and data-quality engine in Rust + Apache Arrow (Python via PyO3)**

`basaltic-red` provides a Rust core for working with file-based data lakes: a memory-mapped catalog (`.br_map.ipc`), row/column slicing without full file reads, parallel data-quality filtering with per-row audit codes, and DataFusion SQL over Arrow batches. Python is a thin PyO3 layer; results are returned as `pyarrow.Table` / `RecordBatch` for use with Polars, DuckDB, pandas, etc.

Demo dataset in [`demo.ipynb`](demo.ipynb): **NYC TLC Yellow Taxi 2009–2025, 204 Parquet files, 29.66 GB, 1,826,960,642 rows × 20 columns** (verified by `pq.read_metadata` in the notebook).

---

## What the demo measures

Numbers below are from a single run of `demo.ipynb` on commodity hardware (Apple Silicon, macOS). They vary by machine, filesystem cache and dataset layout — treat them as indicative, not guarantees.

| Scenario | Scope | Observed in demo |
| :--- | :--- | :--- |
| **Catalog inspection (cold vs warm)** | 204 files | Cold scan + map build ~18.07 s → warm `memmap2` read ~0.5 ms (avg over 5 runs). Orders of magnitude faster because warm path avoids a directory walk. |
| **Volume scan (metadata only)** | 1,826,960,642 rows (36.5B cells) | Metadata crawl (row counts + file sizes) completes in under a second; reported as ~0.7 s in the notebook. |
| **Full-lake quality filter** | 1,826,960,642 rows, 5 rules | ~21 s end-to-end via Rayon parallel read + filter (`filter_files_parallel`). Summary: 1,780,228,507 clean / 46,732,135 trash on those 5 rules. |
| **Single-file SQL aggregation** | 4,305,006 rows (one monthly batch) | `GROUP BY` via `execute_sql_stream` completes in ~0.1 s; zero-copy handoff to DuckDB/Polars via `to_pyarrow()`. |

> Filtering uses plain Rust loops over Arrow arrays that LLVM auto-vectorizes; the "SIMD" label in older docs means auto-vectorized, not hand-written intrinsics. Audit codes are `u64` bitmasks per row (bit *i* = rule *i* violated, chunked for >64 rules).

---

## Installation

```bash
# From GitHub via uv
uv add "git+https://github.com/VanDung-dev/basaltic-red.git"

# With interop helpers (Polars, DuckDB, pandas, numpy)
uv add "basaltic-red[interop] @ git+https://github.com/VanDung-dev/basaltic-red.git"

# With notebook dependencies (Jupyter, matplotlib, seaborn)
uv add "basaltic-red[interop,notebook] @ git+https://github.com/VanDung-dev/basaltic-red.git"
```

Build from source:

```bash
git clone https://github.com/VanDung-dev/basaltic-red.git
cd basaltic-red
uv run --no-sync maturin develop --release
```

Requires Python 3.12+.

---

## Example

```python
import basaltic_red as br
import polars as pl
import duckdb

# 1. Diagnose / create the catalog. First call builds .br_map.ipc; later calls are mmap'd.
report = br.lake.doctor("data", auto_heal=True)
print(report["status"], report["total_files"])  # HEALTHY / HEALED / DRIFT_DETECTED

# 2. Slice rows without reading the whole file
table = br.read.slice_rows("data/yellow_tripdata_2025-12.parquet", offset=0, limit=100)
df = pl.from_arrow(table)

# 3. Filter with dynamic rules. Returns (clean, trash) Arrow tables; trash has audit_error_code.
summary = br.filter.filter_files_parallel("data/yellow_tripdata_*.parquet", rules=[
    "passenger_count >= 1",
    "trip_distance > 0.0",
    "fare_amount > 0.0",
    "total_amount > 0.0",
])
print(summary)  # {total_files, total_rows, clean_rows, trash_rows}

# 4. SQL over files via DataFusion, then hand off to DuckDB/Polars
stream = br.sql.execute_sql_stream(
    "SELECT passenger_count, AVG(fare_amount) FROM 'data/yellow_tripdata_2025-12.parquet' GROUP BY passenger_count"
)
duck_rel = duckdb.from_arrow(stream.to_pyarrow())
print(duck_rel.df())
```

See `demo.ipynb` sections 0–6 for the full pipeline: download → doctor → schema → preview → filter → SQL → lake-map drift simulation → final audit.

---

## Python API

| Group | Operation | Call |
| :--- | :--- | :--- |
| `lake` | Diagnose / heal catalog | `br.lake.doctor("data", auto_heal=True)` |
| `lake` | Build catalog | `br.lake.create_map("data")` |
| `read` | Row slice | `br.read.slice_rows("file.parquet", offset=0, limit=100)` |
| `read` | Column projection | `br.read.slice_cols("file.parquet", columns=["fare_amount", "trip_distance"])` |
| `filter` | In-memory filter (one file) | `clean, trash = br.filter.filter_matrix("file.parquet", rules=[...])` |
| `filter` | Parallel filter (many files) | `br.filter.filter_files_parallel("data/*.parquet", rules=[...])` |
| `sql` | DataFusion stream | `br.sql.execute_sql_stream("SELECT * FROM 'file.parquet'")` |
| `sql` | DataFusion execute | `br.sql.execute_sql("SELECT ...")` |
| `formats` | Custom delimited format | `br.formats.register_delimited(ext="dat", delimiter="|", has_header=True)` |

Full reference: `docs/en/reference/python-api.md`. Rules syntax: `docs/en/reference/rule-syntax.md`.

---

## License

Distributed under the **[MIT License](LICENSE)**.
