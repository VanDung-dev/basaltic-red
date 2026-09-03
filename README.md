# Basaltic-Red

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/Rust-1.80+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-58.4.0-red.svg)](https://crates.io/crates/arrow)
[![DataFusion](https://img.shields.io/badge/DataFusion-54.1.0-purple.svg)](https://crates.io/crates/datafusion)

> Data lake acceleration and data-quality toolkit in Rust and Apache Arrow with Python bindings via PyO3.

`basaltic-red` is not a database. It has no background daemon, network socket, or proprietary storage format. It is a companion toolkit designed to work with existing query engines like DuckDB, Polars, PyArrow, pandas, and DataFusion.

Utilities for file-based data lakes:
* Memory-mapped lake catalog (`.br_map.ipc`): loads metadata in under 0.5 ms via OS `mmap`, with automated drift detection (`br.lake.doctor`) and a terminal progress bar.
* Zero-copy slicing (`br.read`): reads row ranges and column projections without loading entire multi-gigabyte files into RAM.
* Parallel data-quality filtering (`br.filter`): multi-threaded dynamic rule validation with per-row `u64` audit bitmasks that separate clean from invalid rows.
* Embedded SQL execution (`br.sql`): runs in-memory DataFusion SQL queries over directories and hands RecordBatches to DuckDB or Polars without copying data.
* Custom format registration & sniffing (`br.formats`): detects file types via magic bytes and enables user-defined delimiters without recompiling.

Demo dataset in [`demo.ipynb`](demo.ipynb): NYC TLC Yellow Taxi 2009 to 2025, containing 204 Parquet files, 29.66 GB, and 1,826,960,642 rows by 20 columns.

---

## Comparison with traditional databases

| Feature | Basaltic-Red | Databases (ClickHouse, PostgreSQL) |
| :--- | :--- | :--- |
| Architecture | In-process Python extension (Rust cdylib) | Standalone server daemon process |
| Storage format | Open files (Parquet, Arrow IPC, CSV, JSON, Avro, ORC) | Internal table storage and WAL files |
| Network and ports | In-process via Arrow C Data Interface | TCP sockets and wire protocols |
| Role in ecosystem | Pre-processing, cataloging, quality auditing, slicing | Persistent storage and query serving |
| Interoperability | Direct zero-copy handoff to DuckDB, Polars, PyArrow | Client drivers and network serialization |

---

## Observed measurements

Numbers below are from a run of `demo.ipynb` on an Apple Silicon Mac. Results vary by hardware, filesystem cache, and data layout.

| Scenario | Scope | Observed in demo |
| :--- | :--- | :--- |
| Catalog inspection (cold vs warm) | 204 files | Cold scan and map build took ~18.07 s; warm `memmap2` read took ~0.5 ms (average over 5 runs). Warm path avoids traversing the directory tree. |
| Volume scan (metadata only) | 1,826,960,642 rows (36.5B cells) | Metadata read of row counts and file sizes completed in ~0.7 s. |
| Full-lake quality filter | 1,826,960,642 rows, 5 rules | Took ~21 s using Rayon parallel read and filter (`filter_files_parallel`), yielding 1,780,228,507 clean rows and 46,732,135 invalid rows. |
| Single-file SQL aggregation | 4,305,006 rows (one monthly batch) | `GROUP BY` via `execute_sql_stream` took ~0.1 s, followed by zero-copy handoff to DuckDB or Polars. |

Filtering uses plain Rust loops over Arrow arrays that LLVM auto-vectorizes. Audit codes are `u64` bitmasks per row (bit *i* corresponds to rule *i* failure, chunked when rules exceed 64).

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

# 1. Build or diagnose the catalog
map_path = br.lake.create_map("data", show_progress=True)
report = br.lake.doctor("data", auto_heal=True)
print(report["status"], report["total_files"])

# 2. Slice rows without reading the whole file
table = br.read.slice_rows("data/yellow_tripdata_2025-12.parquet", offset=0, limit=100)
df = pl.from_arrow(table)

# 3. Parallel filter with dynamic rules (returns summary statistics)
summary = br.filter.filter_files_parallel("data/yellow_tripdata_*.parquet", rules=[
    "passenger_count >= 1",
    "trip_distance > 0.0",
    "fare_amount > 0.0",
    "total_amount > 0.0",
])
print(summary)

# 4. Run SQL via DataFusion, then hand off to DuckDB or Polars
stream = br.sql.execute_sql_stream(
    "SELECT passenger_count, AVG(fare_amount) FROM 'data/yellow_tripdata_2025-12.parquet' GROUP BY passenger_count"
)
duck_rel = duckdb.from_arrow(stream.to_pyarrow())
print(duck_rel.df())
```

See `demo.ipynb` sections 0 to 6 for the full pipeline: download, doctor, schema, preview, filter, SQL, lake-map drift simulation, and final audit.

---

## Python API

| Group | Operation | Call |
| :--- | :--- | :--- |
| `lake` | Diagnose / heal catalog | `br.lake.doctor("data", auto_heal=True)` |
| `lake` | Build catalog | `br.lake.create_map("data", show_progress=True)` |
| `read` | Row slice | `br.read.slice_rows("file.parquet", offset=0, limit=100)` |
| `read` | Column projection | `br.read.slice_cols("file.parquet", selected_cols=["fare_amount", "trip_distance"], offset=0, limit=100)` |
| `filter` | In-memory filter (one file) | `clean, trash = br.filter.filter_matrix("file.parquet", rules=[...])` |
| `filter` | Parallel filter (many files) | `br.filter.filter_files_parallel("data/*.parquet", rules=[...])` |
| `sql` | DataFusion stream | `br.sql.execute_sql_stream("SELECT * FROM 'file.parquet'")` |
| `sql` | DataFusion execute | `br.sql.execute_sql("SELECT ...")` |
| `formats` | Custom delimited format | `br.formats.register_delimited(ext="dat", delimiter="|", has_header=True)` |

Full reference: `docs/en/reference/python-api.md`. Rules syntax: `docs/en/reference/rule-syntax.md`.

---

## License

Distributed under the [MIT License](LICENSE).
