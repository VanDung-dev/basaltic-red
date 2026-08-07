# Basaltic-Red

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https.mit-license.org)
[![Rust](https://img.shields.io/badge/Rust-1.96+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-58.3.0-red.svg)](https://crates.io/crates/arrow)
[![DataFusion](https://img.shields.io/badge/DataFusion-54.1.0-purple.svg)](https://crates.io/crates/datafusion)

---

## Engine Performance & Memory Budget

**Basaltic-Red** is engineered for enterprise Big Data processing at `500+ MB/s` with a balanced, comfortable memory budget:
- **Default Bounded RAM**: `< 2048 MB` (2 GB) RAM - Ideal for high-throughput zero-copy SIMD streaming.

---

## Quick Start & Installation

```bash
# Clone repository
git clone https://github.com/vandungdev/basaltic-red.git
cd basaltic-red

# Setup Python environment and build Rust extension
uv sync --extra dev --extra interop
uv run maturin develop --release
```

---

## Python SDK Overview

**Basaltic-Red** is a Python SDK (**`import basaltic_red`**) powered by a high-speed Rust engine. The API is organized into namespaced submodules so every command lives under `basaltic_red.<group>.<command>`:

| Group | Operation | Python SDK (`import basaltic_red as br`) |
| :--- | :--- | :--- |
| `read` | **Slice Row Range** | `br.read.slice_rows("data.parquet", offset=100, limit=50)` |
| `read` | **Slice Column Projection** | `br.read.slice_cols("data.csv", selected_cols=["id", "email"], offset=0, limit=50)` |
| `read` | **Preview Sample** | `br.read.preview_sample("data.parquet", limit_rows=100)` |
| `filter` | **Dynamic Column Filter** | `clean_b, trash_b = br.filter.filter_matrix("data.csv", rules=["price >= 50.0"])` |
| `filter` | **Multi-Threaded Parallel Filter** | `summary = br.filter.filter_files_parallel("data/", rules=["age >= 18"])` |
| `filter` | **Stream Partition Pruning** | `summary = br.filter.filter_files_parallel("test_lakehouse", partition_filter="year=2026/month=08")` |
| `filter` | **Batch Bitmask Filter** | `clean_b, trash_b = br.filter.process_batch(record_batch)` |
| `sql` | **SQL Query Pushdown (DataFusion)** | `table = br.sql.execute_sql("SELECT id, salary FROM 'data/analytics'")` |
| `sql` | **SQL Stream** | `stream = br.sql.execute_sql_stream("SELECT * FROM 'data/analytics'")` |
| `lake` | **Split Matrix File** | `br.lake.split_file("data.csv", max_rows_per_file=100000, output_dir="./parts", format="parquet")` |
| `lake` | **Process & Write Lakehouse** | `br.lake.process_and_write_lake("in/", "clean/", "trash/", partition_filter=None, batch_size=65536)` |
| `lake` | **Generate Gold Table** | `br.lake.generate_gold_table("clean/", "gold/", table_version="v1", partition_filter=None, batch_size=65536)` |
| `dictionary` | **Export Data Dictionary** | `br.dictionary.export_data_dictionary_md("data.parquet", "schema.md")` |
| `graph` | **Generate ER Diagram** | `br.graph.generate_er_graph("data/relational", output_path="er.md")` |

> `MatrixEngine` remains available as `br.MatrixEngine()` for advanced use (custom filter thresholds).

### SQL Stream Interop (User-Side)

`execute_sql_stream` returns a `PyBatchIterator` exposing an Arrow bridge via `to_pyarrow()`. The ecosystem consumes it directly — no SDK wrappers:

```python
import polars as pl
import duckdb

stream = br.sql.execute_sql_stream("SELECT * FROM 'data/analytics'")
df = pl.from_arrow(stream.to_pyarrow())        # Polars DataFrame
rel = duckdb.from_arrow(stream.to_pyarrow())   # DuckDB relation
```

---

## Basic Python Example

```python
import basaltic_red as br

# 1. Zero-copy row slicing (Returns PyArrow Table)
table = br.read.slice_rows("data/sample.parquet", offset=100, limit=50)

# 2. Selected column projection slicing
cols_table = br.read.slice_cols("data/sample.csv", selected_cols=["id", "email"], offset=0, limit=50)

# 3. Execute ANSI SQL query with Apache DataFusion Pushdown on a directory tree
sql_result = br.sql.execute_sql("SELECT id, age, salary FROM 'data/analytics' WHERE age >= 18 ORDER BY salary DESC")

# 4. Split giant matrix file into part files
parts_count = br.lake.split_file("data/sample.csv", max_rows_per_file=100000, output_dir="./parts", format="parquet")

# 5. Generate Mermaid ER Diagram
mermaid_code = br.graph.generate_er_graph("data/relational", output_path="er_graph.md")
```

---

## License

Distributed under the **[MIT License](LICENSE)**.
