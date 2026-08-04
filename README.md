# Basaltic-Red

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https.mit-license.org)
[![Rust](https://img.shields.io/badge/Rust-1.96+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-58.3.0-red.svg)](https://crates.io/crates/arrow)
[![DataFusion](https://img.shields.io/badge/DataFusion-54.1.0-purple.svg)](https://crates.io/crates/datafusion)

---

## Engine Performance & Memory Budget

**Basaltic-Red** & **`bazan` CLI** are engineered for enterprise Big Data processing at `500+ MB/s` with a balanced, comfortable memory budget:
- **Default Bounded RAM**: `< 2048 MB` (2 GB) RAM - Ideal for high-throughput zero-copy SIMD streaming.

---

## Quick Start & Installation

```bash
# Clone repository
git clone https://github.com/vandungdev/basaltic-red.git
cd basaltic-red

# Setup Python environment and build Rust extension
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
maturin develop --release

# Build standalone bazan CLI executable
cargo build --release --bin bazan
```

---

## `bazan` CLI & Python SDK Equivalents

**Basaltic-Red** provides dual interfaces: high-speed Terminal CLI (**`bazan`**) and Python SDK (**`import basaltic_red`**).

| Action / Operation | Terminal CLI (`bazan`) | Python SDK Equivalent (`import basaltic_red`) |
| :--- | :--- | :--- |
| **Slice Row Range** | `bazan slice-rows data.parquet --offset 100 --limit 50` | `engine.slice_rows("data.parquet", offset=100, limit=50)` |
| **Slice Column Projection** | `bazan slice-cols data.csv --cols id,email --limit 50` | `engine.slice_cols("data.csv", selected_cols=["id", "email"], offset=0, limit=50)` |
| **Dynamic Column Filter** | `bazan filter data.csv --rule "price >= 50.0"` | `clean_b, trash_b = engine.filter_matrix("data.csv", rules=["price >= 50.0"])` |
| **Multi-Threaded Parallel Filter** | `bazan filter "data/**/*.parquet" --rule "age >= 18" --threads 8` | `summary = engine.filter_files_parallel("data/", rules=["age >= 18"])` |
| **Stream Partition Pruning** | `bazan filter test_lakehouse -p "year=2026/month=08" --rule "age >= 18"` | `summary = engine.filter_files_parallel("test_lakehouse", partition_filter="year=2026/month=08")` |
| **Pack `.bazan` Container** | `bazan pack input_dir/ --output lakehouse.bazan` | `count, bytes_written = engine.pack_directory("input_dir", "lakehouse.bazan")` |
| **Inspect `.bazan` Container** | `bazan inspect lakehouse.bazan` | `manifest = br.read_bazan_manifest("lakehouse.bazan")` |
| **SQL Query Pushdown (DataFusion)** | `bazan sql "SELECT id, salary FROM 'lakehouse.bazan' WHERE age >= 18 ORDER BY salary DESC"` | `table = engine.execute_sql("SELECT id, salary FROM 'lakehouse.bazan'")` |
| **Split Matrix File** | `bazan split data.csv --max-rows 100000 --output-dir ./parts` | `engine.split_file("data.csv", max_rows_per_file=100000, output_dir="./parts", format="parquet")` |
| **Preview Top N Rows** | `bazan preview data.parquet --limit 20` | `engine.slice_rows("data.parquet", offset=0, limit=20)` |
| **Export Data Dictionary** | `bazan dict data.parquet --output schema.md` | `engine.export_data_dictionary_md("data.parquet", "schema.md")` |
| **Generate ER Diagram** | `bazan graph data/relational --output er.md` | `engine.generate_er_graph_py("data/relational", output_path="er.md")` |

---

## Basic Python Example

```python
import basaltic_red as br

# Initialize Engine
engine = br.MatrixEngine()

# 1. Zero-copy row slicing (Returns PyArrow Table)
table = engine.slice_rows("data/sample.parquet", offset=100, limit=50)

# 2. Selected column projection slicing
cols_table = engine.slice_cols("data/sample.csv", selected_cols=["id", "email"], offset=0, limit=50)

# 3. Pack database directory into single .bazan container
count, size = engine.pack_directory("test_hive_lakehouse", "lakehouse.bazan")

# 4. Execute ANSI SQL query with Apache DataFusion Pushdown
sql_result = engine.execute_sql("SELECT id, age, salary FROM 'lakehouse.bazan' WHERE age >= 18 ORDER BY salary DESC")

# 5. Split giant matrix file into part files
parts_count = engine.split_file("data/sample.csv", max_rows_per_file=100000, output_dir="./parts", format="parquet")

# 6. Generate Mermaid ER Diagram
mermaid_code = engine.generate_er_graph_py("data/relational", output_path="er_graph.md")
```

---

## License

Distributed under the **[MIT License](LICENSE)**.
