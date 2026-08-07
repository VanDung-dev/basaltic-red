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

**Basaltic-Red** is a Python SDK (**`import basaltic_red`**) powered by a high-speed Rust engine:

| Operation | Python SDK (`import basaltic_red`) |
| :--- | :--- |
| **Slice Row Range** | `engine.slice_rows("data.parquet", offset=100, limit=50)` |
| **Slice Column Projection** | `engine.slice_cols("data.csv", selected_cols=["id", "email"], offset=0, limit=50)` |
| **Dynamic Column Filter** | `clean_b, trash_b = engine.filter_matrix("data.csv", rules=["price >= 50.0"])` |
| **Multi-Threaded Parallel Filter** | `summary = engine.filter_files_parallel("data/", rules=["age >= 18"])` |
| **Stream Partition Pruning** | `summary = engine.filter_files_parallel("test_lakehouse", partition_filter="year=2026/month=08")` |
| **SQL Query Pushdown (DataFusion)** | `table = engine.execute_sql("SELECT id, salary FROM 'data/analytics'")` |
| **Split Matrix File** | `engine.split_file("data.csv", max_rows_per_file=100000, output_dir="./parts", format="parquet")` |
| **Export Data Dictionary** | `engine.export_data_dictionary_md("data.parquet", "schema.md")` |
| **Generate ER Diagram** | `engine.generate_er_graph_py("data/relational", output_path="er.md")` |

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

# 3. Execute ANSI SQL query with Apache DataFusion Pushdown on a directory tree
sql_result = engine.execute_sql("SELECT id, age, salary FROM 'data/analytics' WHERE age >= 18 ORDER BY salary DESC")

# 4. Split giant matrix file into part files
parts_count = engine.split_file("data/sample.csv", max_rows_per_file=100000, output_dir="./parts", format="parquet")

# 5. Generate Mermaid ER Diagram
mermaid_code = engine.generate_er_graph_py("data/relational", output_path="er_graph.md")
```

---

## License

Distributed under the **[MIT License](LICENSE)**.
