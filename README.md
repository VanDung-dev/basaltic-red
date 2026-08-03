# Basaltic-Red

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https.mit-license.org)
[![Rust](https://img.shields.io/badge/Rust-1.96+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-59.1.0-red.svg)](https://crates.io/crates/arrow)

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

# 3. Split giant matrix file into part files
parts_count = engine.split_file("data/sample.csv", max_rows_per_file=100000, output_dir="./parts", format="parquet")

# 4. Generate Mermaid ER Diagram
mermaid_code = engine.generate_er_graph_py("data/relational", output_path="er_graph.md")
```

---

## License

Distributed under the **[MIT License](LICENSE)**.
