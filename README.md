# Basaltic-Red: Core SIMD Matrix Engine for BigData Lakehouses

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https.mit-license.org)
[![Rust](https://img.shields.io/badge/Rust-1.96+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-59.1.0-red.svg)](https://crates.io/crates/arrow)

Languages: [English](README.md) | [Tiếng Việt](README.vi.md)

---

## Project Overview

**Basaltic-Red** is a high-performance Python Native Extension written in **Rust (PyO3)** and **Apache Arrow**. It is designed to filter, split, and govern enterprise Big Data Parquet files at **500+ MB/s throughput** while guaranteeing a **bounded memory footprint of `< 2.0 GB RAM`**, even when processing Terabyte-scale datasets.

### Key Features
- **Deterministic SIMD Bitmask Engine**: Splits raw Parquet data into **Clean Matrix** and **Trash Matrix** at CPU native speed.
- **Audit Error Bitmask Flagging**: Attaches binary error codes (`0x01: Invalid Passenger`, `0x02: Invalid Fare`, `0x04: Speed Anomaly`) to trash records for 100% auditability.
- **Zero-Copy DuckDB 1.4.5 Preview**: Provides instant `< 10ms` SQL preview queries via PyCapsule Arrow zero-copy transfer.
- **Native Data Dictionary Generator**: Automatically inspects Parquet schemas (files or directories) and exports a clean Markdown Data Dictionary table.

---

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/vandungdev/basaltic-red.git
cd basaltic-red

# Setup Python environment and build Rust extension
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
maturin develop --release
```

---

## Basic Usage

### 1. Filter Parquet Data Lake
```python
import basaltic_red as br

# Initialize Engine with domain rules
engine = br.MatrixEngine(
    min_passenger=1,     # Valid passenger count: 1 to 9
    max_passenger=9,
    min_fare=0.01,       # Valid fare amount: >= $0.01
    max_speed_mph=100.0  # Valid speed limit: <= 100 mph
)

# Process entire Data Lake directory
num_files, total_rows, clean_rows, trash_rows = engine.process_and_write_lake(
    input_dir="data",
    clean_output_dir="output/clean_lake",
    trash_output_dir="output/trash_lake",
    partition_filter=None,
    batch_size=65536
)

print(f"Processed {total_rows:,} rows | Clean: {clean_rows:,} | Trash: {trash_rows:,}")
```

### 2. Export Data Dictionary Markdown Table
```python
import basaltic_red as br

engine = br.MatrixEngine()

# Export Data Dictionary table (accepts a single file or a directory)
engine.export_data_dictionary_md("data", "data_dictionary.md")
```

### 3. Read Clean & Trash Data with DuckDB
```python
import duckdb

con = duckdb.connect("matrix_warehouse.db")

# Query Clean Matrix
df_clean = con.execute("SELECT * FROM clean_matrix LIMIT 10").df()
print(df_clean)

# Query Trash Matrix with audit codes
df_trash = con.execute("SELECT passenger_count, fare_amount, audit_error_code FROM trash_matrix LIMIT 10").df()
print(df_trash)
```

---

## License

Distributed under the **[MIT License](LICENSE)**.
