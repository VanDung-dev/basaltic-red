# Basaltic-Red

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https.mit-license.org)
[![Rust](https://img.shields.io/badge/Rust-1.96+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-59.1.0-red.svg)](https://crates.io/crates/arrow)

Languages: [English](README.md) | [Tiếng Việt](README.vi.md)

---

## Project Overview

**Basaltic-Red** is a high-performance Python Native Extension written in **Rust (PyO3)** and **Apache Arrow**. It is designed to filter, split, and govern enterprise Big Data Lakehouse files at `500+ MB/s` while guaranteeing a **bounded memory footprint of `< 2.0 GB` RAM.

### Key Features
- **Multi-Format Unified Streaming Engine**: Seamlessly processes **Parquet (`.parquet`, `.pq`)**, **CSV (`.csv`)**, **TSV (`.tsv`)**, **JSON (`.json`)**, and **NDJSON / JSON Lines (`.ndjson`, `.jsonl`)**.
- **Deterministic SIMD Bitmask Engine**: Splits raw Big Data into **Clean Matrix** and **Trash Matrix** at CPU native speed.
- **Audit Error Bitmask Flagging**: Attaches binary error codes (`0x01: Invalid Passenger`, `0x02: Invalid Fare`, `0x04: Speed Anomaly`) to trash records for 100% auditability.
- **Zero-Copy DuckDB 1.4.5 Preview**: Provides instant `< 10ms` SQL preview queries via PyCapsule Arrow zero-copy transfer.
- **Native Data Dictionary Generator**: Automatically inspects file and directory schemas and exports a clean Markdown Data Dictionary table.

---

## Supported Formats

| Format | Extensions | Streaming Engine |
| :--- | :--- | :--- |
| **Parquet** | `.parquet`, `.pq` | Parallel multi-threaded ZSTD Reader/Writer (Rayon) |
| **CSV** | `.csv` | Schema-inferred Arrow CSV Streaming Reader |
| **TSV** | `.tsv` | Utf8 safe Arrow TSV Reader |
| **JSON** | `.json` | Schema-inferred Arrow JSON Reader |
| **NDJSON / JSONL** | `.ndjson`, `.jsonl` | 1MB Bounded-memory Zero-Copy Line Streaming Reader |

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

### 1. Process Unified Files (CSV, TSV, JSON, NDJSON, Parquet)
```python
import basaltic_red as br

# Initialize Engine with domain rules
engine = br.MatrixEngine(
    min_passenger=1,     # Valid passenger count: 1 to 9
    max_passenger=9,
    min_fare=0.01,       # Valid fare amount: >= $0.01
    max_speed_mph=100.0  # Valid speed limit: <= 100 mph
)

# Process any supported format transparently
total_rows, clean_rows, trash_rows = engine.process_file("demo.ndjson", batch_size=65536)
print(f"Total: {total_rows:,} | Clean: {clean_rows:,} | Trash: {trash_rows:,}")
```

### 2. Process Entire Data Lake Directory
```python
num_files, total_rows, clean_rows, trash_rows = engine.process_and_write_lake(
    input_dir="data",
    clean_output_dir="output/clean_lake",
    trash_output_dir="output/trash_lake",
    partition_filter=None,
    batch_size=65536
)
print(f"Scanned {num_files} files | Total: {total_rows:,} | Clean: {clean_rows:,} | Trash: {trash_rows:,}")
```

### 3. Export Data Dictionary Markdown Table
```python
engine = br.MatrixEngine()

# Export Data Dictionary table (accepts a single file or a directory)
engine.export_data_dictionary_md("demo.parquet", "data_dictionary.md")
```

---

## License

Distributed under the **[MIT License](LICENSE)**.
