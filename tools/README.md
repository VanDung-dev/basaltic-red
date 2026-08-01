# Tools

## bigdata-gen

Generate large datasets (30 columns, 10M+ rows) in multiple formats.

### Quick start

```bash
cargo run -p bigdata-gen --release -- --help
```

### Usage

```bash
# All 13 formats, 100K rows each
cargo run -p bigdata-gen --release

# Single format
cargo run -p bigdata-gen --release -- csv --rows 10000000 --output data.csv

# Multiple formats to directory
cargo run -p bigdata-gen --release -- all --rows 10000000 --output-dir ./data

# Binary (no cargo prefix)
cargo build --release -p bigdata-gen
./target/release/bigdata-gen all --rows 10000000 --output-dir ./data
```

### Formats

| Argument / Alias | Output | Internal Module |
|------------------|--------|-----------------|
| `csv` | comma-separated | `csv.rs` |
| `tsv` | tab-separated | `tsv.rs` |
| `psv` | pipe-separated | `psv.rs` |
| `txt` | semicolon-separated text | `txt.rs` |
| `json` | Formatted JSON array | `json.rs` |
| `jsonl` | Single-line JSON array | `jsonl.rs` |
| `ndjson` | Newline Delimited JSON stream | `ndjson.rs` |
| `parquet` | Apache Parquet (ZSTD) | `parquet.rs` |
| `feather`, `arrow`, `ipc` | Arrow IPC file format | `feather.rs` |
| `avro` | Apache Avro | `avro.rs` |
| `xlsx` | Excel workbook (max 1M rows) | `xlsx.rs` |
| `orc` | Apache ORC | `orc.rs` |
| `msgpack` | MessagePack binary JSON | `msgpack.rs` |



Use `all` to generate every format.


| Flag | Default | Description |
|------|---------|-------------|
| `--rows` | `100000` | Rows per format |
| `--cols` | `30` | Number of columns (1 to 100, dynamic schema expansion) |
| `--output` | auto | File path (single format only) |
| `--output-dir` | `bigdata_output` | Directory (multiple formats) |
| `--seed` | `42` | Random seed |
| `-j`, `--jobs` | `4` | Max concurrent format generators |
| `--help` | | Display help menu and format details |


