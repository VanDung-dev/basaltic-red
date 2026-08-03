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

---

## relational-db-gen

Generate linked relational test databases (5 to 100 tables) with primary key (`PK`) and foreign key (`FK`) relationships across multi-format files (`users.parquet`, `products.csv`, `orders.parquet`, `order_items.csv`, `payments.parquet`).

### Quick start

```bash
cargo run -p relational-db-gen --release -- --help
```

### Usage

```bash
# Generate default 5 linked tables (users, products, orders, order_items, payments)
cargo run -p relational-db-gen --release -- --tables 5 --output-dir data/relational

# Generate 10 linked tables (custom table count from 5 to 100)
cargo run -p relational-db-gen --release -- --tables 10 --rows 500 --output-dir data/relational_10

# Visualize ER diagram using bazan CLI
bazan graph data/relational --output er_graph.md
```

### Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--tables` | `5` | Number of linked tables to generate (Min: 5, Max: 100) |
| `--rows` | `100` | Number of rows per generated table |
| `--output-dir` | `data/relational` | Target output directory |
