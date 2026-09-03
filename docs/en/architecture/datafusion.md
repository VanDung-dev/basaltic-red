---
title: DataFusion SQL Layer
description: How br.sql resolves targets, plans queries, and streams Arrow batches to Python
icon: material/database-search
---

# DataFusion SQL Layer

`src/engine/sql.rs` embeds Apache DataFusion. Both commands accept a single SQL string where the `FROM` clause references a **file path or directory** (single-quoted):

```python
import basaltic_red as br

table  = br.sql.execute_sql("SELECT COUNT(*) FROM 'data/analytics'")       # collected Table
stream = br.sql.execute_sql_stream("SELECT * FROM 'data/analytics'")       # PyBatchIterator
```

---

## Target Resolution

Before planning, the engine registers the `FROM` target as a table named `br_target`. Two paths exist:

1. **Native ListingTable**, for extensions DataFusion reads itself:

    | Extension | DataFusion format |
    | :--- | :--- |
    | `parquet`, `pq` | ParquetFormat |
    | `csv`, `tsv`, `psv` | CsvFormat (delimiter-aware) |
    | `json`, `jsonl`, `ndjson` | JsonFormat (newline-delimited objects) |
    | `arrow`, `ipc`, `feather` | ArrowFormat |

    A *directory* of homogeneous extension registers as one ListingTable, enabling predicate & projection pushdown across all files.

2. **Fallback MemTable**, for formats without a native DataFusion reader (`xlsx`, `avro`, `orc`, `msgpack`, mixed directories) and for JSON files whose top level is an array (`[...]`). The file is read through the [format registry](formats.md), collected into a MemTable, then queried in memory.

!!! note "Top-level JSON arrays"

    DataFusion's `JsonFormat` expects newline-delimited objects. Files starting with `[` are detected by inspecting the first non-whitespace byte and routed to the streaming handler instead.

---

## Streaming vs Collecting

| Command | Return | Memory profile |
| :--- | :--- | :--- |
| `execute_sql(query)` | PyArrow `Table` | Fully collected |
| `execute_sql_stream(query)` | `PyBatchIterator` | Lazy, batches produced on demand |

### PyBatchIterator (`src/pyapi/iterator.rs`)

Two internal sources behind one class:

- **Lazy**, a live `SendableRecordBatchStream`; each `next()` pulls one batch from the tokio runtime (`memory::global_runtime()`).
- **Eager**, a pre-collected `Vec<RecordBatch>` (used when a MemTable fallback was required).

Python-facing API:

- iteration: `for batch in stream:` yields PyArrow RecordBatches
- `stream.to_pyarrow()` → complete PyArrow Table
- `repr(stream)` → `PyBatchIterator(batches=N, rows=M)` (counts known eagerly; filled during consumption for lazy streams)

The output feeds directly into Polars / DuckDB with no copy, see [Integrate with Polars & DuckDB](../how-to/integrate-polars-duckdb.md).

---

## What Gets Pushed Down

On native ListingTables, DataFusion decodes only the row groups and columns your plan needs:

```python
# Reads only passenger_count + fare_amount column chunks, prunes row groups by predicate
br.sql.execute_sql_stream(
    "SELECT passenger_count, AVG(fare_amount) "
    "FROM 'data/output/clean_trips.parquet' GROUP BY passenger_count"
)
```
