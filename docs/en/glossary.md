---
title: Glossary
description: Quick-reference map of terms, source locations, and vocabulary — not narrative documentation
icon: material/book-alphabet
---

# Glossary

Quick lookup only. For explanations, follow the linked component pages.

## Source Map

| Term | Location | One-line meaning |
| :--- | :--- | :--- |
| `MatrixEngine` | `src/engine/mod.rs` | Central struct holding quality thresholds |
| Engine bindings | `src/pyapi/engine.rs` | `#[pymethods]` exposed to Python |
| `default_engine()` | `src/pyapi/mod.rs` | Process-wide engine singleton (`OnceLock`) |
| `BazanError` | `src/error.rs` | Error enum; mapped by `bazan_to_pyerr()` |
| Static filter | `src/engine/filter.rs` | Fixed-threshold fast path (`filter_batch_native`) |
| Audit bit flags | `src/filter.rs` | `ERR_INVALID_PASSENGER` / `_FARE` / `_SPEED` |
| Dynamic kernel | `src/engine/dynamic_filter.rs` | `FilterRule::parse` + multi-chunk bitmask |
| Slicing | `src/engine/slice.rs` | `slice_rows_native`, `slice_cols_native` |
| Parallel filter | `src/engine/parallel_filter.rs` | Rayon multi-file run, `ParallelFilterSummary` |
| Partition pruning | `src/engine/partition.rs` | Hive-style path parsing & file pruning |
| Splitter | `src/engine/splitter.rs` | `split_file_native` part writer |
| Ingest | `src/engine/ingest.rs` | Directory ingestion, Parquet normalization |
| Lake writers | `src/engine/formats/core/parquet.rs` | Clean/trash lake write, gold table, ZSTD |
| SQL layer | `src/engine/sql.rs` | DataFusion session, ListingTable vs MemTable, `.br_cache` |
| `PyBatchIterator` | `src/pyapi/iterator.rs` | Eager/Lazy batch source, `to_pyarrow()` |
| Format trait | `src/engine/formats/mod.rs` | `FormatHandler`, registries, magic-byte sniffer |
| Tier 1–3 handlers | `formats/core/`, `common/`, `plugins/adapters/` | Parquet/Feather · CSV & JSON families · XLSX/Avro/ORC/MsgPack |
| Row chunking | `formats/plugins/base_templates/row_chunker.rs` | Shared row→batch conversion template |
| Lake Map | `src/engine/map.rs` | `LakeMap`, `.br_map.ipc` IO, `doctor_lake_map` |
| Memory budget | `src/engine/memory.rs` | RAM cap, batch sizing, tokio/Rayon runtimes |
| CSV Guard | `src/engine/csv_guard.rs` | Formula-injection sanitizer |
| ER graphs | `src/engine/graph.rs` | Mermaid ER diagram generator |
| File discovery | `src/utils.rs` | Recursive, sorted, partition-aware walkers |

## Vocabulary

| Term | Definition |
| :--- | :--- |
| RecordBatch | Arrow columnar chunk — unit of streaming everywhere |
| `OpenedSource` | Schema + lazy batch iterator returned by a handler |
| Clean / Trash | Rows passing all rules vs violating ≥1 rule |
| Bitmask chunk | 64 rules per `u64`; rule *i* → bit `i % 64` of chunk `i / 64` |
| `audit_error_code` | `UInt64` bitmask of violated rules 0–63 on Trash rows |
| `audit_violated_rules` | `List<UInt32>` of all violated indices (rules > 64 only) |
| `.br_map.ipc` | Arrow IPC catalog at lake root (5 columns, see spec) |
| Doctor status | `HEALTHY` / `DRIFT_DETECTED` / `HEALED` |
| Drift classes | `modified_files`, `unindexed_files`, `missing_files` |
| Partition filter | Hive-style subtree selector, e.g. `year=2026/month=08` |
| Summary dict | `{total_files, pruned_dirs, total_rows, clean_rows, trash_rows}` |
| Native target | Extension DataFusion reads directly (pushdown enabled) |
| MemTable fallback | Non-native formats loaded to RAM before querying |

## Environment Variables

| Variable | Default | Effect |
| :--- | :--- | :--- |
| `BASALTIC_RED_MAX_RAM_GB` | `2` | Total streaming RAM budget |
| `BR_INGEST_NORMALIZE` | off | `1`/`true` normalizes row formats during ingest |
| `BASALTIC_RED_AUTO_NORMALIZE` | off | `1` enables the SQL-side transcode cache |
| `BASALTIC_RED_CACHE_DIR` | `<dir>/.br_cache` | Relocates the SQL transcode cache |
