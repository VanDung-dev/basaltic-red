---
title: Troubleshooting & Optimization
description: Diagnosing common issues and maximizing data lake throughput
icon: material/wrench
---

# Troubleshooting & Optimization

## Common Issues & Fixes

### 1. Lake Status reports `DRIFT_DETECTED`
- **Cause**: Files were added, modified, or deleted without updating the catalog.
- **Fix**: Run `br.lake.doctor("data", auto_heal=True)` — the report's `modified_files` / `unindexed_files` / `missing_files` keys tell you exactly what drifted.

### 2. `ValueError: Unsupported file format '.xyz'`
- **Cause**: The extension is not registered and magic-byte sniffing failed.
- **Fix A**: Check `br.formats.list_formats()` for supported extensions (`csv, tsv, psv, txt, json, jsonl, ndjson, parquet, pq, feather, arrow, ipc, avro, xlsx, orc, msgpack`).
- **Fix B**: Register a custom handler: `br.formats.register_delimited(ext="xyz", delimiter="|")`.

### 3. `IOError: .parquet file is empty`
- **Cause**: `preview_sample` / filtering opened a file whose first batch has zero rows.
- **Fix**: Remove or re-generate the empty file; run `br.lake.doctor` to find it.

### 4. Rule seems ignored (all rows pass)
- **Cause**: The rule references a column that doesn't exist, an unsupported dtype (dates, decimals…), or a value that can't be parsed into the column type — such rules are silently skipped per [Rule Syntax](../reference/rule-syntax.md#evaluation-semantics).
- **Fix**: Verify column names via `br.read.slice_rows(path, 0, 1).schema`.

### 5. SQL fails on JSON files
- **Cause**: DataFusion expects newline-delimited objects; top-level `[...]` arrays take the MemTable fallback path.
- **Fix**: Prefer NDJSON for large SQL workloads.

### 6. CSV formula-injection warnings
- **Cause**: String cells begin with `=`, `+`, `@`, or a non-numeric `-`.
- **Behavior**: The CSV Guard neutralizes these cells by prefixing `'` when writing CSV output; numeric negatives like `-5.0` pass through untouched (see `src/engine/csv_guard.rs`).

## Optimization Tips

- **Partition pruning**: keep Hive-style layouts (`year=.../month=.../`) and pass `partition_filter=` to skip whole directories before IO.
- **RAM ceiling**: raise/lower the budget with `BASALTIC_RED_MAX_RAM_GB` (default 2).
- **Parallel width**: cap workers with `num_threads=` on `filter_files_parallel` to leave cores for other jobs.
- **Prefer Parquet**: native ListingTable pushdown (predicate + projection) only applies to Parquet/delimited/JSON/IPC targets in SQL.
