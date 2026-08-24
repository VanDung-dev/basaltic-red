---
title: Lake Map Specification
description: Binary layout and column schema of .br_map.ipc files
icon: material/map
---

# Lake Map Specification

The Lake Map is serialized as an **Apache Arrow IPC file** named `.br_map.ipc` at the root of the data lake (`resolve_map_path()` in `src/engine/map.rs`). It is written by `br.lake.create_map()` and read back memory-mapped (<1 ms) by `doctor_lake_map`.

## RecordBatch Schema (single row per data file)

| Column | Arrow Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `rel_path` | `Utf8` | No | File path relative to the lake root |
| `size_bytes` | `UInt64` | No | File size in bytes |
| `mtime_ms` | `Int64` | No | Modification time, milliseconds since Unix epoch |
| `total_rows` | `UInt64` | No | Row count of the file |
| `stats_json` | `Utf8` | No | JSON: `{"total_rows": N, "columns": {"<name>": {"min": f64, "max": f64, "min_str": str, "max_str": str}}}` |

## Doctor Comparison Keys

An entry is *healthy* when all three of `size_bytes`, `mtime_ms`, and existence match the current filesystem. Any mismatch classifies the file as `modified`; files present on disk but absent from the catalog are `unindexed`; catalog entries without a backing file are `missing`.

## Aggregate Totals

The in-memory `LakeMap` struct additionally carries derived totals: `total_files`, `total_rows`, `total_bytes`.
