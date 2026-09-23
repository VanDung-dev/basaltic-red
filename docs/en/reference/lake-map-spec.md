---
title: Lake Map Specification
description: Binary layout and column schema of .br_map.bazan files
icon: material/map
---

# Lake Map Specification

The Lake Map is serialized as an **Apache Arrow IPC payload** in the system file `.br_map.bazan` at the root of the data lake (`resolve_map_path()` in `src/engine/map.rs`). It is written by `br.lake.create_map()` and can be loaded memory-mapped in sub-millisecond time. New maps contain Parquet row-group and ORC stripe locations, NDJSON/JSONL and JSON-array checkpoints, CSV/TSV/PSV/TXT row-block byte checkpoints, and Arrow IPC/Feather RecordBatch ordinals; legacy five-column maps remain readable but cannot accelerate slices. `.br_map.ipc` is reserved as an ignored legacy sidecar name. `doctor_lake_map` additionally walks current file metadata to detect drift.

New maps carry schema metadata identifying `bazan.kind=lake_map`, `bazan.version=1`, `bazan.payload=arrow_ipc`, and `bazan.map_schema=2`. The payload remains an ordinary Arrow IPC file; the `.bazan` suffix reserves the system-file namespace without taking `.ipc` away from user data.

## RecordBatch Schema (single row per data file)

| Column | Arrow Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `rel_path` | `Utf8` | No | File path relative to the lake root |
| `size_bytes` | `UInt64` | No | File size in bytes |
| `mtime_ms` | `Int64` | No | Modification time, milliseconds since Unix epoch |
| `total_rows` | `UInt64` | No | Row count of the file |
| `stats_json` | `Utf8` | No | JSON: `{"total_rows": N, "columns": {"<name>": {"min": f64, "max": f64, "min_str": str, "max_str": str}}}` |
| `first_global_row` | `UInt64` | No | Starting row in the path-sorted lake |
| `row_groups_json` | `Utf8` | No | JSON array of Parquet row groups, ORC stripes, NDJSON/JSONL/JSON-array blocks, CSV/TSV/PSV/TXT row blocks, or Arrow IPC/Feather RecordBatches; `[]` for other formats |

Each object contains `ordinal`, `first_row`, `row_count`, `first_byte`, `total_byte_size`, `compressed_size`, and `columns`. Parquet objects additionally contain column paths, compressed byte ranges, and optional page locations. ORC objects use `first_byte` and `total_byte_size` for native stripes. NDJSON, JSONL, CSV, TSV, PSV, and TXT objects use the same fields for 64K-row blocks; JSON-array objects use them for 64K top-level objects; delimited checkpoints are quote-safe. Arrow IPC/Feather objects use the ordinal and row range, with `first_byte`, `total_byte_size`, and `columns` empty because the reader uses Arrow IPC's native batch index. These are block/row-group/stripe/batch locations, not individual row byte offsets.

## Doctor Comparison Keys

An entry is *healthy* when all three of `size_bytes`, `mtime_ms`, and existence match the current filesystem. Any mismatch classifies the file as `modified`; files present on disk but absent from the catalog are `unindexed`; catalog entries without a backing file are `missing`.

## Aggregate Totals

The in-memory `LakeMap` struct additionally carries derived totals: `total_files`, `total_rows`, `total_bytes`.
