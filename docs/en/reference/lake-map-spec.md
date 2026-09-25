---
title: Lake Map Specification
description: Binary layout and column schema of .br_map.bazan files
icon: material/map
---

# Lake Map Specification

The Lake Map is serialized as an **Apache Arrow IPC payload** in the system file `.br_map.bazan` at the root of the data lake (`resolve_map_path()` in `src/engine/map.rs`). It is written by `br.lake.create_map()` and loaded through a memory map; load time depends on map size and the storage system. Map creation and Lake Doctor use extension-based discovery: they include files with built-in or currently registered dynamic handlers and omit extensionless files, even though direct file reads may sniff some of them. New maps contain Parquet row-group, ORC stripe, Avro OCF block, MsgPack object-block, and XLSX logical row-block locations, NDJSON/JSONL and JSON-array checkpoints, CSV/TSV/PSV/TXT row-block byte checkpoints, and Arrow IPC/Feather RecordBatch ordinals; legacy five-column maps remain readable but cannot accelerate slices. `.br_map.ipc` is reserved as an ignored legacy sidecar name. `doctor_lake_map` additionally walks current file metadata to detect drift.

New maps carry schema metadata identifying `bazan.kind=lake_map`, `bazan.version=1`, `bazan.payload=arrow_ipc`, and `bazan.map_schema=3`. Schema 3 also stores `bazan.checkpoint_stride_rows`, `bazan.fingerprint`, and `bazan.stats_columns`. The payload remains an ordinary Arrow IPC file; the `.bazan` suffix reserves the system-file namespace without taking `.ipc` away from user data. The loader keeps compatibility with five-column legacy maps and seven-column schema 2 maps, applying the previous defaults to both.

## RecordBatch Schema (single row per data file)

| Column | Arrow Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `rel_path` | `Utf8` | No | File path relative to the lake root |
| `size_bytes` | `UInt64` | No | File size in bytes |
| `mtime_ms` | `Int64` | No | Modification time, milliseconds since Unix epoch |
| `total_rows` | `UInt64` | No | Row count of the file |
| `stats_json` | `Utf8` | No | JSON: `{"total_rows": N, "columns": {"<name>": {"min": f64, "max": f64, "min_str": str, "max_str": str}}}` |
| `first_global_row` | `UInt64` | No | Starting row in the path-sorted lake |
| `row_groups_json` | `Utf8` | No | JSON array of Parquet row groups, ORC stripes, Avro OCF blocks, MsgPack object blocks, XLSX logical row blocks, JSON/JSONL/NDJSON line or array blocks, CSV/TSV/PSV/TXT row blocks, or Arrow IPC/Feather RecordBatches; `[]` for other formats |
| `content_hash` | `Utf8` | Yes | BLAKE3 digest when `fingerprint="blake3"`; null for metadata-only maps |

Saved options control map construction: `checkpoint_stride_rows` must be positive and affects row-oriented checkpoint formats; `fingerprint` is `metadata` or `blake3`; `stats_columns` is null for all currently supported statistic types, an empty list for no statistics, or a list of selected column names. Disabling statistics does not skip the file scan needed to count rows and build locators. Native Parquet row groups, ORC stripes, Avro blocks, Arrow batches, and XLSX logical blocks are not resized by the stride.

Each object contains `ordinal`, `first_row`, `row_count`, `first_byte`, `total_byte_size`, `compressed_size`, and `columns`. Parquet objects additionally contain column paths, compressed byte ranges, and optional page locations. ORC objects use `first_byte` and `total_byte_size` for native stripes. Avro objects use `first_byte`, `total_byte_size`, and `compressed_size` for native OCF data blocks. MsgPack objects use `first_byte` and `total_byte_size` for object checkpoints at the configured stride. XLSX objects use `first_row` and `row_count` for logical data-row blocks at the configured stride; `first_byte`, `total_byte_size`, `compressed_size`, and `columns` remain empty because worksheet XML is usually Deflate-compressed inside ZIP. NDJSON and line-oriented JSONL objects use the same fields for row blocks at the configured stride; JSON-array objects use them for top-level object blocks at that stride; delimited checkpoints are quote-safe and ignore empty physical lines. The default stride is 65,536 rows. For `.jsonl`, the first non-whitespace byte selects array (`[`) or line-oriented checkpoints. Arrow IPC/Feather objects use the ordinal and row range, with `first_byte`, `total_byte_size`, and `columns` empty because the reader uses Arrow IPC's native batch index. These are block/row-group/stripe/batch locations, not individual row byte offsets. When a dynamic handler overrides a registered extension, `row_groups_json` is `[]`; map-backed slicing uses the registered handler instead of a built-in locator.

For Parquet slices, the map selects row groups and the row offset within the first selected group. When the source file has an OffsetIndex, the Parquet Arrow reader loads it optionally and can skip unselected data pages while applying that offset and limit. Slice does not consume the page locations serialized in the map; the reader loads the index from the Parquet file. Without an OffsetIndex, the slice remains correct but may read more data within the selected row groups. Column projection in row-oriented formats changes the returned columns but does not skip the unselected field bytes within each selected record; some readers also decode the full selected row before projection.

The same content-shape rule applies to `.json`: when it contains JSON objects per line, Lake Map uses line checkpoints. `locate_row` resolves against the saved catalog and does not check source freshness; run Lake Doctor after source or file-set changes. BLAKE3 plus Doctor detects same-size, same-mtime content changes, while slices only check size and modification time.

## Doctor Comparison Keys

An entry is *healthy* when its existence and selected fingerprint match the current filesystem. Metadata mode compares `size_bytes` and `mtime_ms`; it does not detect same-size content changes that preserve the recorded modification time. BLAKE3 mode also compares the content digest and detects those changes during Doctor. Any mismatch classifies the file as `modified`; files present on disk but absent from the catalog are `unindexed`; catalog entries without a backing file are `missing`. Slices retain the fast size/mtime check and do not hash the entire file per range, so run Doctor before slicing when you need BLAKE3 drift detection.

## Aggregate Totals

The in-memory `LakeMap` struct additionally carries derived totals: `total_files`, `total_rows`, `total_bytes`. Entries sort by relative path before `first_global_row` is assigned, so mixed-format lakes use the same stable path order.

`stats_json` currently records non-null minimum and maximum values for Arrow `Int64`, `Float64`, and `Utf8` columns only. Numeric values use `min`/`max`; UTF-8 strings use lexicographic `min_str`/`max_str`. Other Arrow types and columns with no non-null values are omitted. `Int64` extrema are converted to JSON `f64`, so values outside the exactly representable integer range (±2^53) may be rounded. These statistics are descriptive metadata and do not guarantee exact integer predicates.
