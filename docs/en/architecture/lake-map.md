---
title: Binary Lake Map & Lake Doctor
description: The .br_map.ipc catalog, its Arrow schema, and the doctor diagnostic/healing loop
icon: material/map
---

# Binary Lake Map & Lake Doctor

Implemented in `src/engine/map.rs`. The Lake Map stores a pre-compiled Arrow IPC index, `.br_map.ipc`, at the root of the data lake. It contains file metadata for every supported format and physical row-group/column-chunk locations for Parquet. Lake Doctor still checks current file metadata on disk to detect drift.

---

## Catalog Lifecycle

```mermaid
stateDiagram-v2
    direction LR
    state "Building catalog" as Building
    state ".br_map.ipc saved" as Saved
    state "HEALTHY" as Healthy
    state "DRIFT_DETECTED" as Drift
    state "HEALED" as Healed

    [*] --> Building: br.lake.create_map()
    Building --> Saved: save_lake_map_ipc()
    Saved --> Healthy: doctor · entries match
    Saved --> Drift: doctor · modified / unindexed / missing
    Healthy --> Drift: files change on disk
    Drift --> Healed: doctor(auto_heal=True)
    Healed --> Healthy: catalog in sync again
```

- `build_lake_map()` walks the directory (via `discover_data_files`), reads each file's schema/row count and full-file min/max stats for supported numeric/string columns.
- For Parquet, the builder also reads the footer and records each row group's row range, compressed byte range, and per-column chunk ranges. This is metadata work; it does not create one byte offset per logical row.
- `save_lake_map_ipc()` serializes the map; `load_lake_map_ipc()` reads it back through a memory map.

## On-Disk Schema

| Column | Arrow Type | Description |
| :--- | :--- | :--- |
| `rel_path` | `Utf8` | Path relative to the lake root |
| `size_bytes` | `UInt64` | File size |
| `mtime_ms` | `Int64` | Modification time in **milliseconds** since Unix epoch |
| `total_rows` | `UInt64` | Row count |
| `stats_json` | `Utf8` | JSON blob: per-column `{min, max, min_str, max_str}` plus row count |
| `first_global_row` | `UInt64` | Starting row when files are ordered by relative path |
| `row_groups_json` | `Utf8` | Parquet row-group locations; `[]` for non-Parquet files |

The aggregate struct also carries `total_files`, `total_rows`, `total_bytes`.

## Location Resolution

For a healthy Parquet entry, `slice_rows` and `slice_cols` resolve the row-group ranges, select only the intersecting row groups, and pass their ordinals to the Parquet reader. When the source contains an OffsetIndex, the map records page locations and the reader can skip pages before the requested offset. `br.lake.locate_row()` exposes the global-row resolution without decoding data. A missing, legacy, or stale map falls back to the normal streaming reader; it is never used to read a modified file.

---

## Lake Doctor

`doctor_lake_map(dir_path, auto_heal)` compares the on-disk reality against the catalog:

| Report field | Meaning |
| :--- | :--- |
| `status` | `"HEALTHY"` \| `"DRIFT_DETECTED"` \| `"HEALED"` |
| `total_files` | Files seen on disk |
| `healthy_count` | Entries matching the catalog exactly (path + size + mtime) |
| `modified_files` | Known paths whose size/mtime changed |
| `unindexed_files` | New files missing from the catalog |
| `missing_files` | Indexed files no longer on disk |
| `healed` | Whether healing ran |

**Healing** rebuilds the entry list from what still exists (dropping `missing_files`, refreshing stats for modified/unindexed entries) and rewrites `.br_map.ipc`. Any file inspection error aborts the operation instead of producing a partial catalog. Status becomes `"HEALED"`. Without `auto_heal=True` the report is purely diagnostic.

```python
import basaltic_red as br

report = br.lake.doctor("data", auto_heal=False)
if report["status"] != "HEALTHY":
    report = br.lake.doctor("data", auto_heal=True)
```

Full command signatures: [`br.lake.*`](../reference/python-api.md#basaltic_redlake) · binary layout details: [Lake Map Specification](../reference/lake-map-spec.md).
