---
title: MatrixEngine Core
description: The central engine struct, quality thresholds, error taxonomy, and slicing primitives
icon: material/engine
---

# MatrixEngine Core

`MatrixEngine` (`src/engine/mod.rs`) is the single struct behind every command in the SDK. It carries the four data-quality thresholds used by the static fast-path filter and `preview_sample`:

```rust
pub struct MatrixEngine {
    pub min_passenger: i64,   // default 1
    pub max_passenger: i64,   // default 9
    pub min_fare: f64,        // default 0.01
    pub max_speed_mph: f64,   // default 100.0; used when taxi timestamps exist
}
```

## Construction

```python
import basaltic_red as br

# Shared singleton behind all br.<group>.* commands (thresholds 1, 9, 0.01, 100.0)
br.read.slice_rows(...)

# Custom thresholds for advanced use
engine = br.MatrixEngine(
    min_passenger=0,
    max_passenger=20,
    min_fare=-5.0,
    max_speed_mph=200.0,
)
```

`br.MatrixEngine(...)` accepts the same arguments positionally or by keyword. Thresholds determine static validation filtering criteria. The shared instance is created once per process via a `OnceLock`, so thresholds never drift between sub-commands.

---

## Slicing Primitives

Implemented in `src/engine/slice.rs`; exposed through [`br.read.*`](../reference/python-api.md#basaltic_redread):

| Method | Behavior |
| :--- | :--- |
| `slice_rows(file_path, offset, limit)` | Parquet resolves healthy `.br_map.bazan` row groups; NDJSON seeks to the indexed row block; both then apply the local offset/limit. Other formats stream and skip batches. IPC/Feather sources use `memmap2` where supported. |
| `slice_cols(file_path, selected_cols, offset, limit)` | Same location resolution. Parquet pushes projection into the reader; NDJSON seeks to its row block, reads the bounded range, then projects the requested columns. |
| `preview_sample(file_path, limit_rows)` | Opens the first batch only and runs the **static** threshold filter; returns `(clean_table, trash_table)`. |

Both slice methods resolve the handler through the [format registry](formats.md), so they work on every supported format, not just Parquet.

---

## Error Taxonomy

All engine failures flow through one enum, `BazanError` in `src/error.rs`, mapped to Python by a single function (`src/pyapi/mod.rs`):

| Rust variant | Python exception | Typical cause |
| :--- | :--- | :--- |
| `UnsupportedFormat(_)` | `ValueError` | Extension not registered and sniffing failed |
| `DataFusion(_)` | `RuntimeError` | SQL parse/execution failure |
| everything else (`Message`, IO, Arrow) | `IOError` | Missing files, empty files, corrupt data |

Rule-syntax problems raised before execution are surfaced as `ValueError` directly from the rule parser.

---

## Static vs Dynamic Filtering

- **Static fast path** (`src/filter.rs` + `engine/filter.rs`): three fixed bit flags, passenger range, minimum fare, and speed/distance-fare anomaly. Passenger counts support the `Int64` and `Float64` forms present in the TLC data. When pickup/dropoff timestamps exist, speed is computed from distance and duration; other schemas retain the legacy distance/fare fallback. Used by `process_batch` / `process_file` / `process_and_write_lake` / `preview_sample`.
- **Dynamic kernel** (`engine/dynamic_filter.rs`): arbitrary user rules parsed from strings against any supported column type. See [SIMD Bitmask Kernel](simd-kernel.md).
