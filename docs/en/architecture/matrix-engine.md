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
    pub max_speed_mph: f64,   // default 100.0
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
| `slice_rows(file_path, offset, limit)` | Reads one row range as a PyArrow Table. For Parquet it uses the row-group reader; for IPC/Feather it memory-maps via `memmap2`. |
| `slice_cols(file_path, selected_cols, offset, limit)` | Same, with column projection pushed into the reader (Parquet reads only the required column chunks). |
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

- **Static fast path** (`src/filter.rs` + `engine/filter.rs`): three fixed bit flags, passenger range, minimum fare, distance/fare anomaly, evaluated with Arrow compute kernels over whole columns (fully vectorized). Used by `process_batch` / `process_file` / `process_and_write_lake` / `preview_sample`. No rule parsing overhead; expects NYC-taxi-style columns (`passenger_count`, `fare_amount`, `trip_distance`) and ignores rows silently when a column is absent.
- **Dynamic kernel** (`engine/dynamic_filter.rs`): arbitrary user rules parsed from strings against any supported column type. See [SIMD Bitmask Kernel](simd-kernel.md).
