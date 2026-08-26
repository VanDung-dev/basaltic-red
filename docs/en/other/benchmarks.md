---
title: Benchmarks
description: Measurements from demo.ipynb on the 17-year NYC TLC Yellow Taxi dataset
icon: material/chart-line
---

# Benchmarks

Measurements from a single run of [`demo.ipynb`](https://github.com/VanDung-dev/basaltic-red/blob/master/demo.ipynb) on the 17-year NYC TLC Yellow Taxi dataset (2009–2025, 204 Parquet files, 29.66 GB, 1,826,960,642 rows). Apple Silicon / macOS, `uv run --no-sync maturin develop --release`. Results vary by hardware and filesystem cache — indicative only.

---

## 1. Dataset size

| Metric | Value |
| :--- | :--- |
| Parquet files | 204 (2009–2025) |
| Columns | 20 |
| Rows | 1,826,960,642 |
| Cells | 36,539,212,840 |
| Disk size | 29.66 GB (30,371.6 MB) |

Counted via `pq.read_metadata` + `os.path.getsize` in the notebook; volume scan is metadata-only (no full data read).

---

## 2. Catalog inspection (`.br_map.ipc`)

`br.lake.doctor("data", auto_heal=True)` on first call walks the directory and builds `.br_map.ipc`; warm calls read it via `memmap2`.

| Mode | Files | Time (demo) |
| :--- | :--- | :--- |
| Cold (build) | 204 | ~18,068 ms |
| Warm (mmap, avg 5 runs) | 204 | ~0.5 ms |

Warm avoids the directory walk. The exact speedup depends on storage and whether the map is already built.

---

## 3. Full-lake quality filtering

`br.filter.filter_files_parallel("data/yellow_tripdata_*.parquet", rules=[5 rules])` — Rayon parallel read + filter.

Rules in demo: `passenger_count >= 1`, `trip_distance > 0.0`, `fare_amount > 0.0`, `total_amount > 0.0`, `total_amount <= 1000.0`.

```
Total files : 204
Total rows  : 1,826,960,642
Clean       : 1,780,228,507
Trash       : 46,732,135
Wall time   : ~21.1 s (from ExecuteTime 19:48:57.175 → 19:49:18.296)
```

Trash rows carry `audit_error_code` (first 64 rules as `u64` bitmask) and, when rules > 64, `audit_violated_rules` (`List<UInt32>`).

Filtering is plain Rust loops over Arrow arrays (LLVM auto-vectorizes); not hand-written SIMD intrinsics.

---

## 4. Single-file operations

One monthly batch (`yellow_tripdata_2025-12.parquet`, ~4,305,006 rows) — used for the SQL and slicing demos:

- Slicing: `br.read.slice_rows(..., offset=0, limit=100)` returns in milliseconds (no full read).
- SQL: `br.sql.execute_sql_stream("SELECT ... GROUP BY passenger_count")` — aggregation over the single file completes in ~0.1 s in the demo; handoff to DuckDB via `duckdb.from_arrow(stream.to_pyarrow())` is an Arrow FFI transfer.

No claims about pandas/Postgres baselines — comparison depends on query and hardware.
