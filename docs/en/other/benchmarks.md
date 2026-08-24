---
title: Performance Benchmarks
description: In-depth microbenchmarks and end-to-end performance measurements
icon: material/chart-line
---

# Performance Benchmarks

Detailed performance measurements of `basaltic-red` on NYC TLC Yellow Taxi 2025 data (~4.3M rows per file).

---

## 1. Dynamic SIMD Filter Throughput

| Rules Count | Rows Filtered | Execution Time | Throughput |
| :--- | :--- | :--- | :--- |
| **4 rules** | 4,305,006 rows | **0.352 s** | **12.2M rows/s** |
| **20 rules** | 4,305,006 rows | **0.646 s** | **6.66M rows/s** (133.3M rule-checks/s) |
| **70 rules** | 4,305,006 rows | **1.125 s** | **3.82M rows/s** (267.7M rule-checks/s) |

---

## 2. Lake Doctor Health Check

| Data Files | Filesystem Walk (Python) | `basaltic-red` (`memmap2` `.br_map.ipc`) |
| :--- | :--- | :--- |
| **12 files (700 MB)** | ~1.2 s | **0.48 ms** |
| **42 files (2.5 GB)** | ~4.8 s | **0.52 ms** |

---

## 3. DataFusion SQL Stream Pushdown

- Aggregation of 4,305,006 rows (`GROUP BY passenger_count`): **0.107 seconds**.
