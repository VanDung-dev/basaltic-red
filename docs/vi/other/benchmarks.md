---
title: Báo cáo hiệu năng & Benchmarks
description: Báo cáo đo kiểm hiệu năng vi mô và tải thực tế trên dữ liệu lớn
icon: material/chart-line
---

# Báo cáo hiệu năng & Benchmarks

Đo lường chi tiết trên tập dữ liệu NYC TLC Yellow Taxi 2025 (~4.3 triệu dòng mỗi tệp).

---

## 1. Tốc độ nhân lọc SIMD Bitmask

| Số quy tắc | Số dòng xử lý | Thời gian | Tốc độ thông lượng |
| :--- | :--- | :--- | :--- |
| **4 quy tắc** | 4,305,006 dòng | **0.352 s** | **12.2M dòng/s** |
| **20 quy tắc** | 4,305,006 dòng | **0.646 s** | **6.66M dòng/s** (133.3M checks/s) |
| **70 quy tắc** | 4,305,006 dòng | **1.125 s** | **3.82M dòng/s** (267.7M checks/s) |

---

## 2. Tốc độ kiểm tra Lake Doctor

| Quy mô dữ liệu | Quét hệ thống tệp (Python) | `basaltic-red` (`memmap2` `.br_map.ipc`) |
| :--- | :--- | :--- |
| **12 tệp (700 MB)** | ~1.2 s | **0.48 ms** |
| **42 tệp (2.5 GB)** | ~4.8 s | **0.52 ms** |

---

## 3. Luồng DataFusion SQL Pushdown

- Tính toán tổng hợp trên 4,305,006 dòng (`GROUP BY passenger_count`): **0.107 giây**.
