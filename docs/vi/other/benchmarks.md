---
title: Benchmarks
description: Kết quả đo từ demo.ipynb trên bộ dữ liệu 17 năm NYC TLC Yellow Taxi
icon: material/chart-line
---

# Benchmarks

Kết quả từ một lần chạy [`demo.ipynb`](https://github.com/VanDung-dev/basaltic-red/blob/master/demo.ipynb) trên bộ 17 năm NYC TLC Yellow Taxi (2009–2025, 204 file Parquet, 29.66 GB, 1,826,960,642 dòng). Máy Apple Silicon / macOS, `uv run --no-sync maturin develop --release`. Tùy phần cứng và cache — chỉ tham khảo.

---

## 1. Quy mô dữ liệu

| Chỉ số | Giá trị |
| :--- | :--- |
| Số file Parquet | 204 (2009–2025) |
| Số cột | 20 |
| Số dòng | 1,826,960,642 |
| Số ô | 36,539,212,840 |
| Dung lượng | 29.66 GB (30,371.6 MB) |

Đếm bằng `pq.read_metadata` + `os.path.getsize` trong notebook; quét khối lượng chỉ đọc metadata, không đọc toàn bộ dữ liệu.

---

## 2. Kiểm tra catalog (`.br_map.ipc`)

`br.lake.doctor("data", auto_heal=True)` lần đầu quét thư mục và tạo `.br_map.ipc`; lần sau đọc qua `memmap2`.

| Chế độ | Số file | Thời gian (demo) |
| :--- | :--- | :--- |
| Cold (tạo mới) | 204 | ~18,068 ms |
| Warm (mmap, trung bình 5 lần) | 204 | ~0.5 ms |

Warm tránh quét thư mục. Mức tăng tốc phụ thuộc storage và việc map đã tồn tại hay chưa.

---

## 3. Lọc chất lượng toàn lake

`br.filter.filter_files_parallel("data/yellow_tripdata_*.parquet", rules=[5 rule])` — đọc song song + lọc Rayon.

Rule trong demo: `passenger_count >= 1`, `trip_distance > 0.0`, `fare_amount > 0.0`, `total_amount > 0.0`, `total_amount <= 1000.0`.

```
Tổng file : 204
Tổng dòng : 1,826,960,642
Clean     : 1,780,228,507
Trash     : 46,732,135
Thời gian : ~21.1 s (ExecuteTime 19:48:57.175 → 19:49:18.296)
```

Dòng trash mang `audit_error_code` (64 rule đầu dạng bitmask `u64`) và khi >64 rule có thêm `audit_violated_rules` (`List<UInt32>`).

Vòng lặp lọc là Rust thuần trên Arrow array do LLVM tự vector hóa; không phải intrinsic SIMD viết tay.

---

## 4. Thao tác trên một file

Một batch tháng (`yellow_tripdata_2025-12.parquet`, ~4,305,006 dòng) dùng cho demo SQL và slicing:

- Cắt lát: `br.read.slice_rows(..., offset=0, limit=100)` xong trong mili-giây (không đọc toàn bộ file).
- SQL: `br.sql.execute_sql_stream("SELECT ... GROUP BY passenger_count")` — aggregation trên một file xong ~0.1 s trong demo; bàn giao sang DuckDB qua `duckdb.from_arrow(stream.to_pyarrow())` là truyền qua Arrow FFI.

Không so sánh với pandas/Postgres — kết quả phụ thuộc truy vấn và phần cứng.
