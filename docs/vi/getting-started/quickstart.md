---
title: Bắt đầu nhanh
description: Hướng dẫn 5 phút làm chủ quy trình làm việc với basaltic-red
icon: material/lightning-bolt
---

# Hướng dẫn bắt đầu nhanh (5 phút)

Hướng dẫn này trình bày quy trình cơ bản của `basaltic-red`: tạo hoặc sửa Lake Map, đọc khoảng dòng, lọc chất lượng dữ liệu và truy vấn SQL.

---

## Bước 1: Tạo hoặc sửa Lake Map

`doctor(auto_heal=True)` tạo catalog nếu chưa có hoặc lập chỉ mục lại phần metadata lệch cho các file được discovery theo extension hỗ trợ. Phát hiện drift so sánh đường dẫn, dung lượng và thời gian sửa đổi thay vì hash nội dung; auto-heal đọc lại file mới hoặc đã sửa để dựng lại entry.

```python
import basaltic_red as br

# Tạo catalog nếu thiếu, hoặc lập chỉ mục lại metadata bị lệch
health = br.lake.doctor("data", auto_heal=True)
print("Báo cáo sức khỏe Data Lake:")
for k, v in health.items():
    print(f"  - {k:18s}: {v}")
```

---

## Bước 2: Đọc một khoảng dòng

Khi có Lake Map khỏe, slice Parquet dùng vị trí row group để bỏ qua các group không liên quan. Reader vẫn giải mã giá trị được yêu cầu; độ trễ tùy layout file và cache.

```python
import polars as pl

# Đọc 100 dòng bằng API slice có hỗ trợ map
arrow_table = br.read.slice_rows("data/yellow_tripdata_2025-01.parquet", offset=0, limit=100)
df = pl.from_arrow(arrow_table)
print(df.shape)  # (100, 20)
```

---

## Bước 3: Lọc chất lượng dữ liệu tốc độ cao (SIMD)

Định nghĩa các quy tắc kiểm tra và phân tách dữ liệu thành bảng Sạch (Clean) và Rác (Trash):

```python
rules = [
    "passenger_count > 0",
    "trip_distance > 0.0",
    "fare_amount >= 2.5",
    "total_amount > 0.0",
]

# Lọc ma trận trực tiếp trên bộ nhớ RAM
clean_batch, trash_batch = br.filter.filter_matrix(
    "data/yellow_tripdata_2025-01.parquet",
    rules=rules
)

clean_df = pl.from_arrow(clean_batch)
trash_df = pl.from_arrow(trash_batch)

print(f"Số dòng sạch (Clean): {clean_df.height:,}")
print(f"Số dòng rác (Trash) : {trash_df.height:,}")
```

---

## Bước 4: Phân tích SQL dạng stream với DataFusion

Đẩy thực thi truy vấn SQL và chuyển tiếp trực tiếp sang DuckDB hoặc Polars:

```python
import duckdb

# Luồng thực thi DataFusion SQL
stream = br.sql.execute_sql_stream("SELECT passenger_count, AVG(fare_amount) AS avg_fare FROM 'data/output/clean_trips.parquet' GROUP BY passenger_count")

# Bàn giao dữ liệu Arrow sang DuckDB
duck_df = duckdb.from_arrow(stream.to_pyarrow()).df()
print(duck_df)
```
