---
title: Bắt đầu nhanh
description: Hướng dẫn 5 phút làm chủ quy trình làm việc với basaltic-red
icon: material/lightning-bolt
---

# Hướng dẫn bắt đầu nhanh (5 phút)

Hướng dẫn này giúp bạn nắm bắt chu trình vận hành chuẩn của `basaltic-red`.

---

## Bước 1: Khởi tạo Data Lake với Bác sĩ chẩn đoán (Lake Doctor)

Luôn bắt đầu bằng lệnh `br.lake.doctor` khi thao tác với thư mục dữ liệu:

```python
import basaltic_red as br

# Chẩn đoán và tự động tạo/đồng bộ bản đồ .br_map.ipc
health = br.lake.doctor("data", auto_heal=True)
print("Báo cáo sức khỏe Data Lake:")
for k, v in health.items():
    print(f"  - {k:18s}: {v}")
```

---

## Bước 2: Cắt lát dữ liệu Zero-Copy

Đọc lát cắt dòng và cột từ tệp Parquet dung lượng lớn trong micro-giây:

```python
import polars as pl

# Đọc 100 dòng đầu tiên mà không tải toàn bộ tệp vào RAM
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

## Bước 4: Phân tích SQL Zero-Copy với DataFusion

Đẩy thực thi truy vấn SQL và chuyển tiếp trực tiếp sang DuckDB hoặc Polars:

```python
import duckdb

# Luồng thực thi DataFusion SQL
stream = br.sql.execute_sql_stream("SELECT passenger_count, AVG(fare_amount) AS avg_fare FROM 'data/output/clean_trips.parquet' GROUP BY passenger_count")

# Chuyển tiếp zero-copy sang DuckDB
duck_df = duckdb.from_arrow(stream.to_pyarrow()).df()
print(duck_df)
```
