---
title: Lọc chất lượng dữ liệu
description: Quy trình lọc quy tắc động song song trên hồ dữ liệu Parquet quy mô lớn
icon: material/filter-variant
---

# Luồng lọc chất lượng dữ liệu

## Xử lý song song đa luồng với Rayon

Lọc đồng thời hàng chục tệp Parquet trên toàn bộ lõi CPU:

```python
import basaltic_red as br

rules = [
    "passenger_count > 0",
    "passenger_count <= 6",
    "trip_distance > 0.0",
    "fare_amount >= 2.5",
    "total_amount > 0.0",
]

# Lọc song song trên các tệp khớp pattern
summary = br.filter.filter_files_parallel("data/yellow_tripdata_2025-*.parquet", rules=rules)
print(f"Số tệp đã xử lý      : {summary['total_files']:,}")
print(f"Số thư mục bị cắt bỏ : {summary['pruned_dirs']:,}")
print(f"Tổng số dòng đánh giá : {summary['total_rows']:,}")
print(f"Bản ghi hợp lệ (Sạch) : {summary['clean_rows']:,}")
print(f"Bản ghi bất thường (Rác) : {summary['trash_rows']:,}")
```

## Tham số mở rộng

```python
# Giới hạn theo phân vùng Hive và số luồng worker
summary = br.filter.filter_files_parallel(
    "data/lakehouse",
    rules=rules,
    partition_filter="year=2026/month=08",
    num_threads=8,
)
```

`path_pattern` chấp nhận một tệp đơn, một thư mục (duyệt đệ quy) hoặc glob pattern. Xem thêm kiến trúc bên trong tại [Đường ống Lọc](../architecture/filtering-pipeline.md). Lưu ý lệnh này chỉ *đếm* số dòng clean/trash; dùng [`br.lake.process_and_write_lake`](partitioning.md) để ghi kết quả ra đĩa.
