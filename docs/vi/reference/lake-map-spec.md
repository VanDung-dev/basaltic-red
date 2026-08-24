---
title: Đặc tả bản đồ nhị phân
description: Bố cục nhị phân và schema cột của tệp .br_map.ipc
icon: material/map
---

# Đặc tả Lake Map

Lake Map được serialize thành **tệp Apache Arrow IPC** tên `.br_map.ipc` tại gốc hồ dữ liệu (`resolve_map_path()` trong `src/engine/map.rs`). Được `br.lake.create_map()` ghi ra và `doctor_lake_map` đọc ngược qua memory-map (<1 ms).

## Schema RecordBatch (một dòng cho mỗi tệp dữ liệu)

| Cột | Kiểu Arrow | Nullable | Mô tả |
| :--- | :--- | :--- | :--- |
| `rel_path` | `Utf8` | Không | Đường dẫn tệp tương đối so với gốc lake |
| `size_bytes` | `UInt64` | Không | Dung lượng tính bằng byte |
| `mtime_ms` | `Int64` | Không | Thời điểm sửa đổi, mili-giây từ Unix epoch |
| `total_rows` | `UInt64` | Không | Số dòng của tệp |
| `stats_json` | `Utf8` | Không | JSON: `{"total_rows": N, "columns": {"<tên>": {"min": f64, "max": f64, "min_str": str, "max_str": str}}}` |

## Khóa so sánh của Doctor

Một entry *khỏe mạnh* khi cả ba thuộc tính `size_bytes`, `mtime_ms` và sự tồn tại đều khớp hệ thống tệp hiện tại. Mọi sai lệch xếp tệp vào `modified`; tệp có trên đĩa nhưng thiếu trong danh mục là `unindexed`; entry trong danh mục nhưng không còn tệp là `missing`.

## Tổng hợp

Struct `LakeMap` trong bộ nhớ còn mang các tổng số suy dẫn: `total_files`, `total_rows`, `total_bytes`.
