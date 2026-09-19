---
title: Đặc tả bản đồ nhị phân
description: Bố cục nhị phân và schema cột của tệp .br_map.bazan
icon: material/map
---

# Đặc tả Lake Map

Lake Map được serialize thành **payload Apache Arrow IPC** trong tệp hệ thống `.br_map.bazan` tại gốc hồ dữ liệu (`resolve_map_path()` trong `src/engine/map.rs`). Được `br.lake.create_map()` ghi ra và đọc qua memory-map (<1 ms). Map mới có vị trí row group Parquet; map cũ 5 cột vẫn đọc được nhưng không tăng tốc slicing. `.br_map.ipc` được dành riêng làm tên sidecar cũ bị bỏ qua. `doctor_lake_map` vẫn kiểm tra metadata tệp hiện tại để phát hiện drift.

Map mới có schema metadata xác định `bazan.kind=lake_map`, `bazan.version=1`, `bazan.payload=arrow_ipc` và `bazan.map_schema=2`. Payload vẫn là tệp Arrow IPC thông thường; hậu tố `.bazan` dành riêng namespace tệp hệ thống mà không chiếm `.ipc` của data người dùng.

## Schema RecordBatch (một dòng cho mỗi tệp dữ liệu)

| Cột | Kiểu Arrow | Nullable | Mô tả |
| :--- | :--- | :--- | :--- |
| `rel_path` | `Utf8` | Không | Đường dẫn tệp tương đối so với gốc lake |
| `size_bytes` | `UInt64` | Không | Dung lượng tính bằng byte |
| `mtime_ms` | `Int64` | Không | Thời điểm sửa đổi, mili-giây từ Unix epoch |
| `total_rows` | `UInt64` | Không | Số dòng của tệp |
| `stats_json` | `Utf8` | Không | JSON: `{"total_rows": N, "columns": {"<tên>": {"min": f64, "max": f64, "min_str": str, "max_str": str}}}` |
| `first_global_row` | `UInt64` | Không | Dòng bắt đầu trong lake đã sắp xếp theo path |
| `row_groups_json` | `Utf8` | Không | Mảng JSON vị trí row group Parquet, gồm byte range của column chunk; `[]` với định dạng khác |

Mỗi row-group có `ordinal`, `first_row`, `row_count`, `first_byte`, `total_byte_size`, `compressed_size` và `columns`. Mỗi phần tử `columns` có path Parquet cùng `offset`, `length` byte nén và có thể có `pages` gồm `first_row`, `offset`, `length` nếu file có OffsetIndex. Đây là vị trí row-group/page, không phải byte offset của từng dòng.

## Khóa so sánh của Doctor

Một entry *khỏe mạnh* khi cả ba thuộc tính `size_bytes`, `mtime_ms` và sự tồn tại đều khớp hệ thống tệp hiện tại. Mọi sai lệch xếp tệp vào `modified`; tệp có trên đĩa nhưng thiếu trong danh mục là `unindexed`; entry trong danh mục nhưng không còn tệp là `missing`.

## Tổng hợp

Struct `LakeMap` trong bộ nhớ còn mang các tổng số suy dẫn: `total_files`, `total_rows`, `total_bytes`.
