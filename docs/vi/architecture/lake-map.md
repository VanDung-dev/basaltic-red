---
title: Lake Map & Lake Doctor
description: Danh mục nhị phân .br_map.bazan, schema Arrow của nó và vòng chẩn đoán/tự chữa lành
icon: material/map
---

# Binary Lake Map & Lake Doctor

Cài đặt trong `src/engine/map.rs`. Lake Map lưu một payload Arrow IPC đã biên dịch sẵn trong tệp hệ thống `.br_map.bazan` tại gốc hồ dữ liệu. Nó lưu metadata cho mọi định dạng, vị trí row-group/column-chunk của Parquet, checkpoint byte theo block dòng của NDJSON và ordinal RecordBatch của Arrow IPC/Feather. Chỉ `.br_map.bazan` được load; `.br_map.ipc` bị bỏ qua như tên sidecar cũ. Lake Doctor vẫn kiểm tra metadata tệp hiện tại trên đĩa để phát hiện drift.

---

## Vòng đời danh mục

```mermaid
stateDiagram-v2
    direction LR
    state "Đang dựng danh mục" as Building
    state "Đã lưu .br_map.bazan" as Saved
    state "HEALTHY" as Healthy
    state "DRIFT_DETECTED" as Drift
    state "HEALED" as Healed

    [*] --> Building: br.lake.create_map()
    Building --> Saved: save_lake_map_ipc()
    Saved --> Healthy: doctor · entry khớp hết
    Saved --> Drift: doctor · modified / unindexed / missing
    Healthy --> Drift: tệp thay đổi trên đĩa
    Drift --> Healed: doctor(auto_heal=True)
    Healed --> Healthy: danh mục đồng bộ trở lại
```

- `build_lake_map()` duyệt thư mục (qua `discover_data_files`), đọc schema/số dòng và thống kê min/max toàn file cho các cột số/chuỗi được hỗ trợ.
- Với Parquet, builder còn đọc footer và lưu khoảng dòng, khoảng byte nén của từng row group cùng khoảng byte của từng column chunk. Đây không phải một byte offset cho từng dòng logic.
- Với NDJSON, builder lưu block 64K dòng cùng dòng logic đầu tiên, byte đầu tiên và độ dài byte. Nó không tạo offset cho từng dòng.
- Với Arrow IPC/Feather, builder lưu ordinal và khoảng dòng của từng RecordBatch. Khi đọc, hệ thống dùng random access batch index native của Arrow IPC thay vì offset cho từng dòng.
- `save_lake_map_ipc()` serialize bản đồ; `load_lake_map_ipc()` đọc ngược qua memory map.

## Schema trên đĩa

| Cột | Kiểu Arrow | Mô tả |
| :--- | :--- | :--- |
| `rel_path` | `Utf8` | Đường dẫn tương đối so với gốc lake |
| `size_bytes` | `UInt64` | Dung lượng tệp |
| `mtime_ms` | `Int64` | Thời điểm sửa đổi tính bằng **mili-giây** từ Unix epoch |
| `total_rows` | `UInt64` | Số dòng |
| `stats_json` | `Utf8` | JSON: `{min, max, min_str, max_str}` từng cột kèm số dòng |
| `first_global_row` | `UInt64` | Dòng bắt đầu khi sắp xếp file theo đường dẫn tương đối |
| `row_groups_json` | `Utf8` | Vị trí row group Parquet, block dòng NDJSON hoặc RecordBatch Arrow IPC/Feather; `[]` với định dạng khác |

Struct tổng hợp cũng mang theo `total_files`, `total_rows`, `total_bytes`.

## Phân giải vị trí

Với entry Parquet còn khỏe, `slice_rows` và `slice_cols` tìm các row group giao với khoảng cần đọc, chỉ truyền ordinal của chúng cho Parquet reader, rồi áp dụng offset/limit cục bộ. Với entry NDJSON còn khỏe, chúng seek tới block chứa dòng yêu cầu rồi parse tiếp từ checkpoint đó. Với entry Arrow IPC/Feather còn khỏe, chúng tìm RecordBatch chứa dòng rồi gọi random-access index native của Arrow IPC trước khi đọc tiếp. Nếu file Parquet có OffsetIndex, map lưu thêm vị trí page để reader bỏ qua page trước offset. `br.lake.locate_row()` phơi ra phép phân giải global-row mà không giải mã dữ liệu. Map thiếu hoặc map stale sẽ fallback về streaming và không bao giờ được dùng cho file đã thay đổi.

---

## Lake Doctor

`doctor_lake_map(dir_path, auto_heal)` so sánh hiện trạng đĩa với danh mục:

| Trường báo cáo | Ý nghĩa |
| :--- | :--- |
| `status` | `"HEALTHY"` \| `"DRIFT_DETECTED"` \| `"HEALED"` |
| `total_files` | Số tệp thấy trên đĩa |
| `healthy_count` | Entry khớp danh mục hoàn toàn (đường dẫn + dung lượng + mtime) |
| `modified_files` | Đường dẫn đã biết nhưng size/mtime thay đổi |
| `unindexed_files` | Tệp mới chưa có trong danh mục |
| `missing_files` | Entry trong danh mục nhưng tệp không còn trên đĩa |
| `healed` | Có chạy chữa lành hay không |

**Chữa lành** dựng lại danh sách entry từ những gì còn tồn tại (bỏ `missing_files`, làm mới thống kê cho entry modified/unindexed) rồi ghi lại `.br_map.bazan`. Nếu đọc một file lỗi, thao tác dừng thay vì tạo catalog thiếu. Status chuyển thành `"HEALED"`. Không có `auto_heal=True` thì báo cáo thuần túy chẩn đoán.

```python
import basaltic_red as br

report = br.lake.doctor("data", auto_heal=False)
if report["status"] != "HEALTHY":
    report = br.lake.doctor("data", auto_heal=True)
```

Chữ ký đầy đủ: [`br.lake.*`](../reference/python-api.md#basaltic_redlake) · chi tiết bố cục nhị phân: [Đặc tả Lake Map](../reference/lake-map-spec.md).
