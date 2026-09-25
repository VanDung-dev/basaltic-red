---
title: Đặc tả bản đồ nhị phân
description: Bố cục nhị phân và schema cột của tệp .br_map.bazan
icon: material/map
---

# Đặc tả Lake Map

Lake Map được serialize thành **payload Apache Arrow IPC** trong tệp hệ thống `.br_map.bazan` tại gốc hồ dữ liệu (`resolve_map_path()` trong `src/engine/map.rs`). `br.lake.create_map()` ghi map và khi load thì dùng memory map; thời gian load phụ thuộc dung lượng map và hệ thống lưu trữ. Tạo map và Lake Doctor discovery theo extension: gồm file có built-in hoặc dynamic handler đang đăng ký, bỏ qua file không extension dù đọc file trực tiếp có thể sniff được một số định dạng. Map mới có vị trí row group Parquet, stripe ORC, block OCF Avro, block object MsgPack và block dòng logic XLSX, checkpoint của NDJSON/JSONL và mảng JSON, checkpoint byte theo block dòng CSV/TSV/PSV/TXT và ordinal RecordBatch của Arrow IPC/Feather; map cũ 5 cột vẫn đọc được nhưng không tăng tốc slicing. `.br_map.ipc` được dành riêng làm tên sidecar cũ bị bỏ qua. `doctor_lake_map` vẫn kiểm tra metadata tệp hiện tại để phát hiện drift.

Map mới có schema metadata xác định `bazan.kind=lake_map`, `bazan.version=1`, `bazan.payload=arrow_ipc` và `bazan.map_schema=3`. Schema 3 lưu thêm `bazan.checkpoint_stride_rows`, `bazan.fingerprint` và `bazan.stats_columns`. Payload vẫn là tệp Arrow IPC thông thường; hậu tố `.bazan` dành riêng namespace tệp hệ thống mà không chiếm `.ipc` của data người dùng. Loader tiếp tục đọc map legacy 5 cột và schema 2 có 7 cột, áp dụng mặc định trước đây cho cả hai.

## Schema RecordBatch (một dòng cho mỗi tệp dữ liệu)

| Cột | Kiểu Arrow | Nullable | Mô tả |
| :--- | :--- | :--- | :--- |
| `rel_path` | `Utf8` | Không | Đường dẫn tệp tương đối so với gốc lake |
| `size_bytes` | `UInt64` | Không | Dung lượng tính bằng byte |
| `mtime_ms` | `Int64` | Không | Thời điểm sửa đổi, mili-giây từ Unix epoch |
| `total_rows` | `UInt64` | Không | Số dòng của tệp |
| `stats_json` | `Utf8` | Không | JSON: `{"total_rows": N, "columns": {"<tên>": {"min": f64, "max": f64, "min_str": str, "max_str": str}}}` |
| `first_global_row` | `UInt64` | Không | Dòng bắt đầu trong lake đã sắp xếp theo path |
| `row_groups_json` | `Utf8` | Không | Mảng JSON vị trí row group Parquet, stripe ORC, block OCF Avro, block object MsgPack, block dòng logic XLSX, checkpoint dòng hoặc mảng JSON/JSONL/NDJSON, block dòng CSV/TSV/PSV/TXT hoặc RecordBatch Arrow IPC/Feather; `[]` với định dạng khác |
| `content_hash` | `Utf8` | Có | Digest BLAKE3 khi `fingerprint="blake3"`; null với map chỉ dùng metadata |

Cấu hình lưu trong map điều khiển việc tạo chỉ mục: `checkpoint_stride_rows` phải lớn hơn 0 và áp dụng cho định dạng checkpoint theo dòng; `fingerprint` nhận `metadata` hoặc `blake3`; `stats_columns` là null để tính mọi kiểu thống kê đang hỗ trợ, danh sách rỗng để tắt, hoặc danh sách cột cần tính. Tắt thống kê không bỏ lượt đọc để đếm dòng và tạo locator. Stride không đổi row group Parquet, stripe ORC, block Avro, batch Arrow hay block logic XLSX.

Mỗi object có `ordinal`, `first_row`, `row_count`, `first_byte`, `total_byte_size`, `compressed_size` và `columns`. Object Parquet có thêm path cột, byte range nén và page location tùy chọn. Object ORC dùng `first_byte` và `total_byte_size` cho stripe native. Object Avro dùng `first_byte`, `total_byte_size` và `compressed_size` cho data block OCF native. Object MsgPack dùng `first_byte` và `total_byte_size` cho checkpoint object theo stride cấu hình. Object XLSX dùng `first_row` và `row_count` cho block dòng logic theo stride cấu hình; `first_byte`, `total_byte_size`, `compressed_size` và `columns` để rỗng vì XML worksheet thường được nén Deflate trong ZIP. Object NDJSON và JSONL dạng mỗi dòng một value dùng các trường đó cho block dòng theo stride cấu hình; object mảng JSON dùng cho block object top-level theo stride đó; checkpoint định dạng phân cách hiểu quote và bỏ qua dòng vật lý rỗng. Stride mặc định là 65.536 dòng. Với `.jsonl`, byte không phải whitespace đầu tiên chọn checkpoint mảng (`[`) hoặc checkpoint theo dòng. Object Arrow IPC/Feather dùng ordinal và khoảng dòng, còn `first_byte`, `total_byte_size`, `columns` để rỗng vì reader dùng batch index native của Arrow IPC. Đây là vị trí block/row-group/stripe/batch, không phải byte offset của từng dòng. Khi dynamic handler override extension đã đăng ký, `row_groups_json` là `[]`; map-backed slice dùng handler đã đăng ký thay cho locator built-in.

Khi slice Parquet, map chọn row group và offset dòng bên trong group đầu tiên. Nếu file nguồn có OffsetIndex, Parquet Arrow reader nạp index theo chế độ tùy chọn và có thể bỏ qua các data page không được chọn khi áp dụng offset và limit. Slice không dùng trực tiếp page location đã serialize trong map; reader tự nạp index từ file Parquet. Không có OffsetIndex thì slice vẫn đúng, nhưng có thể phải đọc thêm dữ liệu bên trong các row group được chọn. Với định dạng theo dòng, phép chiếu cột chỉ đổi các cột trả về, không bỏ qua byte field không được chọn bên trong record; một số reader còn decode toàn bộ dòng được chọn trước khi project.

Quy tắc chọn theo hình dạng nội dung cũng áp dụng cho `.json`: nếu chứa JSON object mỗi dòng, Lake Map dùng checkpoint theo dòng. `locate_row` phân giải dựa trên catalog đã lưu và không kiểm tra độ mới của nguồn; hãy chạy Lake Doctor sau khi nguồn hoặc tập file thay đổi. BLAKE3 cùng Doctor phát hiện nội dung đổi nhưng giữ nguyên dung lượng và mtime, còn slice chỉ kiểm dung lượng và thời gian sửa đổi.

## Khóa so sánh của Doctor

Một entry *khỏe mạnh* khi sự tồn tại và fingerprint được chọn khớp hệ thống tệp hiện tại. Metadata mode so sánh `size_bytes` và `mtime_ms`; mode này không phát hiện đổi nội dung cùng dung lượng nếu mtime được giữ nguyên. BLAKE3 mode còn so sánh digest nội dung và phát hiện thay đổi đó trong Lake Doctor. Mọi sai lệch xếp tệp vào `modified`; tệp có trên đĩa nhưng thiếu trong danh mục là `unindexed`; entry trong danh mục nhưng không còn tệp là `missing`. Slice vẫn kiểm nhanh size/mtime và không hash toàn file ở mỗi range; hãy chạy Doctor trước khi slice nếu cần phát hiện drift bằng BLAKE3.

## Tổng hợp

Struct `LakeMap` trong bộ nhớ còn mang các tổng số suy dẫn: `total_files`, `total_rows`, `total_bytes`. Entry được sắp xếp theo đường dẫn tương đối trước khi gán `first_global_row`, nên lake nhiều định dạng dùng thứ tự path ổn định.

Hiện `stats_json` ghi min/max của giá trị không null chỉ cho cột Arrow `Int64`, `Float64` và `Utf8`. Kiểu số dùng `min`/`max`; chuỗi UTF-8 dùng so sánh từ điển `min_str`/`max_str`. Kiểu Arrow khác và cột không có giá trị không null bị bỏ qua. Biên `Int64` được đổi sang JSON `f64`, nên giá trị ngoài miền số nguyên biểu diễn chính xác (±2^53) có thể bị làm tròn. Không dùng các thống kê này để đảm bảo predicate số nguyên chính xác.
