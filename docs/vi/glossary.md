---
title: Bảng thuật ngữ
description: Đối chiếu nhanh thuật ngữ, vị trí mã nguồn và từ vựng — không phải tài liệu diễn giải
icon: material/book-alphabet
---

# Bảng thuật ngữ (Glossary)

Chỉ dùng để tra nhanh. Muốn giải thích chi tiết, theo link sang trang thành phần tương ứng.

## Bản đồ mã nguồn

| Thuật ngữ | Vị trí | Ý nghĩa một dòng |
| :--- | :--- | :--- |
| `MatrixEngine` | `src/engine/mod.rs` | Struct trung tâm giữ các ngưỡng chất lượng |
| Binding engine | `src/pyapi/engine.rs` | `#[pymethods]` phơi ra Python |
| `default_engine()` | `src/pyapi/mod.rs` | Singleton engine cấp tiến trình (`OnceLock`) |
| `BazanError` | `src/error.rs` | Enum lỗi; ánh xạ bởi `bazan_to_pyerr()` |
| Bộ lọc tĩnh | `src/engine/filter.rs` | Đường tắt ngưỡng cố định (`filter_batch_native`) |
| Cờ bit audit | `src/filter.rs` | `ERR_INVALID_PASSENGER` / `_FARE` / `_SPEED` |
| Nhân động | `src/engine/dynamic_filter.rs` | `FilterRule::parse` + bitmask đa khối |
| Cắt lát | `src/engine/slice.rs` | `slice_rows_native`, `slice_cols_native` |
| Lọc song song | `src/engine/parallel_filter.rs` | Chạy đa tệp Rayon, `ParallelFilterSummary` |
| Cắt tỉa phân vùng | `src/engine/partition.rs` | Phân tích đường dẫn kiểu Hive & lọc tệp |
| Splitter | `src/engine/splitter.rs` | Bộ ghi chia phần `split_file_native` |
| Ingest | `src/engine/ingest.rs` | Nạp thư mục, chuẩn hóa sang Parquet |
| Bộ ghi lake | `src/engine/formats/core/parquet.rs` | Ghi lake sạch/rác, bảng gold, ZSTD |
| Tầng SQL | `src/engine/sql.rs` | Phiên DataFusion, ListingTable vs MemTable, `.br_cache` |
| `PyBatchIterator` | `src/pyapi/iterator.rs` | Nguồn batch Eager/Lazy, `to_pyarrow()` |
| Trait định dạng | `src/engine/formats/mod.rs` | `FormatHandler`, registry, bộ sniff magic-byte |
| Handler tầng 1–3 | `formats/core/`, `common/`, `plugins/adapters/` | Parquet/Feather · họ CSV & JSON · XLSX/Avro/ORC/MsgPack |
| Row chunking | `formats/plugins/base_templates/row_chunker.rs` | Template chuyển dòng → batch dùng chung |
| Lake Map | `src/engine/map.rs` | `LakeMap`, IO `.br_map.ipc`, `doctor_lake_map` |
| Ngân sách bộ nhớ | `src/engine/memory.rs` | Trần RAM, tính batch, runtime tokio/Rayon |
| CSV Guard | `src/engine/csv_guard.rs` | Bộ khử nhiễm formula injection |
| Sơ đồ ER | `src/engine/graph.rs` | Trình sinh sơ đồ ER Mermaid |
| Duyệt tệp | `src/utils.rs` | Bộ duyệt đệ quy, có sort, hiểu phân vùng |

## Từ vựng

| Thuật ngữ | Định nghĩa |
| :--- | :--- |
| RecordBatch | Chunk cột Arrow — đơn vị streaming ở mọi nơi |
| `OpenedSource` | Schema + iterator batch lazy do handler trả về |
| Clean / Trash | Dòng đạt mọi quy tắc vs vi phạm ≥1 quy tắc |
| Khối bitmask | 64 quy tắc mỗi `u64`; quy tắc *i* → bit `i % 64` của khối `i / 64` |
| `audit_error_code` | Bitmask `UInt64` các quy tắc 0–63 bị vi phạm trên dòng Trash |
| `audit_violated_rules` | `List<UInt32>` toàn bộ chỉ số vi phạm (chỉ khi >64 quy tắc) |
| `.br_map.ipc` | Danh mục Arrow IPC tại gốc lake (5 cột, xem đặc tả) |
| Trạng thái Doctor | `HEALTHY` / `DRIFT_DETECTED` / `HEALED` |
| Nhóm drift | `modified_files`, `unindexed_files`, `missing_files` |
| Partition filter | Bộ chọn cây phân vùng kiểu Hive, vd `year=2026/month=08` |
| Dict tổng kết | `{total_files, pruned_dirs, total_rows, clean_rows, trash_rows}` |
| Đích native | Extension DataFusion đọc trực tiếp (bật pushdown) |
| MemTable fallback | Định dạng non-native nạp vào RAM trước khi truy vấn |

## Biến môi trường

| Biến | Mặc định | Tác dụng |
| :--- | :--- | :--- |
| `BASALTIC_RED_MAX_RAM_GB` | `2` | Tổng ngân sách RAM cho streaming |
| `BR_INGEST_NORMALIZE` | tắt | `1`/`true` chuẩn hóa định dạng dòng khi ingest |
| `BASALTIC_RED_AUTO_NORMALIZE` | tắt | `1` bật cache chuyển mã phía SQL |
| `BASALTIC_RED_CACHE_DIR` | `<dir>/.br_cache` | Đổi chỗ cache chuyển mã SQL |
