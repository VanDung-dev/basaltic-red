---
title: Nhật ký thay đổi
description: Lịch sử các phiên bản và ghi chú phát hành của basaltic-red
icon: material/history
---

# Nhật ký thay đổi

## Chưa phát hành (Unreleased)

*Chỉ bao gồm những thay đổi phản chiếu trong cây `src/` hiện tại.*

??? note "Improvements (30)"

    * 2026-08-21

        * **Lake Map**: Đánh chỉ mục hồ dữ liệu với kiểm tra nhất quán, hàm Python `create_map` / `doctor`, và tải danh mục tối ưu bằng memory mapping zero-copy.

    * 2026-08-18

        * **PyAPI**: Ra mắt `PyBatchIterator` kèm tích hợp SQL engine nâng cao.

    * 2026-08-17

        * **Formats**: Khả năng sniff định dạng tệp qua magic-byte.

    * 2026-08-16

        * **Formats**: Base template cho đọc phân cách và row chunking; module plugin tổ chức adapter và template thành các tầng.

    * 2026-08-15

        * **Engine**: Bitmask SIMD đa khối trong `filter_batch_dynamic`; bổ sung hint phát hiện dữ liệu non-Parquet.
        * **Formats & PyAPI**: Đăng ký handler định dạng tùy chỉnh động phơi ra qua các hàm đăng ký Python.

    * 2026-08-12

        * **Engine**: Ước lượng byte mỗi dòng theo schema và tính batch từ schema tệp đầu tiên.
        * **SQL & PyAPI**: Cache auto-normalize cho tệp SQL không native; thực thi streaming lazy cho `PyBatchIterator`.

    * 2026-08-11

        * **Engine**: Nâng cấp ngân sách bộ nhớ với pool Rayon toàn cục; DataFusion native listing table hỗ trợ truy vấn tệp; hint một lần khi đọc tệp non-Parquet.
        * **Ingest**: Song song hóa ingest tệp kèm xử lý xung đột đích.

    * 2026-08-10

        * **JSON**: Mảng JSON tầng ngoài được stream như luồng object thuần.
        * **Ingest**: Ingest thư mục native với chuẩn hóa sang Parquet tùy chọn, phơi ra qua Python API kèm module gợi ý kích thước batch.
        * **Runtime**: Runtime tokio toàn cục và ngân sách dòng batch thống nhất.

    * 2026-08-08

        * **Readers**: Chiếu cột đẩy thẳng vào reader CSV và Parquet.
        * **PyAPI**: Module pyapi mới với các module hàm được chuyển sang.
        * **Engine**: Tối ưu hiệu năng xử lý ma trận.

    * 2026-08-06

        * **SQL**: Thực thi streaming với iterator.
        * **PyAPI**: Nguồn `PyBatchIterator` có mutex guard kèm cầu nối chuyển đổi PyArrow.
        * **Utils**: Danh sách tệp có sắp xếp thứ tự trong tiện ích duyệt tệp.

    * 2026-08-05

        * **Security**: CSV injection guard khử nhiễm ô nguy hiểm.
        * **Formats**: Xử lý thống nhất phía sau `FormatHandler::open`; reader ORC native `orc-rust` thay thế đường đi dựa trên Parquet (phụ thuộc `orc-rust`, `rust_xlsxwriter`).
        * **Engine**: `clamp_batch_size` tích hợp vào mọi handler; cải thiện xác thực đầu vào cho đường dẫn tệp.
        * **Build**: Section `[lib]` bật build Python extension và Rust library.

    * 2026-08-04

        * **Filtering**: Lọc song song đa luồng `filter_files_parallel` với cắt tỉa phân vùng kiểu Hive.

    * 2026-08-03

        * **Engine**: Cắt lát, lọc, chia tệp và sinh sơ đồ ER điều khiển bởi quy tắc cột động.

    * 2026-08-02

        * **Formats**: Hỗ trợ xử lý XLSX, Avro, Feather, ORC và MsgPack cùng phụ thuộc cho các định dạng này.

    * 2026-08-01

        * **Formats**: Hỗ trợ xử lý tệp TXT và PSV.

    * 2026-07-30

        * **Engine**: Lõi `MatrixEngine` với xử lý, lọc và stream Parquet; `filter_batch_native` lọc batch kèm audit lỗi; streaming CSV/TSV/JSON/Parquet và NDJSON/JSONL; preview mẫu và export data dictionary; hằng số bitmask mã lỗi audit.
        * **Utils**: Tiện ích duyệt tệp dữ liệu với tùy chọn lọc.
        * **PyAPI**: Python binding PyO3 cho `MatrixEngine`.

??? warning "Fix (8)"

    * 2026-08-20

        * **Engine**: Siết chặt xử lý tệp và khởi tạo regex.
        * **Engine (IO)**: Tái sử dụng handle tệp, reset trạng thái reader sau suy diễn schema thay vì mở lại tệp.
        * **SQL Cache**: Vô hiệu cache Parquet khi mtime nguồn mới hơn bản cache.
        * **PyAPI**: Nhả GIL quanh lệnh block async (`py.detach`) trong nguồn stream lazy.

    * 2026-08-18

        * **Build**: Sửa vị trí export module `PyBatchIterator`.

    * 2026-08-17

        * **Engine**: Cải thiện xử lý lỗi và phân giải đường dẫn tệp.

    * 2026-08-12

        * **Ingest**: Hạn chế phạm vi `ingest_normalize` trong crate (`pub(crate)`).

    * 2026-08-11

        * **Engine**: Hint non-Parquet nhất quán qua đường slice và phân giải extension với thông báo lỗi rõ hơn.
