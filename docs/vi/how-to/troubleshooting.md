---
title: Khắc phục sự cố & Tối ưu
description: Chẩn đoán các lỗi thường gặp và khai thác tối đa thông lượng hồ dữ liệu
icon: material/wrench
---

# Khắc phục sự cố & Tối ưu

## Các lỗi thường gặp & Cách xử lý

### 1. Trạng thái Lake báo `DRIFT_DETECTED`
- **Nguyên nhân**: Tệp được thêm/sửa/xóa mà chưa cập nhật danh mục.
- **Xử lý**: Chạy `br.lake.doctor("data", auto_heal=True)`, các khóa `modified_files` / `unindexed_files` / `missing_files` trong báo cáo chỉ đích danh thứ gì đã lệch.

### 2. `ValueError: Unsupported file format '.xyz'`
- **Nguyên nhân**: Extension chưa đăng ký và sniff magic-byte thất bại.
- **Cách 1**: Kiểm tra `br.formats.list_formats()` để xem extension được hỗ trợ (`csv, tsv, psv, txt, json, jsonl, ndjson, parquet, pq, feather, arrow, ipc, avro, xlsx, orc, msgpack`).
- **Cách 2**: Đăng ký handler tùy chỉnh: `br.formats.register_delimited(ext="xyz", delimiter="|")`.

### 3. `IOError: .parquet file is empty`
- **Nguyên nhân**: `preview_sample` / bộ lọc mở phải tệp có batch đầu tiên bằng 0 dòng.
- **Xử lý**: Xóa hoặc tạo lại tệp rỗng; chạy `br.lake.doctor` để tìm nó.

### 4. Quy tắc dường như bị bỏ qua (mọi dòng đều pass)
- **Nguyên nhân**: Quy tắc trỏ tới cột không tồn tại, kiểu dữ liệu không hỗ trợ (ngày tháng, decimal…), hoặc giá trị không parse được theo kiểu cột, các quy tắc này bị bỏ qua im lặng theo [Cú pháp quy tắc](../reference/rule-syntax.md#ngu-nghia-anh-gia).
- **Xử lý**: Kiểm tra tên cột qua `br.read.slice_rows(path, 0, 1).schema`.

### 5. SQL lỗi với tệp JSON
- **Nguyên nhân**: DataFusion kỳ vọng object phân tách bằng xuống dòng; mảng `[...]` tầng ngoài sẽ đi đường fallback MemTable.
- **Xử lý**: Ưu tiên NDJSON cho workload SQL lớn.

### 6. Cảnh báo CSV formula injection
- **Nguyên nhân**: Ô chuỗi bắt đầu bằng `=`, `+`, `@`, hoặc `-` không phải số.
- **Hành vi**: CSV Guard khử nhiễm tự động bằng cách thêm tiền tố `'` khi ghi CSV; giá trị âm dạng số như `-5.0` đi qua nguyên vẹn (xem `src/engine/csv_guard.rs`).

## Mẹo tối ưu

- **Cắt tỉa phân vùng**: giữ bố cục kiểu Hive (`year=.../month=.../`) và truyền `partition_filter=` để bỏ cả thư mục trước cả khi đọc IO.
- **Trần RAM**: nâng/hạ ngân sách bằng `BASALTIC_RED_MAX_RAM_GB` (mặc định 2).
- **Độ rộng song song**: giới hạn worker bằng `num_threads=` trên `filter_files_parallel` để chừa lõi cho việc khác.
- **Ưu tiên Parquet**: pushdown ListingTable thuần túy (predicate + projection) chỉ áp dụng cho đích Parquet/phân cách/JSON/IPC trong SQL.
