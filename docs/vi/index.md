---
title: Trang chủ - Basaltic-Red
description: Bộ công cụ gia tốc Data Lake & kiểm soát chất lượng dữ liệu bằng Rust + Apache Arrow
icon: material/home
---

# Basaltic-Red

<p align="center">
  <strong>Bộ công cụ gia tốc Data Lake & kiểm soát chất lượng dữ liệu bằng Rust + Apache Arrow (Python qua PyO3)</strong>
</p>

---

## Tổng quan

`basaltic-red` là lõi tính toán Rust cho data lake dạng file kèm liên kết Python. Chức năng chính bao gồm: catalog ánh xạ bộ nhớ (`.br_map.ipc`), cắt lát dòng và cột không cần đọc toàn bộ file, lọc chất lượng dữ liệu song song với mã audit theo dòng, và thực thi SQL qua DataFusion trên Arrow batch. Kết quả trả về `pyarrow.Table` hoặc `RecordBatch` để sử dụng trực tiếp với Polars, DuckDB, hoặc pandas.

Demo trong [`demo.ipynb`](https://github.com/VanDung-dev/basaltic-red/blob/master/demo.ipynb): NYC TLC Yellow Taxi từ năm 2009 đến 2025, gồm 204 file Parquet, dung lượng 29.66 GB, 1,826,960,642 dòng nhân 20 cột.

```mermaid
graph LR
    A["File thô (204 Parquet / 29.66 GB)"] --> B["Lõi Rust"]
    B --> C["Catalog (.br_map.ipc / memmap2)"]
    B --> D["Lọc song song (Rayon, 1.82B dòng)"]
    B --> E["SQL stream (DataFusion)"]
    D --> F["Clean (1.78B)"]
    D --> G["Trash (46.7M + mã audit)"]
    E --> H["DuckDB / Polars"]
```

## Chức năng chính

- Catalog (`.br_map.ipc`): một file Arrow IPC ở gốc lake; đọc warm qua `memmap2` tránh duyệt cây thư mục. Doctor trả về `HEALTHY`, `DRIFT_DETECTED`, hoặc `HEALED`.
- Cắt lát: `slice_rows` và `slice_cols` chỉ đọc phần dữ liệu được yêu cầu.
- Lọc: quy tắc động theo định dạng `cột toán_tử giá_trị`; dòng lỗi mang `audit_error_code` (bitmask `u64`, chia chunk khi số quy tắc vượt quá 64).
- SQL: phiên DataFusion; `execute_sql_stream` trả về `PyBatchIterator`, `to_pyarrow()` bàn giao batch cho Polars hoặc DuckDB không qua sao chép.
- Định dạng: đăng ký định dạng phân cách tùy chỉnh qua `br.formats.register_delimited`; file không có đuôi được nhận diện bằng magic byte.

---

## Kết quả đo trong demo

Một lần chạy `demo.ipynb` trên Apple Silicon Mac. Tùy thuộc phần cứng, cung cấp để tham khảo.

| Kịch bản | Phạm vi | Quan sát |
| :--- | :--- | :--- |
| Catalog cold vs warm | 204 file | Tạo cold mất ~18.07 s; đọc warm mất ~0.5 ms (trung bình 5 lần) |
| Quét khối lượng (chỉ metadata) | 1,826,960,642 dòng | < 1 s (~0.7 s) |
| Lọc toàn lake (5 rule) | 1,826,960,642 dòng | ~21 s (1,780,228,507 dòng sạch / 46,732,135 dòng rác) |
| SQL `GROUP BY` một file | 4,305,006 dòng | ~0.1 s |

Vòng lặp lọc là Rust thuần trên Arrow array, LLVM tự động vector hóa. Không có intrinsic SIMD viết tay.

---

## Cài đặt nhanh

```bash
uv add "git+https://github.com/VanDung-dev/basaltic-red.git"
```

```python
import basaltic_red as br

report = br.lake.doctor("data/", auto_heal=True)
print(report["status"], report["total_files"])

clean, trash = br.filter.filter_matrix("data/sample.parquet", rules=[
    "passenger_count >= 1",
    "trip_distance > 0.0",
    "fare_amount > 0.0",
])
```

Xem thêm: [Quickstart](getting-started/quickstart.md) · [Lake Doctor](workflows/lake-doctor.md) · [Benchmarks](other/benchmarks.md)
