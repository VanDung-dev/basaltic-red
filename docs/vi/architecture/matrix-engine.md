---
title: Lõi MatrixEngine
description: Struct engine trung tâm, ngưỡng chất lượng, phân loại lỗi và các thao tác cắt lát
icon: material/engine
---

# Lõi MatrixEngine

`MatrixEngine` (`src/engine/mod.rs`) là struct duy nhất đứng sau mọi lệnh của SDK. Nó mang bốn ngưỡng chất lượng dữ liệu dùng cho đường tắt lọc tĩnh và `preview_sample`:

```rust
pub struct MatrixEngine {
    pub min_passenger: i64,   // mặc định 1
    pub max_passenger: i64,   // mặc định 9
    pub min_fare: f64,        // mặc định 0.01
    pub max_speed_mph: f64,   // mặc định 100.0
}
```

## Khởi tạo

```python
import basaltic_red as br

# Singleton dùng chung cho mọi lệnh br.<group>.* (ngưỡng 1, 9, 0.01, 100.0)
br.read.slice_rows(...)

# Ngưỡng tùy chỉnh cho mục đích nâng cao
engine = br.MatrixEngine(
    min_passenger=0,
    max_passenger=20,     # raises ValueError nếu min > max
    min_fare=-5.0,
    max_speed_mph=200.0,
)
```

`br.MatrixEngine(...)` nhận tham số theo vị trí hoặc từ khóa. Instance dùng chung được tạo một lần mỗi tiến trình qua `OnceLock`, nên ngưỡng không bao giờ lệch nhau giữa các lệnh con.

---

## Thao tác cắt lát (Slicing)

Cài đặt trong `src/engine/slice.rs`; phơi ra qua [`br.read.*`](../reference/python-api.md#basaltic_redread):

| Phương thức | Hành vi |
| :--- | :--- |
| `slice_rows(file_path, offset, limit)` | Đọc một khoảng dòng trả về PyArrow Table. Với Parquet dùng row-group reader; với IPC/Feather dùng memory-map qua `memmap2`. |
| `slice_cols(file_path, selected_cols, offset, limit)` | Như trên, kèm chiếu cột đẩy xuống reader (Parquet chỉ đọc đúng các column chunk cần thiết). |
| `preview_sample(file_path, limit_rows)` | Mở batch đầu tiên và chạy bộ lọc **ngưỡng tĩnh**; trả về `(clean_table, trash_table)`. |

Cả hai phương thức slice đều phân giải handler qua [registry định dạng](formats.md), nên hoạt động với mọi định dạng được hỗ trợ chứ không riêng Parquet.

---

## Phân loại lỗi

Toàn bộ lỗi engine đi qua một enum duy nhất — `BazanError` trong `src/error.rs` — được ánh xạ sang Python bởi một hàm duy nhất (`src/pyapi/mod.rs`):

| Variant Rust | Exception Python | Nguyên nhân thường gặp |
| :--- | :--- | :--- |
| `UnsupportedFormat(_)` | `ValueError` | Extension chưa đăng ký và sniff thất bại |
| `DataFusion(_)` | `RuntimeError` | Lỗi parse/thực thi SQL |
| còn lại (`Message`, IO, Arrow) | `IOError` | Thiếu tệp, tệp rỗng, dữ liệu hỏng |

Lỗi cú pháp quy tắc phát sinh trước khi thực thi được trả về dưới dạng `ValueError` ngay từ bộ phân tích quy tắc.

---

## Lọc Tĩnh vs Động

- **Đường tắt tĩnh** (`src/filter.rs` + `engine/filter.rs`): ba cờ bit cố định — khoảng passenger, mức giá tối thiểu, bất thường distance/fare — đánh giá bằng Arrow compute kernel trên cả cột (vector hóa hoàn toàn). Dùng cho `process_batch` / `process_file` / `process_and_write_lake` / `preview_sample`. Không tốn chi phí parse quy tắc; kỳ vọng các cột kiểu taxi NYC (`passenger_count`, `fare_amount`, `trip_distance`) và bỏ qua im lặng khi thiếu cột.
- **Nhân động** (`engine/dynamic_filter.rs`): quy tắc người dùng tùy ý parsed từ chuỗi trên mọi kiểu cột được hỗ trợ. Xem [Nhân SIMD Bitmask](simd-kernel.md).
