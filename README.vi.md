# Basaltic-Red: Core SIMD Matrix Engine cho BigData Lakehouse

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https.mit-license.org)
[![Rust](https://img.shields.io/badge/Rust-1.96+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-59.1.0-red.svg)](https://crates.io/crates/arrow)

Ngôn ngữ: [English](README.md) | [Tiếng Việt](README.vi.md)

---

## Tổng Quan Dự Án

**Basaltic-Red** là thư viện Python Native Extension tốc độ cao được viết bằng **Rust (PyO3)** và **Apache Arrow**. Thư viện chuyên dùng để lọc, phân rã và quản trị dữ liệu lớn Parquet với tốc độ **`500+ MB/s`** và khống chế lượng **RAM tối đa `< 2.0 GB`**, ngay cả khi xử lý các tập dữ liệu dung lượng Terabyte.

### Các Tính Năng Cốt Lõi
- **Core SIMD Bitmask Engine**: Phân rã dữ liệu Parquet thô thành **Ma trận Sạch (Clean Matrix)** và **Ma trận Rác (Trash Matrix)** ở tốc độ native CPU.
- **Gắn Nhãn Mã Lỗi Audit (Audit Error Bitmask)**: Gán mã nhị phân (`0x01: Lỗi số khách`, `0x02: Lỗi cước phí`, `0x04: Lỗi tốc độ`) vào dữ liệu rác giúp kiểm toán 100% nguyên nhân rác.
- **DuckDB 1.4.5 Preview Zero-Copy**: Cho phép DuckDB chạy SQL preview mẫu dữ liệu trong **`< 10ms`** không tốn bộ nhớ copy.
- **Tự Động Sinh Từ Điển Dữ Liệu Markdown**: Đọc trực tiếp Schema tệp/thư mục Parquet thực tế và xuất file Markdown dạng Bảng Tiếng Anh gọn gàng.

---

## Hướng Dẫn Cài Đặt

### Cài Đặt Môi Trường

```bash
# Clone repository
git clone https://github.com/vandungdev/basaltic-red.git
cd basaltic-red

# Khởi tạo môi trường ảo Python và build thư viện Rust
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
maturin develop --release
```

---

## Cách Sử Dụng Cơ Bản

### 1. Lọc Dữ Liệu Data Lake Parquet
```python
import basaltic_red as br

# Khởi tạo Engine với các quy tắc nghiệp vụ
engine = br.MatrixEngine(
    min_passenger=1,     # Số hành khách hợp lệ: 1 đến 9
    max_passenger=9,
    min_fare=0.01,       # Cước phí hợp lệ: >= $0.01
    max_speed_mph=100.0  # Tốc độ an toàn: <= 100 mph
)

# Lọc toàn bộ thư mục Data Lake
num_files, total_rows, clean_rows, trash_rows = engine.process_and_write_lake(
    input_dir="data",
    clean_output_dir="output/clean_lake",
    trash_output_dir="output/trash_lake",
    partition_filter=None,
    batch_size=65536
)

print(f"Đã xử lý {total_rows:,} dòng | Dòng Sạch: {clean_rows:,} | Dòng Rác: {trash_rows:,}")
```

### 2. Xuất File Bảng Từ Điển Dữ Liệu Markdown
```python
import basaltic_red as br

engine = br.MatrixEngine()

# Xuất bảng Từ điển dữ liệu dạng Markdown (Nhận file Parquet đơn lẻ hoặc thư mục data/)
engine.export_data_dictionary_md("data", "data_dictionary.md")
```

### 3. Đọc Dữ Liệu Sạch & Rác Bằng DuckDB
```python
import duckdb

con = duckdb.connect("matrix_warehouse.db")

# Truy vấn Ma trận Sạch
df_clean = con.execute("SELECT * FROM clean_matrix LIMIT 10").df()
print(df_clean)

# Truy vấn Ma trận Rác kèm mã lỗi Bitmask
df_trash = con.execute("SELECT passenger_count, fare_amount, audit_error_code FROM trash_matrix LIMIT 10").df()
print(df_trash)
```

---

## Giấy Phép (License)

Dự án được phân phối dưới giấy phép **[MIT License](LICENSE)**.
