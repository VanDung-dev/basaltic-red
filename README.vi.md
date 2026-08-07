# Basaltic-Red

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https.mit-license.org)
[![Rust](https://img.shields.io/badge/Rust-1.96+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-58.3.0-red.svg)](https://crates.io/crates/arrow)
[![DataFusion](https://img.shields.io/badge/DataFusion-54.1.0-purple.svg)](https://crates.io/crates/datafusion)

---

## Hiệu Năng Engine & Giới Hạn Bộ Nhớ RAM

**Basaltic-Red** được tối ưu để xử lý Big Data doanh nghiệp với tốc độ `500+ MB/s` cùng hạn mức bộ nhớ RAM được cân bằng hợp lý:
- **Hạn Mức Mặc Định (Bounded RAM)**: `< 2048 MB` (2 GB) RAM - Tối ưu cho xử lý stream SIMD zero-copy tốc độ cao.

---

## Cài Đặt & Biên Dịch Trực Tiếp

```bash
# Clone repository
git clone https://github.com/vandungdev/basaltic-red.git
cd basaltic-red

# Khởi tạo môi trường ảo Python và build thư viện Rust
uv sync --extra dev --extra interop
uv run maturin develop --release
```

---

## Tổng Quan Python SDK

**Basaltic-Red** là Python SDK (**`import basaltic_red`**) chạy trên Engine Rust tốc độ cao. API được tổ chức thành các submodule theo nhóm để mọi lệnh đều nằm dưới `basaltic_red.<nhóm>.<lệnh>`:

| Nhóm | Thao Tác / Tính Năng | Cú Pháp Python (`import basaltic_red as br`) |
| :--- | :--- | :--- |
| `read` | **Cắt Khoảng Dòng Zero-Copy** | `br.read.slice_rows("data.parquet", offset=100, limit=50)` |
| `read` | **Lọc Chọn Cột (Projection)** | `br.read.slice_cols("data.csv", selected_cols=["id", "email"], offset=0, limit=50)` |
| `read` | **Xem Trước Mẫu Dữ Liệu** | `br.read.preview_sample("data.parquet", limit_rows=100)` |
| `filter` | **Lọc Quy Tắc Cột Động** | `clean_b, trash_b = br.filter.filter_matrix("data.csv", rules=["price >= 50.0"])` |
| `filter` | **Lọc Đa Luồng Song Song (Rayon)** | `summary = br.filter.filter_files_parallel("data/", rules=["age >= 18"])` |
| `filter` | **Cắt Tỉa Phân Vùng Stream (Hive)** | `summary = br.filter.filter_files_parallel("test_lakehouse", partition_filter="year=2026/month=08")` |
| `filter` | **Lọc Bitmask Hàng Loạt** | `clean_b, trash_b = br.filter.process_batch(record_batch)` |
| `sql` | **Truy Vấn SQL (DataFusion)** | `table = br.sql.execute_sql("SELECT id, salary FROM 'data/analytics'")` |
| `sql` | **Stream SQL** | `stream = br.sql.execute_sql_stream("SELECT * FROM 'data/analytics'")` |
| `lake` | **Chia Tách File Ma Trận** | `br.lake.split_file("data.csv", max_rows_per_file=100000, output_dir="./parts", format="parquet")` |
| `lake` | **Xử Lý & Ghi Lakehouse** | `br.lake.process_and_write_lake("in/", "clean/", "trash/", partition_filter=None, batch_size=65536)` |
| `lake` | **Tạo Bảng Vàng (Gold Table)** | `br.lake.generate_gold_table("clean/", "gold/", table_version="v1", partition_filter=None, batch_size=65536)` |
| `dictionary` | **Xuất Từ Điển Dữ Liệu** | `br.dictionary.export_data_dictionary_md("data.parquet", "schema.md")` |
| `graph` | **Tạo Sơ Đồ Mermaid ER Graph** | `br.graph.generate_er_graph("data/relational", output_path="er.md")` |

> `MatrixEngine` vẫn dùng được qua `br.MatrixEngine()` cho các trường hợp nâng cao (ngưỡng lọc tùy chỉnh).

### Stream SQL Interop (Phía Người Dùng)

`execute_sql_stream` trả về `PyBatchIterator` với cầu nối Arrow qua `to_pyarrow()`. Hệ sinh thái tiêu thụ trực tiếp — SDK không bọc wrapper:

```python
import polars as pl
import duckdb

stream = br.sql.execute_sql_stream("SELECT * FROM 'data/analytics'")
df = pl.from_arrow(stream.to_pyarrow())        # Polars DataFrame
rel = duckdb.from_arrow(stream.to_pyarrow())   # DuckDB relation
```

---

## Ví Dụ Minh Họa Trong Python

```python
import basaltic_red as br

# 1. Cắt dòng zero-copy (Trả về PyArrow Table)
table = br.read.slice_rows("data/sample.parquet", offset=100, limit=50)

# 2. Lọc chọn cột và khoảng dòng
cols_table = br.read.slice_cols("data/sample.csv", selected_cols=["id", "email"], offset=0, limit=50)

# 3. Thực thi câu lệnh SQL ANSI với Động cơ Apache DataFusion Pushdown trên cây thư mục
sql_result = br.sql.execute_sql("SELECT id, age, salary FROM 'data/analytics' WHERE age >= 18 ORDER BY salary DESC")

# 4. Tách file dữ liệu khổng lồ thành các file phần
parts_count = br.lake.split_file("data/sample.csv", max_rows_per_file=100000, output_dir="./parts", format="parquet")

# 5. Xuất sơ đồ Mermaid ER Diagram
mermaid_code = br.graph.generate_er_graph("data/relational", output_path="er_graph.md")
```

---

## Giấy Phép (License)

Dự án được phân phối dưới giấy phép **[MIT License](LICENSE)**.
