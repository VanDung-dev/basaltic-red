# Basaltic-Red

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https.mit-license.org)
[![Rust](https://img.shields.io/badge/Rust-1.96+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-59.1.0-red.svg)](https://crates.io/crates/arrow)

---

## Hiệu Năng Engine & Giới Hạn Bộ Nhớ RAM

**Basaltic-Red** & **`bazan` CLI** được tối ưu để xử lý Big Data doanh nghiệp với tốc độ `500+ MB/s` cùng hạn mức bộ nhớ RAM được cân bằng hợp lý:
- **Hạn Mức Mặc Định (Bounded RAM)**: `< 2048 MB` (2 GB) RAM - Tối ưu cho xử lý stream SIMD zero-copy tốc độ cao.

---

## Cài Đặt & Biên Dịch Trực Tiếp


```bash
# Clone repository
git clone https://github.com/vandungdev/basaltic-red.git
cd basaltic-red

# Khởi tạo môi trường ảo Python và build thư viện Rust
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
maturin develop --release

# Biên dịch công cụ dòng lệnh bazan CLI
cargo build --release --bin bazan
```

---

## Bảng Đối Chiếu Lệnh Terminal CLI `bazan` & Python SDK

**Basaltic-Red** hỗ trợ giao diện kép: Thao tác cực nhanh qua dòng lệnh Terminal (**`bazan`**) hoặc gọi trực tiếp trong code Python (**`import basaltic_red`**).

| Thao Tác / Tính Năng | Lệnh Terminal CLI (`bazan`) | Cú Pháp Python Tương Ứng (`import basaltic_red`) |
| :--- | :--- | :--- |
| **Cắt Khoảng Dòng Zero-Copy** | `bazan slice-rows data.parquet --offset 100 --limit 50` | `engine.slice_rows("data.parquet", offset=100, limit=50)` |
| **Lọc Chọn Cột (Projection)** | `bazan slice-cols data.csv --cols id,email --limit 50` | `engine.slice_cols("data.csv", selected_cols=["id", "email"], offset=0, limit=50)` |
| **Lọc Quy Tắc Cột Động** | `bazan filter data.csv --rule "price >= 50.0"` | `clean_b, trash_b = engine.filter_matrix("data.csv", rules=["price >= 50.0"])` |
| **Chia Tách File Ma Trận** | `bazan split data.csv --max-rows 100000 --output-dir ./parts` | `engine.split_file("data.csv", max_rows_per_file=100000, output_dir="./parts", format="parquet")` |
| **Xem Nhanh N Dòng Bảng** | `bazan preview data.parquet --limit 20` | `engine.slice_rows("data.parquet", offset=0, limit=20)` |
| **Xuất Từ Điển Dữ Liệu** | `bazan dict data.parquet --output schema.md` | `engine.export_data_dictionary_md("data.parquet", "schema.md")` |
| **Tạo Sơ Đồ Mermaid ER Graph** | `bazan graph data/relational --output er.md` | `engine.generate_er_graph_py("data/relational", output_path="er.md")` |

---

## Ví Dụ Minh Họa Trong Python

```python
import basaltic_red as br

# Khởi tạo Engine
engine = br.MatrixEngine()

# 1. Cắt dòng zero-copy (Trả về PyArrow Table)
table = engine.slice_rows("data/sample.parquet", offset=100, limit=50)

# 2. Lọc chọn cột và khoảng dòng
cols_table = engine.slice_cols("data/sample.csv", selected_cols=["id", "email"], offset=0, limit=50)

# 3. Tách file dữ liệu khổng lồ thành các file phần
parts_count = engine.split_file("data/sample.csv", max_rows_per_file=100000, output_dir="./parts", format="parquet")

# 4. Xuất sơ đồ Mermaid ER Diagram
mermaid_code = engine.generate_er_graph_py("data/relational", output_path="er_graph.md")
```

---

## Giấy Phép (License)

Dự án được phân phối dưới giấy phép **[MIT License](LICENSE)**.
