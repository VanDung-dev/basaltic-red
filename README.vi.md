# Basaltic-Red

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/Rust-1.80+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-58.4.0-red.svg)](https://crates.io/crates/arrow)
[![DataFusion](https://img.shields.io/badge/DataFusion-54.1.0-purple.svg)](https://crates.io/crates/datafusion)

> Bộ công cụ gia tốc Data Lake và kiểm soát chất lượng dữ liệu bằng Rust và Apache Arrow với Python binding qua PyO3.

`basaltic-red` không phải là cơ sở dữ liệu. Dự án không có tiến trình daemon chạy ngầm, không mở cổng socket mạng, và không dùng định dạng lưu trữ độc quyền. Đây là bộ công cụ bổ trợ được thiết kế để dùng chung với các công cụ phân tích hiện có như DuckDB, Polars, PyArrow, pandas và DataFusion.

Các tiện ích cho data lake dạng file:
* Catalog ánh xạ bộ nhớ (`.br_map.ipc`): nạp metadata dưới 0.5 ms qua OS `mmap`, tự động phát hiện lệch dữ liệu (`br.lake.doctor`) và hiển thị thanh tiến trình terminal.
* Cắt lát dữ liệu zero-copy (`br.read`): đọc dải dòng hoặc chiếu cột mà không cần nạp toàn bộ file vào RAM.
* Lọc chất lượng dữ liệu song song (`br.filter`): kiểm tra quy tắc động đa luồng với bitmask `u64` cho từng dòng để phân loại dòng hợp lệ và dòng lỗi.
* Thực thi SQL nhúng (`br.sql`): chạy truy vấn DataFusion SQL trên thư mục file và bàn giao RecordBatch cho DuckDB hoặc Polars không qua sao chép bộ nhớ.
* Đăng ký định dạng tùy chỉnh & sniffing (`br.formats`): tự động nhận diện kiểu tệp qua magic byte và hỗ trợ ký tự phân cách tùy biến không cần biên dịch lại.

Bộ dữ liệu demo trong [`demo.ipynb`](demo.ipynb): NYC TLC Yellow Taxi từ năm 2009 đến 2025, gồm 204 file Parquet, dung lượng 29.66 GB và 1,826,960,642 dòng nhân 20 cột.

---

## So sánh với cơ sở dữ liệu truyền thống

| Đặc tính | Basaltic-Red | Cơ sở dữ liệu (ClickHouse, PostgreSQL) |
| :--- | :--- | :--- |
| Kiến trúc | Thư viện Python in-process (Rust cdylib) | Tiến trình máy chủ (daemon) độc lập |
| Định dạng lưu trữ | Tệp tiêu chuẩn mở (Parquet, Arrow IPC, CSV, JSON, Avro, ORC) | Định dạng bảng và file WAL nội bộ |
| Mạng và cổng kết nối | In-process qua Arrow C Data Interface | Socket TCP và giao thức mạng |
| Vai trò trong hệ sinh thái | Tiền xử lý, tạo catalog, audit dữ liệu, cắt lát | Lưu trữ bền vững và phục vụ truy vấn |
| Khả năng tương tác | Bàn giao zero-copy trực tiếp sang DuckDB, Polars, PyArrow | Cần driver client và tuần tự hóa qua mạng |

---

## Kết quả đo trong demo

Số liệu dưới đây lấy từ một lần chạy `demo.ipynb` trên Apple Silicon Mac. Kết quả có thể thay đổi tùy phần cứng, cache hệ thống và cấu trúc dữ liệu.

| Kịch bản | Phạm vi | Quan sát trong demo |
| :--- | :--- | :--- |
| Kiểm tra catalog (cold vs warm) | 204 file | Quét cold và tạo map mất ~18.07 s; đọc warm qua `memmap2` mất ~0.5 ms (trung bình 5 lần). Đọc warm nhanh hơn vì không cần duyệt cây thư mục. |
| Quét khối lượng (chỉ metadata) | 1,826,960,642 dòng (36.5B ô) | Đọc metadata số dòng và kích thước file hoàn tất trong ~0.7 s. |
| Lọc chất lượng toàn lake | 1,826,960,642 dòng, 5 rule | Mất ~21 s khi dùng `filter_files_parallel` (đọc song song và lọc Rayon), cho ra 1,780,228,507 dòng sạch và 46,732,135 dòng rác. |
| SQL aggregation một file | 4,305,006 dòng (một batch tháng) | `GROUP BY` qua `execute_sql_stream` mất ~0.1 s, sau đó bàn giao zero-copy sang DuckDB hoặc Polars. |

Vòng lặp lọc dùng code Rust thuần trên Arrow array và được LLVM tự vector hóa. Mã audit là bitmask `u64` theo từng dòng (bit *i* ứng với rule *i* vi phạm, chia chunk khi số rule vượt quá 64).

---

## Cài đặt

```bash
# Từ GitHub qua uv
uv add "git+https://github.com/VanDung-dev/basaltic-red.git"

# Kèm interop (Polars, DuckDB, pandas, numpy)
uv add "basaltic-red[interop] @ git+https://github.com/VanDung-dev/basaltic-red.git"

# Kèm notebook (Jupyter, matplotlib, seaborn)
uv add "basaltic-red[interop,notebook] @ git+https://github.com/VanDung-dev/basaltic-red.git"
```

Build từ mã nguồn:

```bash
git clone https://github.com/VanDung-dev/basaltic-red.git
cd basaltic-red
uv run --no-sync maturin develop --release
```

Yêu cầu Python 3.12+.

---

## Ví dụ

```python
import basaltic_red as br
import polars as pl
import duckdb

# 1. Tạo hoặc chẩn đoán catalog
map_path = br.lake.create_map("data", show_progress=True)
report = br.lake.doctor("data", auto_heal=True)
print(report["status"], report["total_files"])

# 2. Cắt lát dòng không đọc toàn bộ file
table = br.read.slice_rows("data/yellow_tripdata_2025-12.parquet", offset=0, limit=100)
df = pl.from_arrow(table)

# 3. Lọc song song theo rule động trên nhiều tệp (trả về dict thống kê)
summary = br.filter.filter_files_parallel("data/yellow_tripdata_*.parquet", rules=[
    "passenger_count >= 1",
    "trip_distance > 0.0",
    "fare_amount > 0.0",
    "total_amount > 0.0",
])
print(summary)

# 4. Chạy SQL qua DataFusion rồi đưa sang DuckDB hoặc Polars
stream = br.sql.execute_sql_stream(
    "SELECT passenger_count, AVG(fare_amount) FROM 'data/yellow_tripdata_2025-12.parquet' GROUP BY passenger_count"
)
duck_rel = duckdb.from_arrow(stream.to_pyarrow())
print(duck_rel.df())
```

Luồng đầy đủ xem `demo.ipynb` các phần từ 0 đến 6: tải dữ liệu, doctor, schema, preview, filter, SQL, mô phỏng drift của lake map và kiểm tra cuối.

---

## Python API

| Nhóm | Thao tác | Lệnh |
| :--- | :--- | :--- |
| `lake` | Chẩn đoán / tự phục hồi | `br.lake.doctor("data", auto_heal=True)` |
| `lake` | Tạo catalog | `br.lake.create_map("data", show_progress=True)` |
| `read` | Cắt dòng | `br.read.slice_rows("file.parquet", offset=0, limit=100)` |
| `read` | Chiếu cột | `br.read.slice_cols("file.parquet", selected_cols=["fare_amount", "trip_distance"], offset=0, limit=100)` |
| `filter` | Lọc trong RAM (một file) | `clean, trash = br.filter.filter_matrix("file.parquet", rules=[...])` |
| `filter` | Lọc song song (nhiều file) | `br.filter.filter_files_parallel("data/*.parquet", rules=[...])` |
| `sql` | DataFusion stream | `br.sql.execute_sql_stream("SELECT * FROM 'file.parquet'")` |
| `sql` | DataFusion execute | `br.sql.execute_sql("SELECT ...")` |
| `formats` | Định dạng phân cách tùy chỉnh | `br.formats.register_delimited(ext="dat", delimiter="|", has_header=True)` |

Chi tiết: `docs/vi/reference/python-api.md`, cú pháp rule: `docs/vi/reference/rule-syntax.md`.

---

## Giấy phép

Dự án được phân phối dưới [MIT License](LICENSE).
