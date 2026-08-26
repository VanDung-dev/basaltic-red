# Basaltic-Red

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/Rust-1.80+-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/)
[![Arrow](https://img.shields.io/badge/Arrow--rs-58.4.0-red.svg)](https://crates.io/crates/arrow)
[![DataFusion](https://img.shields.io/badge/DataFusion-54.1.0-purple.svg)](https://crates.io/crates/datafusion)

> **Engine Data Lake zero-copy và kiểm tra chất lượng dữ liệu viết bằng Rust + Apache Arrow (Python qua PyO3)**

`basaltic-red` cung cấp lõi Rust để làm việc với data lake dạng file: catalog ánh xạ bộ nhớ (`.br_map.ipc`), cắt lát dòng/cột không cần đọc toàn bộ file, lọc chất lượng song song với mã audit theo dòng, và SQL qua DataFusion trên Arrow batch. Lớp Python chỉ là binding PyO3 mỏng; kết quả trả về `pyarrow.Table` / `RecordBatch` để dùng tiếp với Polars, DuckDB, pandas...

Bộ dữ liệu demo trong [`demo.ipynb`](demo.ipynb): **NYC TLC Yellow Taxi 2009–2025, 204 file Parquet, 29.66 GB, 1,826,960,642 dòng × 20 cột** (đếm bằng `pq.read_metadata` trong notebook).

---

## Kết quả đo trong demo

Số liệu dưới đây lấy từ một lần chạy `demo.ipynb` trên máy phổ thông (Apple Silicon, macOS). Tùy phần cứng, cache hệ thống và cách lưu file mà kết quả khác nhau — chỉ mang tính tham khảo.

| Kịch bản | Phạm vi | Quan sát trong demo |
| :--- | :--- | :--- |
| **Kiểm tra catalog (cold vs warm)** | 204 file | Quét cold + tạo map ~18.07 s → đọc warm qua `memmap2` ~0.5 ms (trung bình 5 lần). Nhanh hơn do warm tránh quét thư mục. |
| **Quét khối lượng (chỉ metadata)** | 1,826,960,642 dòng (36.5B ô) | Chỉ đọc metadata (số dòng + dung lượng) xong trong dưới 1 s; notebook ghi ~0.7 s. |
| **Lọc chất lượng toàn lake** | 1,826,960,642 dòng, 5 rule | ~21 s end-to-end qua `filter_files_parallel` (đọc song song + lọc Rayon). Kết quả: 1,780,228,507 clean / 46,732,135 trash với 5 rule demo. |
| **SQL aggregation một file** | 4,305,006 dòng (một batch tháng) | `GROUP BY` qua `execute_sql_stream` ~0.1 s; bàn giao zero-copy sang DuckDB/Polars qua `to_pyarrow()`. |

> Lọc dùng vòng lặp Rust trên Arrow array được LLVM tự vector hóa; chữ "SIMD" trong tài liệu cũ nghĩa là auto-vectorized, không phải intrinsic viết tay. Mã audit là bitmask `u64` theo dòng (bit *i* = rule *i* vi phạm, chia chunk khi >64 rule).

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

# 1. Chẩn đoán / tạo catalog. Lần đầu tạo .br_map.ipc; lần sau đọc qua mmap.
report = br.lake.doctor("data", auto_heal=True)
print(report["status"], report["total_files"])  # HEALTHY / HEALED / DRIFT_DETECTED

# 2. Cắt lát dòng không đọc toàn bộ file
table = br.read.slice_rows("data/yellow_tripdata_2025-12.parquet", offset=0, limit=100)
df = pl.from_arrow(table)

# 3. Lọc theo rule động. Trả về (clean, trash); trash có cột audit_error_code.
summary = br.filter.filter_files_parallel("data/yellow_tripdata_*.parquet", rules=[
    "passenger_count >= 1",
    "trip_distance > 0.0",
    "fare_amount > 0.0",
    "total_amount > 0.0",
])
print(summary)

# 4. SQL qua DataFusion rồi đưa sang DuckDB/Polars
stream = br.sql.execute_sql_stream(
    "SELECT passenger_count, AVG(fare_amount) FROM 'data/yellow_tripdata_2025-12.parquet' GROUP BY passenger_count"
)
duck_rel = duckdb.from_arrow(stream.to_pyarrow())
print(duck_rel.df())
```

Luồng đầy đủ xem `demo.ipynb` mục 0–6: tải dữ liệu → doctor → schema → preview → filter → SQL → mô phỏng drift của lake map → kiểm tra cuối.

---

## Python API

| Nhóm | Thao tác | Lệnh |
| :--- | :--- | :--- |
| `lake` | Chẩn đoán / tự phục hồi | `br.lake.doctor("data", auto_heal=True)` |
| `lake` | Tạo catalog | `br.lake.create_map("data")` |
| `read` | Cắt dòng | `br.read.slice_rows("file.parquet", offset=0, limit=100)` |
| `read` | Chiếu cột | `br.read.slice_cols("file.parquet", columns=["fare_amount", "trip_distance"])` |
| `filter` | Lọc trong RAM (một file) | `clean, trash = br.filter.filter_matrix("file.parquet", rules=[...])` |
| `filter` | Lọc song song (nhiều file) | `br.filter.filter_files_parallel("data/*.parquet", rules=[...])` |
| `sql` | DataFusion stream | `br.sql.execute_sql_stream("SELECT * FROM 'file.parquet'")` |
| `sql` | DataFusion execute | `br.sql.execute_sql("SELECT ...")` |
| `formats` | Định dạng phân cách tùy chỉnh | `br.formats.register_delimited(ext="dat", delimiter="|", has_header=True)` |

Chi tiết: `docs/vi/reference/python-api.md`, cú pháp rule: `docs/vi/reference/rule-syntax.md`.

---

## Giấy phép

Dự án được phân phối dưới giấy phép **[MIT License](LICENSE)**.
