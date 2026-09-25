---
title: Tầng SQL DataFusion
description: Cách br.sql phân giải đích truy vấn, lập kế hoạch và stream batch Arrow về Python
icon: material/database-search
---

# Tầng SQL DataFusion

`src/engine/sql.rs` nhúng Apache DataFusion. Cả hai lệnh nhận một chuỗi SQL duy nhất, mệnh đề `FROM` tham chiếu tới **đường dẫn tệp hoặc thư mục** (bọc trong ngoặc đơn):

```python
import basaltic_red as br

table  = br.sql.execute_sql("SELECT COUNT(*) FROM 'data/analytics'")       # Table thu thập đủ
stream = br.sql.execute_sql_stream("SELECT * FROM 'data/analytics'")       # PyBatchIterator
```

---

## Phân giải đích

Trước khi lập kế hoạch, engine đăng ký đích `FROM` thành bảng tên `br_target`. Có hai đường:

1. **ListingTable thuần túy**, với extension DataFusion tự đọc được:

    | Extension | Định dạng DataFusion |
    | :--- | :--- |
    | `parquet`, `pq` | ParquetFormat |
    | `csv`, `tsv`, `psv` | CsvFormat (đúng ký tự phân cách) |
    | `json`, `jsonl`, `ndjson` | JsonFormat (object phân tách bởi xuống dòng) |
    | `arrow`, `ipc`, `feather` | ArrowFormat |

    Một *thư mục* đồng nhất extension đăng ký thành một ListingTable duy nhất, cho phép DataFusion áp dụng predicate và projection pruning mà reader hỗ trợ trên các file. Mức IO tiết kiệm được tùy định dạng và query plan.

2. **MemTable dự phòng**, cho định dạng không có reader DataFusion (`xlsx`, `avro`, `orc`, `msgpack`, `txt`, thư mục lẫn loại) và tệp JSON có tầng ngoài là mảng (`[...]`). Tệp được đọc qua [registry định dạng](formats.md), nạp vào MemTable rồi truy vấn trong RAM.

Reader `ListingTable` native dùng trực tiếp `FileFormat` của DataFusion, không tra registry `FormatHandler` động. Vì vậy, đăng ký handler cho extension native như `csv` không thay đổi cách `br.sql.*` đọc extension đó.

!!! note "JSON mảng ở tầng ngoài"

    `JsonFormat` của DataFusion kỳ vọng object phân tách bằng xuống dòng. Tệp bắt đầu bằng `[` được phát hiện qua byte đầu tiên khác whitespace và chuyển sang đường streaming handler.

---

## Streaming vs Thu thập

| Lệnh | Kết quả | Mức tiêu thụ RAM |
| :--- | :--- | :--- |
| `execute_sql(query)` | `pyarrow.Table` | Thu thập toàn bộ |
| `execute_sql_stream(query)` | `PyBatchIterator` | Lazy, batch sinh theo nhu cầu |

### PyBatchIterator (`src/pyapi/iterator.rs`)

Hai nguồn nội bộ phía sau một class:

- **Lazy**, một `SendableRecordBatchStream` đang sống; mỗi `next()` kéo một batch từ runtime tokio (`memory::global_runtime()`).
- **Eager**, `Vec<RecordBatch>` thu thập sẵn (dùng khi phải fallback MemTable).

API phía Python:

- duyệt: `for batch in stream:` trả về RecordBatch PyArrow
- `stream.to_pyarrow()` → PyArrow Table đầy đủ
- `repr(stream)` → `PyBatchIterator(batches=N, rows=M)` (biết ngay với nguồn eager; điền dần khi tiêu thụ với nguồn lazy)

Arrow C Data Interface có thể dùng chung buffer tương thích khi bàn giao kết quả cho Polars hoặc DuckDB. DataFusion vẫn giải mã dữ liệu đầu vào và consumer có thể materialize hoặc copy kết quả; xem [Tích hợp Polars & DuckDB](../how-to/integrate-polars-duckdb.md).

---

## Những gì được đẩy xuống

Với ListingTable Parquet, DataFusion có thể cắt tỉa row group và tránh đọc column chunk không được chọn khi query plan và metadata file cho phép. Định dạng native khác có thể dùng hỗ trợ projection hoặc filter của reader, nhưng không nhất thiết bỏ qua physical input theo cùng cách.

```python
# Chỉ đọc column chunk passenger_count + fare_amount, cắt tỉa row group theo predicate
br.sql.execute_sql_stream(
    "SELECT passenger_count, AVG(fare_amount) "
    "FROM 'data/output/clean_trips.parquet' GROUP BY passenger_count"
)
```
