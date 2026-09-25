---
title: Tích hợp Polars & DuckDB
description: Mô hình trao đổi Arrow giữa basaltic-red, Polars và DuckDB
icon: material/connection
---

# Tích hợp Polars & DuckDB

Reader giải mã đầu vào thành Arrow batch. Ở ranh giới Arrow/Python, buffer tương thích có thể được dùng chung qua Arrow interop; điều này không biến việc đọc file, slicing hay thực thi SQL thành zero-copy.

## Tích hợp với Polars

```python
import polars as pl
import basaltic_red as br

# Đọc khoảng dòng có hỗ trợ map
arrow_table = br.read.slice_rows("data/yellow_tripdata_2025-01.parquet", offset=0, limit=1000)
df = pl.from_arrow(arrow_table)
```

## Tích hợp với DuckDB

```python
import duckdb
import basaltic_red as br

# Thu thập luồng SQL DataFusion thành PyArrow Table
stream = br.sql.execute_sql_stream("SELECT * FROM 'data/yellow_tripdata_2025-01.parquet'")
duck_df = duckdb.from_arrow(stream.to_pyarrow()).df()
```
