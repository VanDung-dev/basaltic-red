---
title: Tích hợp Polars & DuckDB
description: Các mô hình tích hợp Zero-Copy giữa basaltic-red, Polars và DuckDB
icon: material/connection
---

# Tích hợp Polars & DuckDB

## Tích hợp với Polars

```python
import polars as pl
import basaltic_red as br

# Cắt lát dữ liệu zero-copy
arrow_table = br.read.slice_rows("data/yellow_tripdata_2025-01.parquet", offset=0, limit=1000)
df = pl.from_arrow(arrow_table)
```

## Tích hợp với DuckDB

```python
import duckdb
import basaltic_red as br

# Thực thi SQL DataFusion và chuyển tiếp sang DuckDB
stream = br.sql.execute_sql_stream("SELECT * FROM 'data/yellow_tripdata_2025-01.parquet'")
duck_df = duckdb.from_arrow(stream.to_pyarrow()).df()
```
