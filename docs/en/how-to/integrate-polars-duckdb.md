---
title: Integrate with Polars & DuckDB
description: Zero-copy integration patterns between basaltic-red, Polars, and DuckDB
icon: material/connection
---

# Integrate with Polars & DuckDB

## Integration Pattern with Polars

```python
import polars as pl
import basaltic_red as br

# Zero-copy slicing
arrow_table = br.read.slice_rows("data/yellow_tripdata_2025-01.parquet", offset=0, limit=1000)
df = pl.from_arrow(arrow_table)
```

## Integration Pattern with DuckDB

```python
import duckdb
import basaltic_red as br

# DataFusion SQL execution to PyArrow Table
stream = br.sql.execute_sql_stream("SELECT * FROM 'data/yellow_tripdata_2025-01.parquet'")
duck_df = duckdb.from_arrow(stream.to_pyarrow()).df()
```
