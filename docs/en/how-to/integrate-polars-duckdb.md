---
title: Integrate with Polars & DuckDB
description: Arrow interchange patterns between basaltic-red, Polars, and DuckDB
icon: material/connection
---

# Integrate with Polars & DuckDB

File readers decode input into Arrow batches. At the Arrow/Python boundary, compatible buffers may be shared through Arrow interchange; this does not make file reads, slicing, or SQL execution zero-copy.

## Integration Pattern with Polars

```python
import polars as pl
import basaltic_red as br

# Map-assisted row-range read
arrow_table = br.read.slice_rows("data/yellow_tripdata_2025-01.parquet", offset=0, limit=1000)
df = pl.from_arrow(arrow_table)
```

## Integration Pattern with DuckDB

```python
import duckdb
import basaltic_red as br

# DataFusion SQL stream collected as a PyArrow Table
stream = br.sql.execute_sql_stream("SELECT * FROM 'data/yellow_tripdata_2025-01.parquet'")
duck_df = duckdb.from_arrow(stream.to_pyarrow()).df()
```
