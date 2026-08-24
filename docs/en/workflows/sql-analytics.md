---
title: Zero-Copy SQL Analytics
description: End-to-end analytical workflow with DataFusion, Polars, and DuckDB
icon: material/database-search
---

# Zero-Copy SQL Analytics Workflow

## Stream Pushdown Integration

Execute SQL aggregations directly on clean Parquet output files and pass batches to Polars:

```python
import polars as pl
import basaltic_red as br

# DataFusion SQL execution stream
stream = br.sql.execute_sql_stream("""
    SELECT 
        passenger_count, 
        COUNT(*) AS total_trips,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM 'data/output/clean_trips.parquet'
    GROUP BY passenger_count
    ORDER BY total_trips DESC
""")

# Convert to Polars DataFrame without memory copy
df = pl.from_arrow(stream.to_pyarrow())
print(df)
```
