---
title: Quickstart Guide
description: 5-minute practical walkthrough of basaltic-red core capabilities
icon: material/lightning-bolt
---

# 5-Minute Quickstart Guide

This guide walks through a core `basaltic-red` workflow: creating or repairing a Lake Map, reading a row range, applying dynamic quality filters, and querying with SQL.

---

## Step 1: Create or Repair the Lake Map

`doctor(auto_heal=True)` creates a missing catalog or repairs detected drift for files discovered by supported extension. Drift detection compares file paths, sizes, and modification times rather than file-content hashes; auto-heal rereads new or modified files to rebuild their entries.

```python
import basaltic_red as br

# Create the catalog if missing, or reindex detected metadata drift
health = br.lake.doctor("data", auto_heal=True)
print("Lake Health Report:")
for k, v in health.items():
    print(f"  - {k:18s}: {v}")
```

---

## Step 2: Read a Row Range

With a healthy Lake Map, supported Parquet slices use row-group locations to avoid unrelated groups. The reader still decodes the requested values, and latency depends on the file layout and cache state.

```python
import polars as pl

# Read 100 rows through the map-assisted slice API
arrow_table = br.read.slice_rows("data/yellow_tripdata_2025-01.parquet", offset=0, limit=100)
df = pl.from_arrow(arrow_table)
print(df.shape)  # (100, 20)
```

---

## Step 3: High-Speed SIMD Data Quality Filtering

Define arbitrary validation rules and partition records into Clean and Trash tables:

```python
rules = [
    "passenger_count > 0",
    "trip_distance > 0.0",
    "fare_amount >= 2.5",
    "total_amount > 0.0",
]

# High-speed in-memory matrix filter
clean_batch, trash_batch = br.filter.filter_matrix(
    "data/yellow_tripdata_2025-01.parquet",
    rules=rules
)

clean_df = pl.from_arrow(clean_batch)
trash_df = pl.from_arrow(trash_batch)

print(f"Clean rows : {clean_df.height:,}")
print(f"Trash rows : {trash_df.height:,}")
```

---

## Step 4: Streaming SQL Analytics with DataFusion

Stream query execution pushdown directly into Polars or DuckDB:

```python
import duckdb

# DataFusion SQL execution stream
stream = br.sql.execute_sql_stream("SELECT passenger_count, AVG(fare_amount) AS avg_fare FROM 'data/output/clean_trips.parquet' GROUP BY passenger_count")

# Arrow handoff to DuckDB or Polars
duck_df = duckdb.from_arrow(stream.to_pyarrow()).df()
print(duck_df)
```
