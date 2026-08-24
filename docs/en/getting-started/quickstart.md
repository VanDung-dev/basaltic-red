---
title: Quickstart Guide
description: 5-minute practical walkthrough of basaltic-red core capabilities
icon: material/lightning-bolt
---

# 5-Minute Quickstart Guide

This guide walks you through the core workflow of `basaltic-red`: initializing a Data Lake, slicing data zero-copy, applying dynamic SIMD quality filters, and querying with SQL.

---

## Step 1: Initialize Data Lake with Lake Doctor

Always run `br.lake.doctor` when starting to work with a data directory:

```python
import basaltic_red as br

# Diagnose lake and generate .br_map.ipc catalog automatically
health = br.lake.doctor("data", auto_heal=True)
print("Lake Health Report:")
for k, v in health.items():
    print(f"  - {k:18s}: {v}")
```

---

## Step 2: Zero-Copy Sample Slicing

Read row and column slices from large Parquet files in microseconds:

```python
import polars as pl

# Slice rows [0..100] without reading the entire dataset
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

## Step 4: Zero-Copy SQL Analytics with DataFusion

Stream query execution pushdown directly into Polars or DuckDB:

```python
import duckdb

# DataFusion SQL execution stream
stream = br.sql.execute_sql_stream("SELECT passenger_count, AVG(fare_amount) AS avg_fare FROM 'data/output/clean_trips.parquet' GROUP BY passenger_count")

# Zero-copy handoff to DuckDB or Polars
duck_df = duckdb.from_arrow(stream.to_pyarrow()).df()
print(duck_df)
```
