---
title: Data Quality Filtering
description: High-throughput dynamic rule filtering across massive Parquet lakes
icon: material/filter-variant
---

# Data Quality Filtering Workflow

## Multi-Threaded Parallel Execution with Rayon

Filter dozens of Parquet files simultaneously across all CPU cores:

```python
import basaltic_red as br

rules = [
    "passenger_count > 0",
    "passenger_count <= 6",
    "trip_distance > 0.0",
    "fare_amount >= 2.5",
    "total_amount > 0.0",
]

# Parallel filter on all matching files
summary = br.filter.filter_files_parallel("data/yellow_tripdata_2025-*.parquet", rules=rules)
print(f"Files Evaluated : {summary['total_files']:,}")
print(f"Dirs Pruned     : {summary['pruned_dirs']:,}")
print(f"Total Evaluated : {summary['total_rows']:,}")
print(f"Clean Records   : {summary['clean_rows']:,}")
print(f"Trash Records   : {summary['trash_rows']:,}")
```

## Optional Parameters

```python
# Restrict to a Hive-style partition subtree and cap worker threads
summary = br.filter.filter_files_parallel(
    "data/lakehouse",
    rules=rules,
    partition_filter="year=2026/month=08",
    num_threads=8,
)
```

`path_pattern` accepts a single file, a directory (walked recursively), or a glob. See [Filtering Pipeline](../architecture/filtering-pipeline.md) for internals. Note this command *counts* clean/trash rows; use [`br.lake.process_and_write_lake`](partitioning.md) to materialize outputs.
