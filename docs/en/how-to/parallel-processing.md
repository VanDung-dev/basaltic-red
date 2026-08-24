---
title: Multi-File Parallel Processing
description: Scaling Rayon multi-threaded filter across directory partitions
icon: material/layers
---

# Multi-File Parallel Processing with Rayon

`basaltic-red` uses Rayon's work-stealing thread pool to saturate NVMe drive bandwidth and CPU cores:

```python
import basaltic_red as br

rules = ["passenger_count > 0", "trip_distance > 0.0", "fare_amount >= 2.5"]

# Automatically scales across all available CPU threads
summary = br.filter.filter_files_parallel("data/yellow_tripdata_2025-*.parquet", rules=rules)
```
