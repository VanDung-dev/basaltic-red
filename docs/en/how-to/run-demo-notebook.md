---
title: Run NYC Taxi Big Data Demo
description: Step-by-step guide to executing the real-world demo.ipynb notebook
icon: material/notebook
---

# Run NYC Taxi Big Data Demo

## Overview of `demo.ipynb`

The `demo.ipynb` notebook runs the full pipeline on the NYC Yellow Taxi 2009–2025 dataset (204 Parquet files, 29.66 GB, 1,826,960,642 rows — configurable `start_year`/`end_year` in the downloader cell):

1. **Step 0**: Environment setup, Maturing build, workspace cleanup, configurable `target_year` downloader.
2. **Step 1**: Lake Doctor initialization & zero-copy schema extraction.
3. **Step 2**: Slicing & DataFusion SQL streaming.
4. **Step 3**: Rayon SIMD parallel data quality filter.
5. **Step 4**: DataFusion analytics & publication Seaborn charts.
6. **Step 5**: Magic byte sniffing & >64 rules scale test.
7. **Step 6**: Final Lake Doctor integrity audit.

## Running the Notebook

```bash
uv run jupyter lab demo.ipynb
```
