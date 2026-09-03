---
title: Python API Reference
description: Complete signatures for every basaltic_red module, function, and class
icon: material/code-braces
---

# Python API Reference

Signatures below match `src/pyapi/*.rs`. All functions return PyArrow objects (`Table` / `RecordBatch`) through the zero-copy bridge.

---

## Top-level exports

| Name | Kind |
| :--- | :--- |
| `basaltic_red.MatrixEngine(min_passenger=1, max_passenger=9, min_fare=0.01, max_speed_mph=100.0)` | class |
| `basaltic_red.PyBatchIterator` | class |

## `basaltic_red.read`

| Function | Returns |
| :--- | :--- |
| `slice_rows(file_path: str, offset: int, limit: int)` | `pyarrow.Table` |
| `slice_cols(file_path: str, selected_cols: list[str], offset: int, limit: int)` | `pyarrow.Table` |
| `preview_sample(file_path: str, limit_rows: int)` | `(pyarrow.Table, pyarrow.Table)`, static-threshold clean/trash split of the first batch |

## `basaltic_red.filter`

| Function | Returns |
| :--- | :--- |
| `process_batch(batch: pyarrow.RecordBatch)` | `(RecordBatch, RecordBatch)`, static-threshold clean/trash of one in-memory batch |
| `process_file(file_path: str, batch_size: int)` | `(total_rows, clean_rows, trash_rows)` |
| `filter_matrix(file_path: str, rules: list[str])` | `(pyarrow.Table, pyarrow.Table)`, dynamic rules; Trash gains audit columns |
| `filter_files_parallel(path_pattern: str, rules: list[str], partition_filter: str \| None = None, num_threads: int \| None = None)` | `dict` with keys `total_files`, `pruned_dirs`, `total_rows`, `clean_rows`, `trash_rows` |

## `basaltic_red.lake`

| Function | Returns |
| :--- | :--- |
| `ingest(src_dir: str, dst_dir: str, auto_normalize: bool \| None = None)` | `(files_ingested, rows_ingested)` |
| `split_file(file_path: str, max_rows_per_file: int, output_dir: str, format: str)` | number of parts written |
| `process_and_write_lake(input_dir: str, clean_output_dir: str, trash_output_dir: str, partition_filter: str \| None, batch_size: int)` | `(total_files, total_rows, clean_rows, trash_rows)` |
| `generate_gold_table(input_dir: str, gold_output_dir: str, table_version: str, partition_filter: str \| None, batch_size: int)` | `(files_read, gold_rows, manifest_path)` |
| `create_map(dir_path: str, show_progress: bool = True)` | path to the written `.br_map.ipc` (`str`) |
| `doctor(dir_path: str, auto_heal: bool = False)` | dict, see [Lake Doctor](../architecture/lake-map.md#lake-doctor) |

## `basaltic_red.sql`

| Function | Returns |
| :--- | :--- |
| `execute_sql(query: str)` | collected `pyarrow.Table` |
| `execute_sql_stream(query: str)` | `PyBatchIterator` |

### `PyBatchIterator`

- iterable, yields PyArrow `RecordBatch` objects
- `to_pyarrow()` → complete `pyarrow.Table`
- `repr()` → `PyBatchIterator(batches=N, rows=M)`

The `FROM '<path>'` target may be a file or directory. See [DataFusion SQL Layer](../architecture/datafusion.md).

## `basaltic_red.dictionary`

| Function | Returns |
| :--- | :--- |
| `export_data_dictionary_md(target_path: str, output_path: str)` | Markdown content string (also written to `output_path`) |

## `basaltic_red.graph`

| Function | Returns |
| :--- | :--- |
| `generate_er_graph(path: str, output_path: str \| None = None)` | Mermaid ER diagram string (also written when `output_path` given); `path` may be a file or directory |

## `basaltic_red.formats`

| Function | Returns |
| :--- | :--- |
| `register_delimited(ext: str, delimiter: str, has_header: bool = True)` | registers a custom delimited handler (first byte of `delimiter` is used) |
| `unregister_format(ext: str)` | `bool`, whether a dynamic handler was removed |
| `list_formats()` | sorted `list[str]` of supported extensions |

---

## Error Mapping

| Python exception | Raised by |
| :--- | :--- |
| `ValueError` | unsupported format, invalid rule syntax, invalid engine arguments |
| `RuntimeError` | DataFusion SQL failures, stream errors |
| `IOError` | missing/empty/corrupt files and other IO problems |
