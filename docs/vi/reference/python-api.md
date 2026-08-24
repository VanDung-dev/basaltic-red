---
title: Python API Reference
description: Chữ ký đầy đủ của mọi module, hàm và class trong basaltic_red
icon: material/code-braces
---

# Tài liệu tham chiếu Python API

Chữ ký dưới đây khớp với `src/pyapi/*.rs`. Mọi hàm trả về đối tượng PyArrow (`Table` / `RecordBatch`) qua cầu nối zero-copy.

---

## Export cấp cao nhất

| Tên | Loại |
| :--- | :--- |
| `basaltic_red.MatrixEngine(min_passenger=1, max_passenger=9, min_fare=0.01, max_speed_mph=100.0)` | class |
| `basaltic_red.PyBatchIterator` | class |

## `basaltic_red.read`

| Hàm | Trả về |
| :--- | :--- |
| `slice_rows(file_path: str, offset: int, limit: int)` | `pyarrow.Table` |
| `slice_cols(file_path: str, selected_cols: list[str], offset: int, limit: int)` | `pyarrow.Table` |
| `preview_sample(file_path: str, limit_rows: int)` | `(pyarrow.Table, pyarrow.Table)` — tách clean/trash theo ngưỡng tĩnh trên batch đầu tiên |

## `basaltic_red.filter`

| Hàm | Trả về |
| :--- | :--- |
| `process_batch(batch: pyarrow.RecordBatch)` | `(RecordBatch, RecordBatch)` — clean/trash ngưỡng tĩnh cho một batch trong RAM |
| `process_file(file_path: str, batch_size: int)` | `(total_rows, clean_rows, trash_rows)` |
| `filter_matrix(file_path: str, rules: list[str])` | `(pyarrow.Table, pyarrow.Table)` — quy tắc động; Trash có thêm cột kiểm toán |
| `filter_files_parallel(path_pattern: str, rules: list[str], partition_filter: str \| None = None, num_threads: int \| None = None)` | `dict` với khóa `total_files`, `pruned_dirs`, `total_rows`, `clean_rows`, `trash_rows` |

## `basaltic_red.lake`

| Hàm | Trả về |
| :--- | :--- |
| `ingest(src_dir: str, dst_dir: str, auto_normalize: bool \| None = None)` | `(files_ingested, rows_ingested)` |
| `split_file(file_path: str, max_rows_per_file: int, output_dir: str, format: str)` | số phần đã ghi |
| `process_and_write_lake(input_dir: str, clean_output_dir: str, trash_output_dir: str, partition_filter: str \| None, batch_size: int)` | `(total_files, total_rows, clean_rows, trash_rows)` |
| `generate_gold_table(input_dir: str, gold_output_dir: str, table_version: str, partition_filter: str \| None, batch_size: int)` | `(files_read, gold_rows, manifest_path)` |
| `create_map(dir_path: str)` | đường dẫn tới `.br_map.ipc` vừa ghi (`str`) |
| `doctor(dir_path: str, auto_heal: bool = False)` | dict — xem [Lake Doctor](../architecture/lake-map.md#lake-doctor) |

## `basaltic_red.sql`

| Hàm | Trả về |
| :--- | :--- |
| `execute_sql(query: str)` | `pyarrow.Table` thu thập đủ |
| `execute_sql_stream(query: str)` | `PyBatchIterator` |

### `PyBatchIterator`

- có thể duyệt — trả về từng `RecordBatch` PyArrow
- `to_pyarrow()` → `pyarrow.Table` đầy đủ
- `repr()` → `PyBatchIterator(batches=N, rows=M)`

Đích `FROM '<path>'` có thể là tệp hoặc thư mục. Xem [Tầng SQL DataFusion](../architecture/datafusion.md).

## `basaltic_red.dictionary`

| Hàm | Trả về |
| :--- | :--- |
| `export_data_dictionary_md(target_path: str, output_path: str)` | chuỗi Markdown (đồng thời ghi ra `output_path`) |

## `basaltic_red.graph`

| Hàm | Trả về |
| :--- | :--- |
| `generate_er_graph(path: str, output_path: str \| None = None)` | chuỗi sơ đồ ER Mermaid (ghi file nếu truyền `output_path`); `path` là tệp hoặc thư mục |

## `basaltic_red.formats`

| Hàm | Trả về |
| :--- | :--- |
| `register_delimited(ext: str, delimiter: str, has_header: bool = True)` | đăng ký handler phân cách tùy chỉnh (dùng byte đầu của `delimiter`) |
| `unregister_format(ext: str)` | `bool` — có gỡ được handler động hay không |
| `list_formats()` | `list[str]` các extension hỗ trợ, đã sắp xếp |

---

## Ánh xạ lỗi

| Exception Python | Phát sinh từ |
| :--- | :--- |
| `ValueError` | định dạng không hỗ trợ, cú pháp quy tắc sai, tham số engine không hợp lệ |
| `RuntimeError` | lỗi SQL DataFusion, lỗi stream |
| `IOError` | tệp thiếu/rỗng/hỏng và các vấn đề IO khác |
