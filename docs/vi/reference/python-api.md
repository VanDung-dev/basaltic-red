---
title: Python API Reference
description: Chữ ký đầy đủ của mọi module, hàm và class trong basaltic_red
icon: material/code-braces
---

# Tài liệu tham chiếu Python API

Chữ ký dưới đây khớp với `src/pyapi/*.rs`. Các hàm trả về `Table` hoặc `RecordBatch` đi qua ranh giới PyO3 bằng Arrow C Data Interface; các hàm khác trả về scalar, tuple hoặc dictionary như liệt kê bên dưới.

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
| `preview_sample(file_path: str, limit_rows: int)` | `(pyarrow.Table, pyarrow.Table)`, tách clean/trash theo ngưỡng tĩnh trên batch đầu tiên |

Lệnh slice trả về khoảng dòng được yêu cầu; `slice_cols` trả cột theo thứ tự `selected_cols`. Parquet, Arrow IPC/Feather và ORC chuyển phép chiếu cột xuống reader. Delimited trên đường mapped project cột khi parse nhưng vẫn đọc byte của từng record được chọn; Avro, MsgPack, họ JSON và XLSX có thể chiếu sau khi decode các dòng được chọn. Khi không có locator Lake Map phù hợp, fallback có thể stream rồi bỏ qua từ đầu. Đọc trực tiếp một file có thể sniff một số file không có extension hoặc extension lạ; discovery thư mục của map và Lake Doctor dựa trên extension và chỉ gồm extension có handler đang hoạt động.

## `basaltic_red.filter`

| Hàm | Trả về |
| :--- | :--- |
| `process_batch(batch: pyarrow.RecordBatch)` | `(RecordBatch, RecordBatch)`, clean/trash ngưỡng tĩnh cho một batch trong RAM |
| `process_file(file_path: str, batch_size: int)` | `(total_rows, clean_rows, trash_rows)` |
| `filter_matrix(file_path: str, rules: list[str])` | `(pyarrow.Table, pyarrow.Table)`, quy tắc động; Trash có thêm cột kiểm toán |
| `filter_files_parallel(path_pattern: str, rules: list[str], partition_filter: str \| None = None, num_threads: int \| None = None)` | `dict` với khóa `total_files`, `pruned_dirs`, `total_rows`, `clean_rows`, `trash_rows` |

`filter_files_parallel` chỉ cắt tỉa phân vùng khi `path_pattern` là thư mục. Đích là một tệp hoặc glob được liệt kê trực tiếp; `partition_filter` không thu hẹp các đích đó.

## `basaltic_red.lake`

| Hàm | Trả về |
| :--- | :--- |
| `ingest(src_dir: str, dst_dir: str, auto_normalize: bool \| None = None)` | `(files_ingested, rows_ingested)` |
| `split_file(file_path: str, max_rows_per_file: int, output_dir: str, format: str)` | số phần đã ghi |
| `process_and_write_lake(input_dir: str, clean_output_dir: str, trash_output_dir: str, partition_filter: str \| None, batch_size: int)` | `(total_files, total_rows, clean_rows, trash_rows)` |
| `generate_gold_table(input_dir: str, gold_output_dir: str, table_version: str, partition_filter: str \| None, batch_size: int)` | `(files_read, gold_rows, manifest_path)` |
| `create_map(dir_path: str, show_progress: bool = True, *, checkpoint_stride_rows: int = 65536, fingerprint: str = "metadata", stats_columns: list[str] \| None = None)` | đường dẫn tới `.br_map.bazan` vừa ghi (`str`) |
| `locate_row(dir_path: str, global_row: int)` | dict vị trí hoặc `None`: file, offset trong file, row-group, dòng cục bộ và trạng thái page-index |
| `doctor(dir_path: str, auto_heal: bool = False)` | dict, xem [Lake Doctor](../architecture/lake-map.md#lake-doctor) |

`checkpoint_stride_rows` phải lớn hơn 0 và điều khiển checkpoint cho định dạng theo dòng; row group Parquet, stripe ORC, block Avro, batch Arrow và block logic XLSX vẫn theo ranh giới riêng của định dạng. `fingerprint` nhận `"metadata"` (mặc định) hoặc `"blake3"`. `stats_columns=None` tính mọi thống kê hiện được hỗ trợ, `[]` tắt thống kê, còn danh sách tên giới hạn cột được thống kê. Tắt thống kê vẫn cần đọc tệp để đếm dòng và tạo locator.

`locate_row` phân giải từ map đã lưu và không kiểm tra độ mới của file nguồn. Hãy chạy `doctor` sau khi thêm, xóa hoặc sửa file; với BLAKE3, Doctor có thể phát hiện nội dung đổi nhưng giữ nguyên dung lượng và mtime. `create_map` và `doctor` discovery theo extension được hỗ trợ; file không có extension nhưng đọc trực tiếp được nằm ngoài phạm vi thư mục này. Extension động phải được đăng ký trong mỗi process chạy tạo hoặc kiểm tra map. Map schema 3 lưu các tùy chọn này và hash BLAKE3 tùy chọn cho từng file; map 5 cột cũ và schema 2 được nạp với mặc định trước đây. Slice vẫn chỉ kiểm size/mtime để không hash lại toàn file ở mỗi range.

## `basaltic_red.sql`

| Hàm | Trả về |
| :--- | :--- |
| `execute_sql(query: str)` | `pyarrow.Table` thu thập đủ |
| `execute_sql_stream(query: str)` | `PyBatchIterator` |

### `PyBatchIterator`

- có thể duyệt, trả về từng `RecordBatch` PyArrow
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
| `unregister_format(ext: str)` | `bool`, có gỡ được handler động hay không |
| `list_formats()` | `list[str]` các extension hỗ trợ, đã sắp xếp |

---

## Ánh xạ lỗi

| Exception Python | Phát sinh từ |
| :--- | :--- |
| `ValueError` | định dạng không hỗ trợ, cú pháp quy tắc sai, tham số engine không hợp lệ |
| `RuntimeError` | lỗi SQL DataFusion, lỗi stream |
| `IOError` | tệp thiếu/rỗng/hỏng và các vấn đề IO khác |
