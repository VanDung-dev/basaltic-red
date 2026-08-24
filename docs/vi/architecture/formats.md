---
title: Định dạng & Magic Byte
description: Trait FormatHandler, bảng định dạng sẵn có, đăng ký động và nhận diện qua byte đầu tệp
icon: material/file-code
---

# Registry Định dạng & Magic-Byte Sniffing

Mọi truy cập tệp trong `basaltic-red` đều được phân giải về một `FormatHandler` — lớp trừu tượng pluggable trong `src/engine/formats/mod.rs`.

---

## Trait `FormatHandler`

```rust
pub struct OpenedSource {
    pub schema: SchemaRef,
    pub batches: Box<dyn Iterator<Item = Result<RecordBatch, BazanError>> + Send>,
}

pub trait FormatHandler: Send + Sync {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError>;
    fn process_file(&self, engine: &MatrixEngine, file_path: &str, batch_size: usize)
        -> Result<(usize, usize, usize), BazanError>;
    fn read_range(&self, file_path: &str, offset: usize, limit: usize, batch_size: usize)
        -> Result<RecordBatch, BazanError>;
    fn open_with_columns(&self, file_path: &str, batch_size: usize, columns: &[String])
        -> Result<OpenedSource, BazanError>;
    fn read_range_columns(&self, /* ... */) -> Result<RecordBatch, BazanError>;
}
```

Handler trả về `OpenedSource` kiểu lazy — một Arrow schema kèm iterator batch dạng streaming. Các định dạng theo dòng (CSV, JSONL, XLSX) chia khối qua template dùng chung tại `plugins/base_templates/row_chunker.rs`; các định dạng cột (Parquet, IPC) stream thuần túy.

---

## Định dạng sẵn có

Bảng tĩnh `HANDLERS` ánh xạ extension về handler:

| Tier | Extension | Nguồn handler |
| :--- | :--- | :--- |
| **1 — Core** | `parquet`, `pq`, `feather`, `arrow`, `ipc` | `formats/core/parquet.rs`, `formats/core/arrow_ipc.rs` |
| **2 — Common** | `csv`, `tsv`, `psv`, `txt`, `json`, `jsonl`, `ndjson` | `formats/common/csv.rs`, `formats/common/json.rs` |
| **3 — Pluggable adapters** | `xlsx`, `avro`, `orc`, `msgpack` | `formats/plugins/adapters/` |

Các biến thể phân cách dùng chung một template (`plugins/base_templates/delimited.rs`), chỉ khác byte phân cách và cờ header.

---

## Thứ tự phân giải Handler

`resolve_handler_for_file()` thử lần lượt:

1. **Tra extension** (`handler_for`) — O(1), kiểm *registry động trước*, rồi đến bảng tĩnh. Extension chuẩn hóa về chữ thường.
2. **Sniff magic-byte** (`sniff_format_from_file`) — đọc 512 byte đầu và soi qua `sniff_format_from_bytes`:

| Chữ ký header | Định dạng suy ra |
| :--- | :--- |
| `PAR1` | `parquet` |
| `ARROW1` | `feather` |
| `PK\x03\x04` (Zip) | `xlsx` |
| `Obj\x01` | `avro` |
| `ORC` | `orc` |
| `0x80–0x8F / 0xDE / 0xDF` (MessagePack map) | `msgpack` |
| byte không phải space đầu tiên là `[` | `json` |
| byte không phải space đầu tiên là `{` | `ndjson` |
| dòng UTF-8 chứa `\t` / `\|` / `;` / `,` | `tsv` / `psv` / `txt` / `csv` |

Đây là lý do tệp không có extension vẫn mở đúng.

---

## Đăng ký động từ Python

Registry động nằm sau `RwLock<HashMap<String, Arc<dyn FormatHandler>>>` và thao tác qua [`br.formats.*`](../reference/python-api.md#basaltic_redformats):

```python
import basaltic_red as br

br.formats.register_delimited(ext="dat", delimiter="|", has_header=True)
print(br.formats.list_formats())     # gồm cả "dat" cùng các built-in
table = br.read.slice_rows("data/custom.dat", offset=0, limit=50)
br.formats.unregister_format("dat")  # trả True nếu từng tồn tại
```

Handler đăng ký động sẽ đè built-in cùng extension.
