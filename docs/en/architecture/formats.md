---
title: Format Registry & Magic-Byte Sniffing
description: FormatHandler trait, built-in format table, dynamic registration, and header-byte detection
icon: material/file-code
---

# Format Registry & Magic-Byte Sniffing

Every file access in `basaltic-red` resolves to a `FormatHandler` — the pluggable abstraction in `src/engine/formats/mod.rs`.

---

## The `FormatHandler` Trait

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

Handlers return a lazy `OpenedSource` — an Arrow schema plus a streaming batch iterator. Row-based formats (CSV, JSONL, XLSX) chunk their rows through shared templates in `plugins/base_templates/row_chunker.rs`; columnar formats (Parquet, IPC) stream natively.

---

## Built-in Formats

Static table `HANDLERS` maps extensions to handlers:

| Tier | Extensions | Handler source |
| :--- | :--- | :--- |
| **1 — Core** | `parquet`, `pq`, `feather`, `arrow`, `ipc` | `formats/core/parquet.rs`, `formats/core/arrow_ipc.rs` |
| **2 — Common** | `csv`, `tsv`, `psv`, `txt`, `json`, `jsonl`, `ndjson` | `formats/common/csv.rs`, `formats/common/json.rs` |
| **3 — Pluggable adapters** | `xlsx`, `avro`, `orc`, `msgpack` | `formats/plugins/adapters/` |

Delimited variants share one template (`plugins/base_templates/delimited.rs`) differing only in delimiter byte and header flag.

---

## Handler Resolution Order

`resolve_handler_for_file()` tries, in order:

1. **Extension lookup** (`handler_for`) — O(1), checks the *dynamic registry first*, then the static table. Extension case is normalized to lowercase.
2. **Magic-byte sniffing** (`sniff_format_from_file`) — reads the first 512 bytes and inspects them via `sniff_format_from_bytes`:

| Header signature | Resolved format |
| :--- | :--- |
| `PAR1` | `parquet` |
| `ARROW1` | `feather` |
| `PK\x03\x04` (Zip) | `xlsx` |
| `Obj\x01` | `avro` |
| `ORC` | `orc` |
| `0x80–0x8F / 0xDE / 0xDF` (MessagePack map) | `msgpack` |
| first non-space byte `[` | `json` |
| first non-space byte `{` | `ndjson` |
| UTF-8 text line containing `\t` / `\|` / `;` / `,` | `tsv` / `psv` / `txt` / `csv` |

This is why files without any extension still open correctly.

---

## Dynamic Registration from Python

The dynamic registry lives behind a `RwLock<HashMap<String, Arc<dyn FormatHandler>>>` and is manipulated via [`br.formats.*`](../reference/python-api.md#basaltic_redformats):

```python
import basaltic_red as br

br.formats.register_delimited(ext="dat", delimiter="|", has_header=True)
print(br.formats.list_formats())     # includes "dat" among the built-ins
table = br.read.slice_rows("data/custom.dat", offset=0, limit=50)
br.formats.unregister_format("dat")  # returns True if it existed
```

Registered handlers override built-ins for the same extension.
