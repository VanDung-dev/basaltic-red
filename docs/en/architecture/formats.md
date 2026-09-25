---
title: Format Registry & Magic-Byte Sniffing
description: FormatHandler trait, built-in format table, dynamic registration, and header-byte detection
icon: material/file-code
---

# Format Registry & Magic-Byte Sniffing

Every file access in `basaltic-red` resolves to a `FormatHandler`, the pluggable abstraction in `src/engine/formats/mod.rs`.

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

Handlers return a lazy `OpenedSource`, an Arrow schema plus a streaming batch iterator. The CSV/JSON readers and columnar Parquet/IPC readers yield Arrow batches directly; the Avro, MessagePack, and XLSX adapters use `plugins/base_templates/row_chunker.rs` to convert decoded rows into batches.

---

## Built-in Formats

`HANDLERS` in `formats/mod.rs` currently registers 16 extensions. Rows below group aliases that share one handler.
Locator inspection and slice routing live in [`map.rs`](../../../src/engine/map.rs) and [`slice.rs`](../../../src/engine/slice.rs).

| Extension(s) | Schema, header, nulls, and read errors | Row slicing and Lake Map | Column slicing | Source |
| :--- | :--- | :--- | :--- | :--- |
| `.parquet`, `.pq` | Stored Parquet Arrow schema/types; Arrow nulls preserved; invalid files or reads error. | Physical row-group checkpoints; missing/stale/nonmatching map uses the normal reader. | Parquet `ProjectionMask` on mapped and fallback paths. | [parquet.rs](../../../src/engine/formats/core/parquet.rs) |
| `.feather`, `.arrow`, `.ipc` | Arrow IPC **file** schema/types; Arrow nulls preserved; invalid files or reads error. | Record-batch ordinal and row span; starts at that batch and skips locally, with no byte seek. | Selected field indices passed to the IPC reader on mapped and fallback paths. | [arrow_ipc.rs](../../../src/engine/formats/core/arrow_ipc.rs) |
| `.csv`, `.psv`, `.txt` | First row header; delimiters `,`, `|`, `;`; Arrow infers types from up to 100 records and uses default null handling. No custom null token; parser/type errors propagate. | Quote-aware byte checkpoints at the configured stride; empty physical lines skipped. | Mapped reads parse selected columns but still read each selected record's bytes; fallback can project in Arrow CSV. | [csv.rs](../../../src/engine/formats/common/csv.rs) |
| `.tsv` | First row header; all columns nullable UTF-8; exact `\N` is null. Short rows pad with nulls; extra fields/parser errors fail. | Quote-aware byte checkpoints at the configured stride; empty physical lines skipped. | Mapped reads parse selected columns but still read each selected record's bytes; fallback can project in Arrow CSV. | [csv.rs](../../../src/engine/formats/common/csv.rs) |
| `.json`, `.jsonl` | Newline-delimited objects or a top-level array of object rows; schema inferred from up to 100 records; missing/null values become null. Invalid JSON, incompatible values, or non-object array elements error. | Configured-stride row blocks: object-start byte checkpoints for arrays, line checkpoints otherwise. | Reads the row range then projects. | [json.rs](../../../src/engine/formats/common/json.rs) |
| `.ndjson` | Expected shape: one object per line; schema inferred from up to 100 records; missing/null values become null. Invalid JSON or incompatible values error; final record may omit newline. | Byte checkpoints at the configured stride of nonblank records. | Reads the row range then projects. | [json.rs](../../../src/engine/formats/common/json.rs) |
| `.orc` | Arrow schema/types; Arrow nulls preserved; ORC open/decode errors propagate. | Physical stripe checkpoints; starts at the stripe containing the first requested row and skips locally. | Reads full columns for the range, then projects. | [orc.rs](../../../src/engine/formats/plugins/adapters/orc.rs) |
| `.avro` | Record fields nullable; `long`/`int`/`double`/`boolean` → `Int64`/`Int32`/`Float64`/`Boolean`; one non-null union branch uses its mapping. Other types are UTF-8 but only strings convert; unsupported/null values become null. Invalid OCF/schema/data errors. | Avro Object Container File block checkpoints; starts at the containing block and skips locally. | Reads full rows for the range, then projects. | [avro.rs](../../../src/engine/formats/plugins/adapters/avro.rs) |
| `.msgpack` | No header. First map defines fields/types (string keys name/populate fields; other keys fall back to `col` and do not populate fields) and is the first row; earlier values ignored. Integer/F32/F64/Boolean → `Int64`/`Float64`/`Boolean`; other types infer UTF-8, preserving strings only. Missing/incompatible values and non-map rows become null; extra keys ignored; truncated tails error. | Byte checkpoints at the configured stride of top-level rows from the first map; schema is inferred from that map before reading the range. | Reads full rows for the range, then projects. | [msgpack.rs](../../../src/engine/formats/plugins/adapters/msgpack.rs) |
| `.xlsx` | First worksheet; first row is header; all data columns UTF-8; empty cells null. No readable first sheet errors; no header row yields an empty schema. | Configured-stride logical data-row blocks; Calamine still materializes the worksheet range. | Reads the row range, then projects. | [excel.rs](../../../src/engine/formats/plugins/adapters/excel.rs) |

All `slice_cols` calls preserve requested order and error if a field is missing. Parquet, Arrow IPC/Feather, and ORC push projection into their readers; mapped CSV/TSV/PSV/TXT parses selected columns while reading complete selected record bytes. JSON, Avro, MsgPack, and XLSX map paths decode the selected row range then project. Map entries require matching file size and modification time. Missing/stale entries or entries for another file fall back; an unreadable sidecar returns an error. Dynamic handlers for built-in extensions bypass built-in locators. Dynamic delimited handlers use `plugins/base_templates/delimited.rs` with registration-selected delimiter/header.

---

## Handler Resolution Order

`resolve_handler_for_file()` tries, in order:

1. **Extension lookup** (`handler_for`), O(1), checks the *dynamic registry first*, then the static table. Extension case is normalized to lowercase.
2. **Magic-byte sniffing** (`sniff_format_from_file`), reads the first 512 bytes and inspects them via `sniff_format_from_bytes`:

| Header signature | Resolved format |
| :--- | :--- |
| `PAR1` | `parquet` |
| `ARROW1` | `feather` |
| `PK\x03\x04` (Zip) | `xlsx` |
| `Obj\x01` | `avro` |
| `ORC` | `orc` |
| `0x80 to 0x8F / 0xDE / 0xDF` (MessagePack map) | `msgpack` |
| first non-space byte `[` | `json` |
| first non-space byte `{` | `ndjson` |
| UTF-8 text line containing `\t` / `\|` / `;` / `,` | `tsv` / `psv` / `txt` / `csv` |

Routes that use `resolve_handler_for_file()` can also open extensionless files through magic-byte sniffing. `filter_files_parallel` currently looks up handlers by extension, so its input files need a registered extension.

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

`handler_for()` prefers a registered handler over a built-in handler for the same extension. Slice APIs honor that override even when a Lake Map exists; the map records no built-in locator for the overridden extension and slicing falls back to the registered handler. SQL's native DataFusion `ListingTable` readers use their own format readers and do not apply dynamic overrides; see [DataFusion SQL](datafusion.md).
