---
title: Định dạng & Magic Byte
description: Trait FormatHandler, bảng định dạng sẵn có, đăng ký động và nhận diện qua byte đầu tệp
icon: material/file-code
---

# Registry Định dạng & Magic-Byte Sniffing

Mọi truy cập tệp trong `basaltic-red` đều được phân giải về một `FormatHandler`, lớp trừu tượng pluggable trong `src/engine/formats/mod.rs`.

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

Handler trả về `OpenedSource` kiểu lazy, một Arrow schema kèm iterator batch dạng streaming. Reader CSV/JSON và reader cột Parquet/IPC trả trực tiếp Arrow batch; adapter Avro, MessagePack và XLSX dùng `plugins/base_templates/row_chunker.rs` để chuyển row đã decode thành batch.

---

## Định dạng sẵn có

`HANDLERS` trong `formats/mod.rs` hiện đăng ký 16 extension. Các alias dùng chung handler được gom cùng hàng.
Việc quét locator và định tuyến slice nằm trong [`map.rs`](../../../src/engine/map.rs) và [`slice.rs`](../../../src/engine/slice.rs).

| Extension(s) | Schema, header, null và lỗi đọc | Cắt dòng và Lake Map | Cắt cột | Source |
| :--- | :--- | :--- | :--- | :--- |
| `.parquet`, `.pq` | Giữ schema/type Arrow trong Parquet, giữ Arrow null; file/lỗi đọc được trả về. | Checkpoint row group vật lý; map thiếu/cũ/không khớp thì dùng reader thường. | `ProjectionMask` ở cả nhánh map và fallback. | [parquet.rs](../../../src/engine/formats/core/parquet.rs) |
| `.feather`, `.arrow`, `.ipc` | Schema/type Arrow IPC **file** và Arrow null; file/lỗi đọc được trả về. | Lưu ordinal/khoảng dòng record batch; bắt đầu ở batch rồi skip cục bộ, không seek byte. | Truyền index field đã chọn xuống IPC reader ở cả hai nhánh. | [arrow_ipc.rs](../../../src/engine/formats/core/arrow_ipc.rs) |
| `.csv`, `.psv`, `.txt` | Dòng đầu là header; delimiter `,`, `|`, `;`. Arrow suy luận type tối đa 100 record và dùng null mặc định; không có token null riêng. Lỗi parser/type được trả về. | Byte checkpoint hiểu quote theo stride cấu hình; bỏ dòng vật lý rỗng. | Nhánh map parse cột được chọn nhưng vẫn đọc byte của từng record; fallback có thể project trong Arrow CSV. | [csv.rs](../../../src/engine/formats/common/csv.rs) |
| `.tsv` | Dòng đầu là header; mọi cột UTF-8 nullable; chính xác `\N` là null. Dòng ngắn đệm null; thừa field/lỗi parser gây lỗi. | Byte checkpoint hiểu quote theo stride cấu hình; bỏ dòng vật lý rỗng. | Nhánh map parse cột được chọn nhưng vẫn đọc byte của từng record; fallback có thể project trong Arrow CSV. | [csv.rs](../../../src/engine/formats/common/csv.rs) |
| `.json`, `.jsonl` | JSON object mỗi dòng hoặc mảng top-level các object; schema suy luận tối đa 100 record; field thiếu/null thành null. JSON sai, value không tương thích hoặc phần tử mảng không phải object gây lỗi. | Block dòng theo stride cấu hình: checkpoint đầu object nếu là mảng, nếu không checkpoint theo dòng. | Đọc row range rồi project. | [json.rs](../../../src/engine/formats/common/json.rs) |
| `.ndjson` | Dạng kỳ vọng: mỗi dòng một object; schema suy luận tối đa 100 record; field thiếu/null thành null. JSON sai/value không tương thích gây lỗi; dòng cuối không cần newline. | Byte checkpoint theo stride cấu hình của record không rỗng. | Đọc row range rồi project. | [json.rs](../../../src/engine/formats/common/json.rs) |
| `.orc` | Schema/type Arrow; giữ Arrow null; lỗi mở/decode ORC được trả về. | Checkpoint stripe vật lý; bắt đầu tại stripe chứa dòng đầu rồi skip cục bộ. | Đọc đủ cột của range rồi project. | [orc.rs](../../../src/engine/formats/plugins/adapters/orc.rs) |
| `.avro` | Field record nullable. `long`/`int`/`double`/`boolean` → `Int64`/`Int32`/`Float64`/`Boolean`; union một nhánh khác null dùng mapping của nhánh đó. Type khác là UTF-8 nhưng chỉ string chuyển được; value null/không hỗ trợ thành null. OCF/schema/data sai gây lỗi. | Checkpoint block Avro Object Container File; bắt đầu tại block chứa dòng đầu rồi skip cục bộ. | Đọc đủ row của range rồi project. | [avro.rs](../../../src/engine/formats/plugins/adapters/avro.rs) |
| `.msgpack` | Không có header. Map đầu định nghĩa field/type (key chuỗi đặt tên và điền field; key khác thành tên `col` nhưng không điền field) và là row đầu; value trước bị bỏ qua. Integer/F32/F64/Boolean → `Int64`/`Float64`/`Boolean`; type khác UTF-8, chỉ giữ string. Thiếu/incompatible value và row không phải map thành null; key thừa bỏ qua; phần cuối hỏng gây lỗi đọc. | Byte checkpoint theo stride cấu hình của top-level row kể từ map đầu; schema suy luận từ map đó trước khi đọc range. | Đọc đủ row của range rồi project. | [msgpack.rs](../../../src/engine/formats/plugins/adapters/msgpack.rs) |
| `.xlsx` | Chỉ worksheet đầu; dòng đầu là header; cột dữ liệu UTF-8; cell rỗng thành null. Thiếu sheet đọc được gây lỗi; thiếu header trả schema rỗng. | Block dòng logic theo stride cấu hình; Calamine vẫn materialize worksheet range. | Đọc row range rồi project. | [excel.rs](../../../src/engine/formats/plugins/adapters/excel.rs) |

Mọi `slice_cols` giữ thứ tự cột yêu cầu và báo lỗi nếu thiếu field. Parquet, Arrow IPC/Feather và ORC chuyển phép chiếu xuống reader; CSV/TSV/PSV/TXT trên nhánh map parse cột được chọn nhưng vẫn đọc đủ byte của record. Nhánh map JSON, Avro, MsgPack và XLSX decode khoảng dòng được chọn rồi mới project. Map chỉ dùng khi size và modification time còn khớp. Map thiếu/cũ hoặc entry không khớp thì fallback; sidecar không đọc được trả lỗi. Handler động cho extension built-in bỏ qua locator built-in. Dynamic delimited dùng `plugins/base_templates/delimited.rs` với delimiter/header do người đăng ký chọn.

---

## Thứ tự phân giải Handler

`resolve_handler_for_file()` thử lần lượt:

1. **Tra extension** (`handler_for`), O(1), kiểm *registry động trước*, rồi đến bảng tĩnh. Extension chuẩn hóa về chữ thường.
2. **Sniff magic-byte** (`sniff_format_from_file`), đọc 512 byte đầu và soi qua `sniff_format_from_bytes`:

| Chữ ký header | Định dạng suy ra |
| :--- | :--- |
| `PAR1` | `parquet` |
| `ARROW1` | `feather` |
| `PK\x03\x04` (Zip) | `xlsx` |
| `Obj\x01` | `avro` |
| `ORC` | `orc` |
| `0x80 đến 0x8F / 0xDE / 0xDF` (MessagePack map) | `msgpack` |
| byte không phải space đầu tiên là `[` | `json` |
| byte không phải space đầu tiên là `{` | `ndjson` |
| dòng UTF-8 chứa `\t` / `\|` / `;` / `,` | `tsv` / `psv` / `txt` / `csv` |

Các luồng dùng `resolve_handler_for_file()` cũng có thể mở tệp không phần mở rộng nhờ nhận diện magic byte. `filter_files_parallel` hiện tra handler theo extension, nên tệp đầu vào cần có extension đã đăng ký.

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

`handler_for()` ưu tiên handler đã đăng ký hơn built-in cùng extension. API slice giữ ưu tiên này kể cả khi có Lake Map; map không ghi locator built-in cho extension bị override và slice fallback sang handler đã đăng ký. Reader DataFusion `ListingTable` riêng của SQL không áp dụng override động; xem [SQL DataFusion](datafusion.md).
