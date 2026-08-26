---
title: Nhân SIMD Bitmask
description: Nhân kiểm tra bitmask đa khối không cấp phát và cách mã hóa cột kiểm toán
icon: material/cpu-64-bit
---

# Nhân Bitmask Đa Khối

Bộ lọc chất lượng động nằm trong `src/engine/dynamic_filter.rs`. Nó đánh giá N quy tắc tùy ý trên cả `RecordBatch` **không tạo mảng boolean trung gian**, ghi kết quả thẳng vào bitmask `u64` tại chỗ. Vòng lặp là Rust thuần trên Arrow array, LLVM tự vector hóa (không có intrinsic SIMD viết tay).

---

## Phân tích quy tắc

`FilterRule::parse()` tách mỗi chuỗi quy tắc tại toán tử đầu tiên tìm thấy (ưu tiên toán tử dài để `>=` thắng `>`):

```text
"<column> <op> <value>"   op ∈ { >=, <=, ==, !=, >, < }
```

- Giá trị có thể bọc trong ngoặc `'` hoặc `"` — ngoặc được cắt trước khi parse.
- Quy tắc số parse vế phải theo kiểu phần tử của cột.
- Cột chuỗi (`Utf8`, `LargeUtf8`) so sánh theo thứ tự từ điển.

## Vòng lặp đánh giá

Mỗi quy tắc sở hữu một vị trí bit: quy tắc *i* → khối `i / 64`, bit `i % 64`. Dòng vi phạm quy tắc nào thì bit tương ứng được bật và dòng bị đánh dấu không sạch:

```rust
for (rule_idx, rule) in rules.iter().enumerate() {
    let chunk_idx = rule_idx / 64;
    let bit = 1u64 << (rule_idx % 64);
    // vòng lặp theo kiểu cột ghi trực tiếp vào error_chunks_raw[chunk_idx]
}
```

Đặc tính:

- **Không cấp phát trong vòng lặp trong** — mask được cấp phát một lần mỗi batch (`Vec<Vec<u64>>`).
- **Mở rộng đa khối** — `num_chunks = ceil(rules / 64)`; 1 hay 500+ quy tắc đều hoạt động như nhau.
- **Xử lý NULL** — ô null luôn fail quy tắc (dòng rơi vào Trash).
- **Cột lạ hoặc kiểu dữ liệu không hỗ trợ** — quy tắc bị bỏ qua im lặng với batch đó (không đánh dấu dòng nào).

Kiểu cột hỗ trợ: `Int8/16/32/64`, `UInt8/16/32/64`, `Float32/64`, `Utf8`, `LargeUtf8`.

---

## Cột kiểm toán trên bảng Trash

Sau khi đánh giá, các dòng được tách bằng `filter_record_batch` và batch Trash được gắn thêm:

| Cột | Kiểu | Xuất hiện khi |
| :--- | :--- | :--- |
| `audit_error_code` | `UInt64` | luôn luôn — bitmask của các quy tắc vi phạm **0–63** (khối đầu tiên) |
| `audit_violated_rules` | `List<UInt32>` | chỉ khi **số quy tắc > 64** — danh sách mọi chỉ số quy tắc bị vi phạm |

Giải mã `audit_error_code`: bit *i* bật ⇔ quy tắc *i* vi phạm, ví dụ mã `0b101` nghĩa là quy tắc 0 và 2 fail. Với bộ quy tắc vượt 64, hãy đọc `audit_violated_rules` thay vì dựa vào một `UInt64` duy nhất.

Chi tiết mã hóa đầy đủ: [tham chiếu Mã lỗi Audit](../reference/audit-codes.md).
