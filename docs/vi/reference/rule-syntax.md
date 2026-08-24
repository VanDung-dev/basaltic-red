---
title: Cú pháp quy tắc lọc
description: Ngữ pháp, toán tử và ngữ nghĩa đánh giá của quy tắc lọc động
icon: material/filter-variant
---

# Tài liệu tham chiếu Cú pháp quy tắc

Quy tắc động là chuỗi thuần túy được `FilterRule::parse` phân tích (`src/engine/dynamic_filter.rs`):

```text
<column> <operator> <value>
```

## Toán tử

| Toán tử | Mô tả | Kiểu cột hỗ trợ |
| :--- | :--- | :--- |
| `>` | Lớn hơn | Int8/16/32/64, UInt8/16/32/64, Float32/64 |
| `>=` | Lớn hơn hoặc bằng | như `>` |
| `<` | Nhỏ hơn | như `>` |
| `<=` | Nhỏ hơn hoặc bằng | như `>` |
| `==` | Bằng | kiểu số + `Utf8`, `LargeUtf8` |
| `!=` | Khác | kiểu số + `Utf8`, `LargeUtf8` |

Bộ phân tích chọn **toán tử đầu tiên** tìm thấy, ưu tiên toán tử dài (`>=` trước `>`), nên vị trí khoảng trắng linh hoạt: `"age>=18"` tương đương `"age >= 18"`.

## Giá trị

- Quy tắc số parse vế phải theo kiểu phần tử của cột (ví dụ `"fare_amount >= 2.5"`, `"passenger_count > 0"`).
- So sánh chuỗi có thể bọc ngoặc — cả `'N'` và `"N"` đều được cắt ngoặc: `"store_and_fwd_flag == 'N'"`.

## Ngữ nghĩa đánh giá

| Tình huống | Kết quả với dòng dữ liệu |
| :--- | :--- |
| Quy tắc thỏa mãn | bit giữ 0 → dòng vẫn ở Clean |
| Quy tắc vi phạm **hoặc giá trị NULL** | bit bật → dòng rơi vào Trash kèm `audit_error_code` |
| Cột không tồn tại trong schema | quy tắc bị bỏ qua im lặng (không đánh dấu dòng nào) |
| Kiểu cột không hỗ trợ (ngày tháng, decimal, list…) | quy tắc bị bỏ qua với batch đó |
| Giá trị không parse được theo kiểu cột | quy tắc bị bỏ qua |

Quy tắc không bao giờ làm thay đổi dữ liệu; chúng chỉ quyết định thành viên Clean hay Trash. Xem [Nhân SIMD Bitmask](../architecture/simd-kernel.md) để hiểu cách mã hóa vi phạm.
