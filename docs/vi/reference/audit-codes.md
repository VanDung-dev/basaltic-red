---
title: Mã lỗi & Cột kiểm toán
description: Cách mã hóa bitmask và các cột kiểm toán gắn vào đầu ra trash
icon: material/alert-circle-check
---

# Mã lỗi Audit

Khi chạy quy tắc động (`filter_matrix`), mỗi dòng Trash ghi lại **quy tắc nào** khiến nó bị loại.

## `audit_error_code`, `UInt64`, nullable

Bit thứ *i* bật khi quy tắc *i* (đánh số từ 0) bị vi phạm:

$$\text{audit\_error\_code} = \sum_{i \in \text{vi phạm},\, i < 64} 2^i$$

Ví dụ: mã `0b101` (=5) nghĩa là quy tắc 0 và 2 fail; quy tắc 1 đạt.

!!! warning "Chỉ 64 quy tắc đầu nằm gọn trong UInt64"

    Cột này lưu khối 0 của bitmask. Khi dùng hơn 64 quy tắc, vi phạm của quy tắc ≥ 64 **không** phản ánh ở cột này.

## `audit_violated_rules`, `List<UInt32>`, nullable

Chỉ được thêm vào schema Trash khi **số quy tắc > 64**. Mỗi dòng chứa danh sách đầy đủ các chỉ số quy tắc vi phạm qua mọi khối, ví dụ `[0, 2, 71]`.

## Kiểm toán theo ngưỡng tĩnh

Đường tắt tĩnh (`process_batch`, `process_file`, `process_and_write_lake`, `preview_sample`) dùng ba cờ bit cố định thay thế:

| Cờ | Bit | Điều kiện |
| :--- | :--- | :--- |
| `ERR_INVALID_PASSENGER` | 1<<0 | `passenger_count` null hoặc ngoài `[min_passenger, max_passenger]` |
| `ERR_INVALID_FARE` | 1<<1 | `fare_amount` null hoặc thấp hơn `min_fare` |
| `ERR_INVALID_SPEED` | 1<<2 | vi phạm giá **và** `trip_distance > 0` (bất thường distance/fare) |

Mã được tính như tổng có trọng số vector hóa (`p*1 + f*2 + s*4`) và lưu dưới dạng `UInt64` không null trong đầu ra Trash chế độ tĩnh. Cột thiếu dữ liệu đóng góp không có vi phạm.

## Schema bảng Clean

Không đổi, bảng Clean luôn giữ nguyên schema gốc, không thêm cột.
