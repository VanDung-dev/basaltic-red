---
title: Phân vùng Dữ liệu Sạch & Rác
description: Cơ chế phân tách dữ liệu sạch và dữ liệu rác kèm mã lỗi bitwise
icon: material/call-split
---

# Luồng phân vùng Dữ liệu Sạch & Rác

Khi thực thi lọc chất lượng dữ liệu, các bản ghi được chia thành hai bảng riêng biệt:

---

## 1. Bảng Dữ Liệu Sạch (Clean Table)
Chứa toàn bộ các dòng thỏa mãn **100%** các quy tắc kiểm tra. Giữ nguyên định dạng schema gốc.

## 2. Bảng Dữ Liệu Rác (Trash Table)
Chứa tất cả các bản ghi vi phạm ít nhất 1 quy tắc, được tự động bổ sung các cột kiểm toán:
- `audit_error_code` (`UInt64`): Mã bitmask trong đó bit thứ $i$ bật lên $1$ nếu quy tắc $i$ bị vi phạm (64 quy tắc đầu tiên).
- `audit_violated_rules` (`List<UInt32>`, chỉ xuất hiện khi dùng >64 quy tắc): danh sách đầy đủ các chỉ số quy tắc bị vi phạm.

```python
clean_batch, trash_batch = br.filter.filter_matrix("data/sample.parquet", rules=rules)
```

Chi tiết mã hóa: [Mã lỗi & Cột kiểm toán](../reference/audit-codes.md).

## Ghi cây Sạch/Rác ra đĩa

[`br.lake.process_and_write_lake`](../architecture/lakehouse-pipeline.md) tách mọi tệp Parquet trong thư mục thành hai cây Parquet đối xứng theo **ngưỡng tĩnh** của engine (không phải quy tắc tùy chỉnh):

```python
stats = br.lake.process_and_write_lake(
    "input_dir", "output/clean/", "output/trash/",
    partition_filter=None, batch_size=65536,
)
# → (tổng_tệp, tổng_dòng, dòng_sạch, dòng_rác)
```

Tệp Trash mang cột `audit_error_code` tĩnh; kết quả nén ZSTD và giữ nguyên cấu trúc thư mục tương đối.
