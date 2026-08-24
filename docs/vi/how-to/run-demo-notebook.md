---
title: Hướng dẫn chạy Demo NYC Taxi
description: Hướng dẫn chi tiết từng bước thực thi sổ tay demo.ipynb trên dữ liệu lớn
icon: material/notebook
---

# Hướng dẫn chạy Demo NYC Taxi

## Tổng quan sổ tay `demo.ipynb`

Sổ tay `demo.ipynb` cung cấp quy trình phân tích hoàn chỉnh trên toàn bộ 12 tháng dữ liệu NYC Yellow Taxi 2025 (~4.3 triệu dòng/tháng):

1. **Step 0**: Thiết lập môi trường, build Maturin, dọn dẹp workspace, cấu hình biến `target_year` tải dữ liệu.
2. **Step 1**: Khởi tạo Bác sĩ Data Lake & trích xuất schema zero-copy.
3. **Step 2**: Cắt lát dữ liệu & luồng DataFusion SQL.
4. **Step 3**: Lọc chất lượng dữ liệu song song Rayon SIMD.
5. **Step 4**: Phân tích DataFusion & vẽ biểu đồ Seaborn.
6. **Step 5**: Nhận diện byte & kiểm thử quy mô >64 quy tắc.
7. **Step 6**: Kiểm toán toàn vẹn Data Lake kết thúc chu trình.

## Chạy sổ tay

```bash
uv run jupyter lab demo.ipynb
```
