---
title: Bác sĩ Data Lake & Tự phục hồi
description: Chu trình vận hành kiểm tra toàn vẹn và phục hồi dữ liệu trôi dạt
icon: material/doctor
---

# Luồng Bác sĩ Data Lake & Tự phục hồi

## Chu trình vận hành chuẩn

Bác sĩ Data Lake là điểm bắt đầu và điểm kết thúc bắt buộc trong mọi pipeline xử lý dữ liệu:

```mermaid
sequenceDiagram
    autonumber
    participant P as Pipeline dữ liệu
    participant D as Lake Doctor

    P->>D: kiểm tra đầu vào: doctor("data", auto_heal=True)
    D-->>P: chuẩn HEALTHY · bản đồ sẵn sàng
    P->>P: ingest · cắt lát · lọc SIMD · SQL
    P->>D: kiểm tra đầu ra: doctor("data", auto_heal=True)
    D-->>P: toàn vẹn 100% · không để lại trạng thái rác
```

## Code mẫu

```python
import basaltic_red as br

# Chạy chẩn đoán
status = br.lake.doctor("data", auto_heal=False)
if status["status"] != "HEALTHY":
    print("Phát hiện sai lệch! Đang tự phục hồi...")
    br.lake.doctor("data", auto_heal=True)
```
