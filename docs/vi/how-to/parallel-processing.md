---
title: Xử lý dữ liệu lớn đa tệp với Rayon
description: Tận dụng nhóm luồng Rayon để tối đa hóa thông lượng ổ đĩa và CPU
icon: material/layers
---

# Xử lý dữ liệu lớn đa tệp với Rayon

`basaltic-red` sử dụng nhóm luồng Rayon tự động phân bổ tải trên toàn bộ lõi CPU:

```python
import basaltic_red as br

rules = ["passenger_count > 0", "trip_distance > 0.0", "fare_amount >= 2.5"]

# Tự động mở rộng trên tất cả các luồng CPU khả dụng
summary = br.filter.filter_files_parallel("data/yellow_tripdata_2025-*.parquet", rules=rules)
```
