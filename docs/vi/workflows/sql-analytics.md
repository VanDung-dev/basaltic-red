---
title: Phân tích SQL Zero-Copy
description: Chu trình phân tích dữ liệu kết hợp DataFusion, Polars và DuckDB
icon: material/database-search
---

# Luồng phân tích SQL Zero-Copy

## Tích hợp truyền luồng kết quả

Chạy các truy vấn tổng hợp SQL trực tiếp trên tệp Parquet sạch và chuyển tiếp sang Polars:

```python
import polars as pl
import basaltic_red as br

# Thực thi SQL DataFusion dưới dạng luồng
stream = br.sql.execute_sql_stream("""
    SELECT 
        passenger_count, 
        COUNT(*) AS total_trips,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM 'data/output/clean_trips.parquet'
    GROUP BY passenger_count
    ORDER BY total_trips DESC
""")

# Chuyển đổi sang Polars DataFrame không qua copy bộ nhớ
df = pl.from_arrow(stream.to_pyarrow())
print(df)
```
