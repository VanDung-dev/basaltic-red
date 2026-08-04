import os
import pandas as pd
import numpy as np

def generate_hive_dataset():
    base_dir = "test_hive_lakehouse"

    partitions = [
        ("year=2024/month=01", "part1.parquet", 10000, 1),
        ("year=2024/month=02", "part2.parquet", 10000, 10001),
        ("year=2025/month=01", "part3.parquet", 15000, 20001),
        ("year=2025/month=02", "part4.parquet", 15000, 35001),
        ("year=2026/month=08", "part5.parquet", 20000, 50001),
        ("year=2026/month=09", "part6.parquet", 30000, 70001),
    ]

    np.random.seed(42)

    for rel_path, filename, row_count, start_id in partitions:
        folder = os.path.join(base_dir, rel_path)
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, filename)

        df = pd.DataFrame({
            "id": range(start_id, start_id + row_count),
            "age": np.random.randint(15, 65, size=row_count),
            "salary": np.random.randint(500, 5000, size=row_count)
        })
        df.to_parquet(file_path, index=False)

    print("✅ Created test_hive_lakehouse with 6 Hive partitions and 100,000 rows total!")

if __name__ == "__main__":
    generate_hive_dataset()
