import os
import pandas as pd
import numpy as np

def generate_dataset():
    output_dir = "test_real_dataset"
    os.makedirs(f"{output_dir}/subfolder_a", exist_ok=True)
    os.makedirs(f"{output_dir}/subfolder_b", exist_ok=True)

    np.random.seed(42)

    # File 1: CSV (10,000 rows)
    df1 = pd.DataFrame({
        "id": range(1, 10001),
        "age": np.random.randint(15, 65, size=10000),
        "salary": np.random.randint(500, 5000, size=10000)
    })
    df1.to_csv(f"{output_dir}/part_01.csv", index=False)

    # File 2: CSV (10,000 rows)
    df2 = pd.DataFrame({
        "id": range(10001, 20001),
        "age": np.random.randint(15, 65, size=10000),
        "salary": np.random.randint(500, 5000, size=10000)
    })
    df2.to_csv(f"{output_dir}/part_02.csv", index=False)

    # File 3: Parquet (20,000 rows)
    df3 = pd.DataFrame({
        "id": range(20001, 40001),
        "age": np.random.randint(15, 65, size=20000),
        "salary": np.random.randint(500, 5000, size=20000)
    })
    df3.to_parquet(f"{output_dir}/subfolder_a/part_03.parquet", index=False)

    # File 4: Parquet (20,000 rows)
    df4 = pd.DataFrame({
        "id": range(40001, 60001),
        "age": np.random.randint(15, 65, size=20000),
        "salary": np.random.randint(500, 5000, size=20000)
    })
    df4.to_parquet(f"{output_dir}/subfolder_a/part_04.parquet", index=False)

    # File 5: CSV (15,000 rows)
    df5 = pd.DataFrame({
        "id": range(60001, 75001),
        "age": np.random.randint(15, 65, size=15000),
        "salary": np.random.randint(500, 5000, size=15000)
    })
    df5.to_csv(f"{output_dir}/subfolder_b/part_05.csv", index=False)

    # File 6: Parquet (25,000 rows)
    df6 = pd.DataFrame({
        "id": range(75001, 100001),
        "age": np.random.randint(15, 65, size=25000),
        "salary": np.random.randint(500, 5000, size=25000)
    })
    df6.to_parquet(f"{output_dir}/subfolder_b/part_06.parquet", index=False)

    print("✅ Created test_real_dataset with 6 files and 100,000 rows total!")

if __name__ == "__main__":
    generate_dataset()
