---
title: Cài đặt & Thiết lập
description: Hướng dẫn cài đặt basaltic-red trên Python và môi trường phát triển Rust
icon: material/download
---

# Cài đặt & Thiết lập

## Yêu cầu môi trường

- **Python**: 3.12+ (`requires-python = ">=3.12"` trong `pyproject.toml`)
- **Trình biên dịch Rust**: `rustc` ổn định + `cargo` (UV / Maturin sẽ tự động gọi khi biên dịch)
- **Công cụ quản lý gói**: Khuyến nghị `uv` (tốc độ nhanh nhất) hoặc `pip`
- **Phụ thuộc runtime**: `pyarrow` (tự động cài kèm)

---

## 1. Cài đặt trực tiếp từ GitHub qua UV

Vì `basaltic-red` được cấu hình chuẩn build backend PEP 517 với Maturin trong `pyproject.toml`, người dùng có thể cài đặt hoặc thêm trực tiếp package từ Git thông qua **UV** chỉ với một câu lệnh:

=== "Thêm vào dự án UV"
    ```bash
    uv add "git+https://github.com/VanDung-dev/basaltic-red.git"
    ```

=== "Cài vào môi trường ảo đang kích hoạt"
    ```bash
    uv pip install "git+https://github.com/VanDung-dev/basaltic-red.git"
    ```

=== "Kèm theo các gói mở rộng (Polars, DuckDB, Notebook)"
    ```bash
    # Cài đặt kèm hỗ trợ phân tích và Jupyter Notebook:
    uv add "basaltic-red[interop,notebook] @ git+https://github.com/VanDung-dev/basaltic-red.git"
    ```

---

## 2. Cài đặt từ mã nguồn cục bộ (Chế độ phát triển)

1. **Clone kho mã nguồn**:
   ```bash
   git clone https://github.com/VanDung-dev/basaltic-red.git
   cd basaltic-red
   ```

2. **Khởi tạo môi trường ảo với UV**:
   ```bash
   uv venv
   source .venv/bin/activate  # Trên Linux/macOS
   # .venv\\Scripts\\activate  # Trên Windows
   ```

3. **Biên dịch và cài đặt**:
   ```bash
   # Thiết lập dev đầy đủ (theo README):
   uv sync --extra dev --extra interop
   uv run maturin develop --release

   # Hoặc cài đặt thường từ thư mục hiện tại:
   uv pip install .
   ```

---

## 3. Kiểm tra cài đặt

```python
import basaltic_red as br

print("Phiên bản Basaltic-Red:", br.__version__)
print("Các module khả dụng:", [m for m in dir(br) if not m.startswith("_")])
```

Kết quả:
```text
Phiên bản Basaltic-Red: 0.1.0
Các module khả dụng: ['MatrixEngine', 'PyBatchIterator', 'dictionary', 'filter', 'formats', 'graph', 'lake', 'read', 'sql']
```
