---
title: Installation
description: How to install and verify basaltic-red in Python and Rust environments
icon: material/download
---

# Installation & Setup

## Prerequisites

- **Python**: 3.12+ (`requires-python = ">=3.12"` in `pyproject.toml`)
- **Rust Toolchain**: stable `rustc` + `cargo` (automatically invoked when building via UV / Maturin)
- **Package Manager**: Recommended `uv` (fastest) or `pip`
- **Runtime dependency**: `pyarrow` (installed automatically)

---

## 1. Install Directly from GitHub via UV

Since `basaltic-red` is hosted on GitHub with a native Maturin PEP 517 build backend, you can install or add it directly with **UV** in a single command. UV will automatically fetch the repo, compile the Rust core, and install the package:

=== "Add to UV Project"
    ```bash
    uv add "git+https://github.com/VanDung-dev/basaltic-red.git"
    ```

=== "Install into Active Virtualenv"
    ```bash
    uv pip install "git+https://github.com/VanDung-dev/basaltic-red.git"
    ```

=== "With Optional Dependencies (Polars, DuckDB, Notebook)"
    ```bash
    # Install with interop & notebook support:
    uv add "basaltic-red[interop,notebook] @ git+https://github.com/VanDung-dev/basaltic-red.git"
    ```

---

## 2. Install from Local Source (Development Mode)

If you have cloned the repository locally:

1. **Clone repository**:
   ```bash
   git clone https://github.com/VanDung-dev/basaltic-red.git
   cd basaltic-red
   ```

2. **Set up virtual environment with UV**:
   ```bash
   uv venv
   source .venv/bin/activate  # On Linux/macOS
   # .venv\\Scripts\\activate  # On Windows
   ```

3. **Install dependencies and compile**:
   ```bash
   # Full dev setup (README quick start):
   uv sync --extra dev --extra interop
   uv run maturin develop --release

   # Or plain install from the local directory:
   uv pip install .
   ```

---

## 3. Verifying Installation

Run a quick Python command to verify that the PyO3 bindings and Rust SIMD engine are ready:

```python
import basaltic_red as br

print("Available Modules:", [m for m in dir(br) if not m.startswith("_")])
```

Output:
```text
Available Modules: ['MatrixEngine', 'PyBatchIterator', 'dictionary', 'filter', 'formats', 'graph', 'lake', 'read', 'sql']
```
