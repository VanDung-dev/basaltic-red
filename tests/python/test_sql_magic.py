import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("IPython")

import basaltic_sql
from IPython.core.interactiveshell import InteractiveShell


@pytest.fixture(scope="module")
def ip():
    shell = InteractiveShell.instance()
    basaltic_sql.load_ipython_extension(shell)
    yield shell
    InteractiveShell.clear_instance()


def _write_taxi(path):
    pq.write_table(
        pa.Table.from_pydict(
            {
                "passenger_count": [1, 2, 0, 5],
                "fare_amount": [15.5, 12.0, -5.0, 100.0],
            }
        ),
        path,
    )


def test_magic_interpolates_and_assigns(tmp_path, ip):
    p = tmp_path / "taxi.parquet"
    _write_taxi(p)
    ip.user_ns["dataset"] = str(p)

    result = ip.run_cell_magic(
        "basaltic_sql",
        "df",
        "SELECT passenger_count, fare_amount FROM '{dataset}' WHERE fare_amount > 0",
    )

    assert ip.user_ns["df"] is result
    assert result.height == 3


def test_magic_path_flag(tmp_path, ip):
    p = tmp_path / "taxi.parquet"
    _write_taxi(p)

    result = ip.run_cell_magic(
        "basaltic_sql", "out --path " + str(p), "SELECT COUNT(*) AS n FROM '{path}'"
    )

    assert "out" in ip.user_ns
    assert result.height == 1
