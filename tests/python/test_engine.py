"""Smoke/contract tests for the pyo3 MatrixEngine Python API.

These cover the pyo3 glue layer (constructor defaults, error mapping,
pyarrow interchange) that the Rust integration tests in tests/ cannot reach.
"""

import json
import os

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest

import basaltic_red

TAXI_CSV = """passenger_count,fare_amount,trip_distance
1,15.5,2.5
2,-5.0,0.0
0,20.0,3.1
5,100.0,10.0
12,50.0,1.2
1,0.0,5.0
"""


def test_constructor_defaults_and_custom_thresholds(tmp_path):
    csv_path = tmp_path / "taxi.csv"
    csv_path.write_text(TAXI_CSV)

    assert basaltic_red.MatrixEngine().process_file(str(csv_path), 1024) == (6, 2, 4)
    assert basaltic_red.MatrixEngine(min_passenger=0).process_file(str(csv_path), 1024) == (6, 3, 3)
    assert (
        basaltic_red.MatrixEngine(min_passenger=0, max_passenger=20).process_file(str(csv_path), 1024)
        == (6, 4, 2)
    )


def test_process_file_csv(tmp_path):
    csv_path = tmp_path / "taxi.csv"
    csv_path.write_text(TAXI_CSV)
    assert basaltic_red.MatrixEngine().process_file(str(csv_path), 1024) == (6, 2, 4)


def test_unsupported_ext_raises_valueerror(tmp_path):
    bad = tmp_path / "data.xyz"
    bad.write_text("x")
    with pytest.raises(ValueError):
        basaltic_red.MatrixEngine().process_file(str(bad), 1024)


def test_missing_file_raises_ioerror(tmp_path):
    missing = tmp_path / "nope.csv"
    with pytest.raises(IOError):
        basaltic_red.MatrixEngine().process_file(str(missing), 1024)


def test_process_batch_roundtrip():
    batch = pa.record_batch(
        [
            [1, 2, 0, 5, 12, 1],
            [15.5, -5.0, 20.0, 100.0, 50.0, 0.0],
            [2.5, 0.0, 3.1, 10.0, 1.2, 5.0],
        ],
        names=["passenger_count", "fare_amount", "trip_distance"],
    )
    clean, trash = basaltic_red.MatrixEngine().process_batch(batch)
    assert clean.num_rows == 2
    assert trash.num_rows == 4


# --- insane-input guards (things a user might actually try) ---


def _write_csv(tmp_path, content):
    p = tmp_path / "taxi.csv"
    p.write_text(content)
    return str(p)


def test_huge_batch_size_is_clamped(tmp_path):
    # batch_size = 10^12 used to force a ~24GB allocation and hang the process
    p = _write_csv(tmp_path, TAXI_CSV)
    assert basaltic_red.MatrixEngine().process_file(p, 10**12) == (6, 2, 4)


def test_zero_batch_size(tmp_path):
    p = _write_csv(tmp_path, TAXI_CSV)
    assert basaltic_red.MatrixEngine().process_file(p, 0) == (0, 0, 0)


def test_negative_batch_size_raises(tmp_path):
    p = _write_csv(tmp_path, TAXI_CSV)
    with pytest.raises(OverflowError):
        basaltic_red.MatrixEngine().process_file(p, -5)


def test_empty_file(tmp_path):
    p = _write_csv(tmp_path, "")
    assert basaltic_red.MatrixEngine().process_file(p, 1024) == (0, 0, 0)


def test_header_only_file(tmp_path):
    p = _write_csv(tmp_path, "passenger_count,fare_amount,trip_distance\n")
    assert basaltic_red.MatrixEngine().process_file(p, 1024) == (0, 0, 0)


def test_uppercase_extension(tmp_path):
    p = tmp_path / "taxi.CSV"
    p.write_text(TAXI_CSV)
    assert basaltic_red.MatrixEngine().process_file(str(p), 1024) == (6, 2, 4)


def test_extension_with_suffix(tmp_path):
    p = tmp_path / "taxi.csvx"
    p.write_text(TAXI_CSV)
    with pytest.raises(ValueError):
        basaltic_red.MatrixEngine().process_file(str(p), 1024)


def test_missing_numeric_columns_all_clean(tmp_path):
    p = _write_csv(tmp_path, "a,b\n1,2\n3,4\n")
    assert basaltic_red.MatrixEngine().process_file(p, 1024) == (2, 2, 0)


def test_directory_path_raises_valueerror(tmp_path):
    with pytest.raises(ValueError):
        basaltic_red.MatrixEngine().process_file(str(tmp_path), 1024)


def test_empty_path_raises_valueerror():
    with pytest.raises(ValueError):
        basaltic_red.MatrixEngine().process_file("", 1024)


def test_wrong_path_types_raise_typeerror(tmp_path):
    e = basaltic_red.MatrixEngine()
    with pytest.raises(TypeError):
        e.process_file(123, 1024)
    with pytest.raises(TypeError):
        e.process_file(None, 1024)


def test_constructor_wrong_types_raise_typeerror():
    with pytest.raises(TypeError):
        basaltic_red.MatrixEngine("a", "b")
    with pytest.raises(TypeError):
        basaltic_red.MatrixEngine(min_passenger=None)


def test_flipped_thresholds_no_rows_pass(tmp_path):
    p = _write_csv(tmp_path, TAXI_CSV)
    e = basaltic_red.MatrixEngine(min_passenger=50, max_passenger=1)
    assert e.process_file(p, 1024) == (6, 0, 6)


def test_process_batch_wrong_type_raises_typeerror():
    e = basaltic_red.MatrixEngine()
    with pytest.raises(TypeError):
        e.process_batch(pa.table({"a": [1, 2]}))
    with pytest.raises(TypeError):
        e.process_batch([1, 2, 3])


def test_process_batch_empty_batch():
    b = pa.record_batch([[]], names=["passenger_count"])
    clean, trash = basaltic_red.MatrixEngine().process_batch(b)
    assert clean.num_rows == 0
    assert trash.num_rows == 0


# --- full README command coverage ---


def _taxi_table():
    return pa.table(
        {
            "passenger_count": [1, 2, 0, 5, 12, 1],
            "fare_amount": [15.5, -5.0, 20.0, 100.0, 50.0, 0.0],
            "trip_distance": [2.5, 0.0, 3.1, 10.0, 1.2, 5.0],
        }
    )


def _write_format(tmp_path, fmt):
    """Write the 6-row taxi dataset in the given format, return its path."""
    t = _taxi_table()
    lines = ["1,15.5,2.5", "2,-5.0,0.0", "0,20.0,3.1", "5,100.0,10.0", "12,50.0,1.2", "1,0.0,5.0"]
    p = tmp_path / f"data.{fmt}"
    if fmt == "csv":
        p.write_text("passenger_count,fare_amount,trip_distance\n" + "\n".join(lines) + "\n")
    elif fmt == "tsv":
        p.write_text(
            "passenger_count\tfare_amount\ttrip_distance\n"
            + "\n".join(l.replace(",", "\t") for l in lines)
            + "\n"
        )
    elif fmt == "psv":
        p.write_text(
            "passenger_count|fare_amount|trip_distance\n"
            + "\n".join(l.replace(",", "|") for l in lines)
            + "\n"
        )
    elif fmt == "txt":
        p.write_text(
            "passenger_count;fare_amount;trip_distance\n"
            + "\n".join(l.replace(",", ";") for l in lines)
            + "\n"
        )
    elif fmt == "json":
        p.write_text(json.dumps(t.to_pylist(), indent=2))
    elif fmt == "jsonl":
        p.write_text(json.dumps(t.to_pylist()))
    elif fmt == "ndjson":
        p.write_text("\n".join(json.dumps(r) for r in t.to_pylist()) + "\n")
    elif fmt == "parquet":
        pq.write_table(t, p)
    elif fmt in ("feather", "ipc"):
        with ipc.new_file(p, t.schema) as writer:
            writer.write_table(t)
    else:
        raise ValueError(f"unknown format {fmt}")
    return str(p)


# tsv forces all columns to Utf8 -> filter sees no numeric cols -> everything clean.
_ALL_FORMATS = {
    "csv": (6, 2, 4),
    "psv": (6, 2, 4),
    "txt": (6, 2, 4),
    "tsv": (6, 6, 0),
    "json": (6, 2, 4),
    "jsonl": (6, 2, 4),
    "ndjson": (6, 2, 4),
    "parquet": (6, 2, 4),
    "feather": (6, 2, 4),
    "ipc": (6, 2, 4),
}


@pytest.mark.parametrize("fmt", list(_ALL_FORMATS))
def test_process_file_all_formats(tmp_path, fmt):
    assert basaltic_red.MatrixEngine().process_file(_write_format(tmp_path, fmt), 1024) == _ALL_FORMATS[fmt]


def test_slice_rows(tmp_path):
    p = _write_format(tmp_path, "parquet")
    batch = basaltic_red.MatrixEngine().slice_rows(p, offset=1, limit=2)
    assert batch.num_rows == 2
    assert batch.num_columns == 3


def test_slice_cols(tmp_path):
    p = _write_format(tmp_path, "csv")
    batch = basaltic_red.MatrixEngine().slice_cols(
        p, selected_cols=["passenger_count", "fare_amount"], offset=0, limit=3
    )
    assert batch.num_rows == 3
    assert batch.num_columns == 2


def test_filter_matrix(tmp_path):
    p = _write_format(tmp_path, "csv")
    clean, trash = basaltic_red.MatrixEngine().filter_matrix(p, rules=["fare_amount > 0"])
    assert clean.num_rows == 4  # fares 15.5, 20, 100, 50
    assert trash.num_rows == 2  # fares -5, 0


def test_filter_files_parallel_dir(tmp_path):
    (tmp_path / "part1.csv").write_text("id,age,salary\n1,25,1000\n2,15,500\n")
    (tmp_path / "part2.csv").write_text("id,age,salary\n3,30,1200\n4,17,400\n")
    summary = basaltic_red.MatrixEngine().filter_files_parallel(str(tmp_path), rules=["age >= 18"])
    assert summary["total_files"] == 2
    assert summary["total_rows"] == 4
    assert summary["clean_rows"] == 2
    assert summary["trash_rows"] == 2


def test_filter_files_parallel_partition_pruning(tmp_path):
    lake = tmp_path / "lake"
    (lake / "year=2026" / "month=08").mkdir(parents=True)
    (lake / "year=2025" / "month=08").mkdir(parents=True)
    (lake / "year=2026" / "month=08" / "a.csv").write_text("id,age,salary\n1,25,1000\n2,15,500\n")
    (lake / "year=2025" / "month=08" / "b.csv").write_text("id,age,salary\n3,30,1200\n")
    summary = basaltic_red.MatrixEngine().filter_files_parallel(
        str(lake), rules=["age >= 18"], partition_filter="year=2026/month=08"
    )
    assert summary == {"total_files": 1, "pruned_dirs": 1, "total_rows": 2, "clean_rows": 1, "trash_rows": 1}


def test_execute_sql_on_directory(tmp_path):
    db = tmp_path / "db"
    db.mkdir()
    (db / "data.csv").write_text("id,age,salary\n1,25,1000\n2,15,500\n3,30,1200\n")

    result = basaltic_red.MatrixEngine().execute_sql(
        f"SELECT id, salary FROM '{db}' WHERE age >= 18 ORDER BY salary DESC"
    )
    assert result.num_rows == 2
    assert result.num_columns == 2


def test_split_file(tmp_path):
    p = _write_format(tmp_path, "parquet")
    parts = tmp_path / "parts"
    n = basaltic_red.MatrixEngine().split_file(p, max_rows_per_file=1, output_dir=str(parts), format="parquet")
    assert n == 6
    assert len(list(parts.glob("*.parquet"))) == 6


def test_export_data_dictionary_md(tmp_path):
    p = _write_format(tmp_path, "parquet")
    out = tmp_path / "schema.md"
    result = basaltic_red.MatrixEngine().export_data_dictionary_md(str(p), str(out))
    assert result == str(out)
    assert "passenger_count" in out.read_text()


def test_generate_er_graph(tmp_path):
    rel = tmp_path / "rel"
    rel.mkdir()
    pq.write_table(pa.table({"id": [1, 2], "age": [25, 15]}), rel / "users.parquet")
    pq.write_table(pa.table({"id": [1, 2], "user_id": [1, 2]}), rel / "orders.parquet")
    out = tmp_path / "er.md"
    mermaid = basaltic_red.MatrixEngine().generate_er_graph_py(str(rel), str(out))
    assert mermaid.startswith("```mermaid")
    assert out.exists()


def test_generate_gold_table(tmp_path):
    rel = tmp_path / "rel"
    rel.mkdir()
    pq.write_table(_taxi_table(), rel / "taxi.parquet")
    gold = tmp_path / "gold"
    total_files, gold_rows, manifest_path = basaltic_red.MatrixEngine().generate_gold_table(
        str(rel), str(gold), "v1", None, 1024
    )
    assert total_files == 1
    assert gold_rows == 2
    assert os.path.exists(manifest_path)


def test_process_and_write_lake(tmp_path):
    rel = tmp_path / "rel"
    rel.mkdir()
    pq.write_table(_taxi_table(), rel / "taxi.parquet")
    stats = basaltic_red.MatrixEngine().process_and_write_lake(
        str(rel), str(tmp_path / "clean"), str(tmp_path / "trash"), None, 1024
    )
    assert stats == (1, 6, 2, 4)


def test_preview_sample(tmp_path):
    p = _write_format(tmp_path, "parquet")
    clean, trash = basaltic_red.MatrixEngine().preview_sample(p, limit_rows=2)
    assert clean.num_rows == 1  # first 2 taxi rows: 1 clean, 1 trash
    assert trash.num_rows == 1


def test_execute_sql_stream(tmp_path):
    p = _write_format(tmp_path, "parquet")
    stream = basaltic_red.MatrixEngine().execute_sql_stream(
        f"SELECT passenger_count, fare_amount FROM '{p}' WHERE fare_amount > 0"
    )
    batches = list(stream)
    assert len(batches) > 0
    for batch in batches:
        assert isinstance(batch, pa.RecordBatch)
    table = pa.Table.from_batches(batches)
    assert table.num_rows > 0
    assert table.num_columns == 2


def test_execute_sql_stream_to_pyarrow(tmp_path):
    p = _write_format(tmp_path, "parquet")
    stream = basaltic_red.MatrixEngine().execute_sql_stream(
        f"SELECT passenger_count, fare_amount FROM '{p}' WHERE fare_amount > 0"
    )
    table = stream.to_pyarrow()
    assert isinstance(table, pa.Table)
    assert table.num_rows > 0
    assert table.num_columns == 2


def test_namespaced_subcommands(tmp_path):
    p = _write_format(tmp_path, "parquet")
    assert basaltic_red.read.slice_rows(str(p), offset=0, limit=2).num_rows == 2
    assert basaltic_red.filter.process_file(str(p), 1024) == (6, 2, 4)
    assert basaltic_red.sql.execute_sql(f"SELECT COUNT(*) AS c FROM '{p}'").to_pydict() == {
        "c": [6]
    }
    summary = basaltic_red.filter.filter_files_parallel(
        str(tmp_path), rules=["fare_amount > 0"]
    )
    assert summary["total_files"] >= 1
    n = basaltic_red.lake.split_file(
        str(p), max_rows_per_file=2, output_dir=str(tmp_path / "out"), format="csv"
    )
    assert n >= 1


def test_sql_stream_repr(tmp_path):
    p = _write_format(tmp_path, "parquet")
    stream = basaltic_red.sql.execute_sql_stream(
        f"SELECT passenger_count, fare_amount FROM '{p}' WHERE fare_amount > 0"
    )
    assert "PyBatchIterator(batches=" in repr(stream)


def test_dynamic_format_plugin_registration_python(tmp_path):
    custom_file = tmp_path / "data.piped"
    custom_file.write_text("id|name|amount\n1|Alice|100\n2|Bob|200\n3|Charlie|300\n")

    # Dynamic registration
    basaltic_red.formats.register_delimited(ext="piped", delimiter="|", has_header=True)
    assert "piped" in basaltic_red.formats.list_formats()

    # Slice rows
    df = basaltic_red.read.slice_rows(str(custom_file), offset=0, limit=2)
    assert df.num_rows == 2

    # SQL query
    res = basaltic_red.sql.execute_sql(f"SELECT name, amount FROM '{custom_file}' WHERE amount > 150")
    assert res.num_rows == 2

    # Unregister
    assert basaltic_red.formats.unregister_format("piped") is True


def test_magic_sniff_file_without_extension_python(tmp_path):
    no_ext_file = tmp_path / "raw_parquet_blob"
    no_ext_file.write_text(TAXI_CSV)

    # Should sniff CSV without extension
    df = basaltic_red.read.slice_rows(str(no_ext_file), offset=0, limit=3)
    assert df.num_rows == 3

    # SQL query on file without extension
    res = basaltic_red.sql.execute_sql(f"SELECT * FROM '{no_ext_file}' WHERE fare_amount > 0")
    assert res.num_rows == 4


def test_create_map_with_progress(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    lake_dir = tmp_path / "test_lake"
    lake_dir.mkdir()

    for i in range(5):
        tbl = pa.Table.from_pydict({"id": [1, 2, 3], "val": [10.0, 20.0, 30.0]})
        pq.write_table(tbl, lake_dir / f"part_{i}.parquet")

    # Test create_map with progress bar enabled and disabled
    map_path_1 = basaltic_red.lake.create_map(str(lake_dir), show_progress=True)
    assert (lake_dir / ".br_map.ipc").exists()
    assert map_path_1.endswith(".br_map.ipc")

    # Test doctor
    report = basaltic_red.lake.doctor(str(lake_dir), auto_heal=True)
    assert report["status"] == "HEALTHY"
    assert report["total_files"] == 5
    assert report["healthy_count"] == 5


def test_sql_with_string_literal_before_from(tmp_path):
    p = _write_format(tmp_path, "parquet")
    # Verifies string literal 'active' before FROM clause is not parsed as file path
    res = basaltic_red.sql.execute_sql(
        f"SELECT 'active' AS status, passenger_count FROM '{p}' WHERE passenger_count > 0"
    )
    assert res.num_rows > 0
    assert "status" in res.column_names
    assert res["status"][0].as_py() == "active"


def test_path_sandboxing_violation(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    data_dir = tmp_path / "allowed_data"
    data_dir.mkdir()
    p = _write_format(data_dir, "parquet")

    monkeypatch.setenv("BASALTIC_RED_DATA_ROOT", str(data_dir))

    # Allowed path inside sandbox succeeds
    res = basaltic_red.sql.execute_sql(f"SELECT * FROM '{p}'")
    assert res.num_rows > 0

    # Path outside sandbox is rejected
    outside_file = tmp_path / "outside.parquet"
    pq.write_table(pa.Table.from_pydict({"x": [1]}), outside_file)

    with pytest.raises(Exception) as exc_info:
        basaltic_red.sql.execute_sql(f"SELECT * FROM '{outside_file}'")
    assert "Path traversal denied" in str(exc_info.value)


def test_lake_map_atomic_write_and_doctor(tmp_path):
    from pathlib import Path
    lake_dir = tmp_path / "atomic_lake"
    lake_dir.mkdir()
    _write_format(lake_dir, "parquet")

    map_path = basaltic_red.lake.create_map(str(lake_dir))
    assert Path(map_path).exists()

    report = basaltic_red.lake.doctor(str(lake_dir), auto_heal=True)
    assert report["status"] == "HEALTHY"
