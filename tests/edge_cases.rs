//! Edge-case / "weird input" coverage for the unified read pipeline: offsets
//! and limits past EOF, multi-batch streaming, malformed or empty inputs,
//! registry aliases, empty directories. These pin behaviours that a casual
//! happy-path test would never touch.

use std::fs::File;
use std::sync::Arc;

use tempfile::tempdir;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use basaltic_red::engine::dynamic_filter::FilterRule;
use basaltic_red::engine::formats::handler_for;
use basaltic_red::engine::MatrixEngine;

fn engine() -> MatrixEngine {
    MatrixEngine::new(1, 9, 0.01, 100.0)
}

fn big_csv(path: &std::path::Path, n: usize) {
    let mut s = "id,val\n".to_string();
    for i in 0..n {
        s.push_str(&format!("{},{}\n", i, i as f64));
    }
    std::fs::write(path, s).unwrap();
}

// --- read_range: the streaming skip/collect/stop logic -----------------------

#[test]
fn read_range_spans_multiple_batches() {
    // batch_size=10 over 100 rows forces read_range to skip 5 batches, keep a
    // partial slice, then stop. Must return exactly rows 50..70.
    let dir = tempdir().unwrap();
    let path = dir.path().join("big.csv");
    big_csv(&path, 100);

    let handler = handler_for("csv").unwrap();
    let batch = handler
        .read_range(path.to_str().unwrap(), 50, 20, 10)
        .unwrap();

    assert_eq!(batch.num_rows(), 20);
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 50);
    assert_eq!(ids.value(19), 69);
}

#[test]
fn read_range_partial_tail() {
    // limit runs past the end of the file: only what exists is returned.
    let dir = tempdir().unwrap();
    let path = dir.path().join("big.csv");
    big_csv(&path, 10);

    let handler = handler_for("csv").unwrap();
    let batch = handler
        .read_range(path.to_str().unwrap(), 8, 10, 4)
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 8);
    assert_eq!(ids.value(1), 9);
}

#[test]
fn read_range_offset_past_eof_returns_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("big.csv");
    big_csv(&path, 10);

    let batch = engine()
        .slice_rows_native(path.to_str().unwrap(), 100, 5)
        .unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn read_range_zero_limit_returns_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("big.csv");
    big_csv(&path, 10);

    let batch = engine()
        .slice_rows_native(path.to_str().unwrap(), 0, 0)
        .unwrap();
    assert_eq!(batch.num_rows(), 0);
}

// --- row-oriented formats: RowChunker batches must also slice correctly -------

#[test]
fn read_range_avro_across_chunks() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("seq.avro");
    let schema = apache_avro::Schema::parse_str(
        r#"{"type":"record","name":"seq","fields":[{"name":"id","type":"long"}]}"#,
    )
    .unwrap();
    {
        let out = File::create(&path).unwrap();
        let mut writer = apache_avro::Writer::new(&schema, out);
        for i in 0..25i64 {
            writer
                .append(apache_avro::types::Value::Record(vec![(
                    "id".into(),
                    apache_avro::types::Value::Long(i),
                )]))
                .unwrap();
        }
        writer.flush().unwrap();
    }

    // batch_size=10 -> 3 chunks; offset 5 limit 12 crosses chunk boundaries.
    let handler = handler_for("avro").unwrap();
    let batch = handler
        .read_range(path.to_str().unwrap(), 5, 12, 10)
        .unwrap();
    assert_eq!(batch.num_rows(), 12);
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 5);
    assert_eq!(ids.value(11), 16);
}

// --- slice_cols --------------------------------------------------------------

#[test]
fn slice_cols_missing_column_errors() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("big.csv");
    big_csv(&path, 3);

    let err = engine()
        .slice_cols_native(path.to_str().unwrap(), &["nope".into()], 0, 2)
        .unwrap_err()
        .to_string();
    assert!(err.contains("Column 'nope' not found"), "{err}");
}

// --- weird inputs ------------------------------------------------------------

#[test]
fn json_object_is_single_row() {
    // A bare JSON object (not an array) is read by arrow's native reader as a
    // single record. Pin the behaviour so a refactor can't silently change it.
    let dir = tempdir().unwrap();
    let path = dir.path().join("obj.json");
    std::fs::write(&path, r#"{"id":1,"val":"x"}"#).unwrap();

    let stats = handler_for("json")
        .unwrap()
        .process_file(&engine(), path.to_str().unwrap(), 1024)
        .unwrap();
    assert_eq!(stats, (1, 1, 0));
}

#[test]
fn empty_jsonl_is_zero_rows() {
    // Every text format returns (0,0,0) on an empty file; jsonl used to hard
    // error with a JSON EOF before the fix.
    let dir = tempdir().unwrap();
    for ext in ["csv", "tsv", "json", "jsonl", "ndjson"] {
        let path = dir.path().join(format!("e.{ext}"));
        std::fs::write(&path, "").unwrap();
        let stats = handler_for(ext)
            .unwrap()
            .process_file(&engine(), path.to_str().unwrap(), 1024)
            .unwrap();
        assert_eq!(stats, (0, 0, 0), "empty .{ext}");
    }
}

#[test]
fn msgpack_rows_before_first_map_are_dropped() {
    // Schema comes from the first Map row; a leading non-map value is skipped.
    use rmpv::Value;
    let dir = tempdir().unwrap();
    let path = dir.path().join("d.msgpack");
    let mut out = File::create(&path).unwrap();
    rmpv::encode::write_value(&mut out, &Value::from(42)).unwrap();
    rmpv::encode::write_value(
        &mut out,
        &Value::Map(vec![(Value::from("id"), Value::from(1))]),
    )
    .unwrap();

    let stats = handler_for("msgpack")
        .unwrap()
        .process_file(&engine(), path.to_str().unwrap(), 1024)
        .unwrap();
    assert_eq!(stats, (1, 1, 0));
}

#[test]
fn empty_parquet_slices_to_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let f = File::create(&path).unwrap();
    let writer = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    writer.close().unwrap();

    let batch = engine()
        .slice_rows_native(path.to_str().unwrap(), 0, 5)
        .unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn empty_orc_is_zero_rows() {
    // A schema-only ORC file (no stripes) must read as zero rows, not error.
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.orc");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let f = File::create(&path).unwrap();
    let writer = orc_rust::ArrowWriterBuilder::new(f, schema)
        .try_build()
        .unwrap();
    writer.close().unwrap();

    let stats = handler_for("orc")
        .unwrap()
        .process_file(&engine(), path.to_str().unwrap(), 1024)
        .unwrap();
    assert_eq!(stats, (0, 0, 0));
}

#[test]
fn read_range_orc_across_batches() {
    // batch_size=10 over 25 rows forces the orc-rust reader to emit 3 batches;
    // read_range must skip, slice and stop across batch boundaries.
    let dir = tempdir().unwrap();
    let path = dir.path().join("seq.orc");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids: Int64Array = (0..25).collect();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)]).unwrap();
    let f = File::create(&path).unwrap();
    let mut writer = orc_rust::ArrowWriterBuilder::new(f, schema)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let handler = handler_for("orc").unwrap();
    let batch = handler
        .read_range(path.to_str().unwrap(), 5, 12, 10)
        .unwrap();
    assert_eq!(batch.num_rows(), 12);
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 5);
    assert_eq!(ids.value(11), 16);
}

#[test]
fn registry_aliases_resolve() {
    for ext in [
        "csv", "tsv", "psv", "txt", "json", "jsonl", "ndjson", "parquet", "pq", "feather", "arrow",
        "ipc", "avro", "msgpack", "xlsx", "orc",
    ] {
        assert!(handler_for(ext).is_some(), "handler_for({ext})");
    }
    assert!(handler_for("nope").is_none());
}

// --- directory error paths ------------------------------------------------

#[test]
fn filter_nonexistent_path_errors() {
    let dir = tempdir().unwrap();
    let rules = vec![FilterRule::parse("age >= 18").unwrap()];
    let err = engine()
        .filter_files_parallel_native(
            dir.path().join("missing.csv").to_str().unwrap(),
            &rules,
            None,
            None,
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("Path does not exist"), "{err}");
}

#[test]
fn filter_glob_pattern() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), "id,age\n1,25\n2,10\n").unwrap();
    std::fs::write(dir.path().join("b.csv"), "id,age\n3,30\n4,5\n").unwrap();
    std::fs::write(dir.path().join("c.txt"), "id,age\n9,50\n").unwrap();

    let rules = vec![FilterRule::parse("age >= 18").unwrap()];
    let pattern = format!("{}/*.csv", dir.path().display());
    let summary = engine()
        .filter_files_parallel_native(&pattern, &rules, None, Some(2))
        .unwrap();
    assert_eq!(summary.total_files, 2);
    assert_eq!(summary.clean_rows, 2);
    assert_eq!(summary.trash_rows, 2);
}

// --- SQL error paths ---------------------------------------------------------

#[tokio::test]
async fn sql_nonexistent_path_errors() {
    let dir = tempdir().unwrap();
    let missing = dir.path().join("nope.csv");
    let err = engine()
        .execute_sql(&format!("SELECT * FROM '{}'", missing.display()))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("not found"), "{err}");
}

#[tokio::test]
async fn sql_empty_result_errors() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("d.csv");
    std::fs::write(&path, "id,val\n1,10\n").unwrap();

    let err = engine()
        .execute_sql(&format!("SELECT * FROM '{}' WHERE id > 99", path.display()))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("returned 0 rows"), "{err}");
}

// --- lazy execute_sql_stream ------------------------------------------------

#[tokio::test]
async fn sql_stream_is_lazy_and_emits_batches() {
    use futures::StreamExt;

    let dir = tempdir().unwrap();
    let path = dir.path().join("big.csv");
    big_csv(&path, 100);

    let mut stream = engine()
        .execute_sql_stream_inner(&format!("SELECT id FROM '{}' WHERE id < 5", path.display()))
        .await
        .unwrap();

    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 5);
}

#[tokio::test]
async fn sql_stream_empty_result_no_error() {
    // The lazy stream must not raise the eager "0 rows" error.
    use futures::StreamExt;

    let dir = tempdir().unwrap();
    let path = dir.path().join("d.csv");
    std::fs::write(&path, "id,val\n1,10\n").unwrap();

    let mut stream = engine()
        .execute_sql_stream_inner(&format!(
            "SELECT * FROM '{}' WHERE id > 99",
            path.display()
        ))
        .await
        .unwrap();

    let mut got = 0;
    while let Some(batch) = stream.next().await {
        got += batch.unwrap().num_rows();
    }
    assert_eq!(got, 0);
}

// --- SQL auto-normalize cache -----------------------------------------------

#[tokio::test]
async fn sql_auto_normalize_caches_msgpack_to_parquet() {
    std::env::set_var("BASALTIC_RED_AUTO_NORMALIZE", "1");
    std::env::set_var("BASALTIC_RED_CACHE_DIR", "");
    let dir = tempdir().unwrap();
    let src = dir.path().join("data.msgpack");
    {
        use rmpv::Value;
        let mut out = File::create(&src).unwrap();
        for (id, val) in [(1i64, 10.5f64), (2, 20.5), (1, 30.5), (2, 40.5)] {
            let map = Value::Map(vec![
                (Value::from("id"), Value::Integer(id.into())),
                (Value::from("val"), Value::from(val)),
            ]);
            rmpv::encode::write_value(&mut out, &map).unwrap();
        }
    }

    // First query: no cache → transcode msgpack → parquet under <.br_cache>.
    let table = engine()
        .execute_sql(&format!("SELECT COUNT(*) AS c FROM '{}'", src.display()))
        .await
        .unwrap();
    let c = table
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(c.value(0), 4);

    let cache = src
        .parent()
        .unwrap()
        .join(".br_cache")
        .join("data.msgpack.parquet");
    assert!(cache.exists(), "cache parquet not created");

    // Second query: hits the cached parquet instead of re-reading msgpack.
    let table = engine()
        .execute_sql(&format!("SELECT val FROM '{}' WHERE id = 1", src.display()))
        .await
        .unwrap();
    assert_eq!(table.num_rows(), 2);
}
