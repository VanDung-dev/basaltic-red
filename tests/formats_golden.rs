//! Golden tests: each format handler must reproduce the same filter result on
//! the same 6-row taxi dataset. Fixtures are written in-memory, no static files.
use std::fs::File;
use std::sync::Arc;

use tempfile::tempdir;

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

use basaltic_red::engine::formats::handler_for;
use basaltic_red::engine::MatrixEngine;

const PASSENGERS: [i64; 6] = [1, 2, 0, 5, 12, 1];
const FARES: [f64; 6] = [15.5, -5.0, 20.0, 100.0, 50.0, 0.0];
const DISTANCES: [f64; 6] = [2.5, 0.0, 3.1, 10.0, 1.2, 5.0];

fn taxi_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("passenger_count", DataType::Int64, false),
        Field::new("fare_amount", DataType::Float64, false),
        Field::new("trip_distance", DataType::Float64, false),
    ]))
}

fn taxi_batch() -> RecordBatch {
    RecordBatch::try_new(
        taxi_schema(),
        vec![
            Arc::new(Int64Array::from(PASSENGERS.to_vec())),
            Arc::new(Float64Array::from(FARES.to_vec())),
            Arc::new(Float64Array::from(DISTANCES.to_vec())),
        ],
    )
    .unwrap()
}

/// Rows 0 and 3 pass; the other 4 are flagged.
fn assert_taxi_stats(ext: &str, file_path: &str, expected: (usize, usize, usize)) {
    let handler = handler_for(ext).unwrap();
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let stats = handler.process_file(&engine, file_path, 1024).unwrap();
    assert_eq!(stats, expected, "format .{ext}");
}

fn run_test(ext: &str, bytes: &[u8], expected: (usize, usize, usize)) {
    let dir = tempdir().unwrap();
    let path = dir.path().join(format!("data.{ext}"));
    std::fs::write(&path, bytes).unwrap();
    assert_taxi_stats(ext, path.to_str().unwrap(), expected);
}

// --- text formats ---
fn csv_text(delim: char) -> String {
    let mut s = format!("passenger_count{delim}fare_amount{delim}trip_distance\n");
    for i in 0..PASSENGERS.len() {
        s.push_str(&format!(
            "{}{}{}{}{}\n",
            PASSENGERS[i], delim, FARES[i], delim, DISTANCES[i]
        ));
    }
    s
}

#[test]
fn golden_csv() {
    run_test("csv", csv_text(',').as_bytes(), (6, 2, 4));
}

#[test]
fn golden_psv() {
    run_test("psv", csv_text('|').as_bytes(), (6, 2, 4));
}

#[test]
fn golden_txt() {
    run_test("txt", csv_text(';').as_bytes(), (6, 2, 4));
}

#[test]
fn golden_tsv_utf8() {
    // TSV handler forces every column to Utf8; the typed SIMD filter sees no
    // Int64/Float64 columns, so all rows pass clean.
    run_test("tsv", csv_text('\t').as_bytes(), (6, 6, 0));
}

// --- JSON family ---
fn json_lines() -> String {
    let mut s = String::new();
    for i in 0..PASSENGERS.len() {
        s.push_str(&format!(
            "{{\"passenger_count\":{},\"fare_amount\":{},\"trip_distance\":{}}}\n",
            PASSENGERS[i], FARES[i], DISTANCES[i]
        ));
    }
    s
}

#[test]
fn golden_json_array() {
    run_test(
        "json",
        format!("[{}]", json_lines().trim_end().replace('\n', ",")).as_bytes(),
        (6, 2, 4),
    );
}

#[test]
fn golden_jsonl_array() {
    run_test(
        "jsonl",
        format!("[{}]", json_lines().trim_end().replace('\n', ",")).as_bytes(),
        (6, 2, 4),
    );
}

#[test]
fn golden_ndjson_lines() {
    run_test("ndjson", json_lines().as_bytes(), (6, 2, 4));
}

// --- columnar formats ---
#[test]
fn golden_parquet() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.parquet");
    let f = File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(f, taxi_schema(), None).unwrap();
    writer.write(&taxi_batch()).unwrap();
    writer.close().unwrap();
    assert_taxi_stats("parquet", path.to_str().unwrap(), (6, 2, 4));
}

#[test]
fn golden_feather() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.feather");
    let f = File::create(&path).unwrap();
    let mut writer = arrow_ipc::writer::FileWriter::try_new(&f, &taxi_schema()).unwrap();
    writer.write(&taxi_batch()).unwrap();
    writer.finish().unwrap();
    drop(f);
    assert_taxi_stats("feather", path.to_str().unwrap(), (6, 2, 4));
}

// --- row-based binary formats ---
#[test]
fn golden_avro() {
    let schema = apache_avro::Schema::parse_str(
        r#"{"type":"record","name":"taxi","fields":[
            {"name":"passenger_count","type":"long"},
            {"name":"fare_amount","type":"double"},
            {"name":"trip_distance","type":"double"}
        ]}"#,
    )
    .unwrap();

    let dir = tempdir().unwrap();
    let path = dir.path().join("data.avro");
    let mut out = File::create(&path).unwrap();
    {
        let mut writer = apache_avro::Writer::new(&schema, &mut out);
        for i in 0..PASSENGERS.len() {
            writer
                .append(apache_avro::types::Value::Record(vec![
                    (
                        "passenger_count".into(),
                        apache_avro::types::Value::Long(PASSENGERS[i]),
                    ),
                    (
                        "fare_amount".into(),
                        apache_avro::types::Value::Double(FARES[i]),
                    ),
                    (
                        "trip_distance".into(),
                        apache_avro::types::Value::Double(DISTANCES[i]),
                    ),
                ]))
                .unwrap();
        }
        writer.flush().unwrap();
    }
    drop(out);
    assert_taxi_stats("avro", path.to_str().unwrap(), (6, 2, 4));
}

#[test]
fn golden_orc() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.orc");
    let f = File::create(&path).unwrap();
    let mut writer = orc_rust::ArrowWriterBuilder::new(f, taxi_schema())
        .try_build()
        .unwrap();
    writer.write(&taxi_batch()).unwrap();
    writer.close().unwrap();
    assert_taxi_stats("orc", path.to_str().unwrap(), (6, 2, 4));
}

#[test]
fn golden_xlsx() {
    let mut workbook = rust_xlsxwriter::Workbook::new();
    let sheet = workbook.add_worksheet();
    sheet.write_string(0, 0, "passenger_count").unwrap();
    sheet.write_string(0, 1, "fare_amount").unwrap();
    sheet.write_string(0, 2, "trip_distance").unwrap();
    for i in 0..PASSENGERS.len() {
        sheet
            .write_number(i as u32 + 1, 0, PASSENGERS[i] as f64)
            .unwrap();
        sheet.write_number(i as u32 + 1, 1, FARES[i]).unwrap();
        sheet.write_number(i as u32 + 1, 2, DISTANCES[i]).unwrap();
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.xlsx");
    workbook.save(&path).unwrap();
    // XlsxHandler maps every cell to Utf8, so the typed filter sees no numeric
    // columns and every row passes clean (same as TSV).
    assert_taxi_stats("xlsx", path.to_str().unwrap(), (6, 6, 0));
}

#[test]
fn golden_msgpack() {
    use rmpv::Value;

    let dir = tempdir().unwrap();
    let path = dir.path().join("data.msgpack");
    let mut out = File::create(&path).unwrap();
    for i in 0..PASSENGERS.len() {
        let map = Value::Map(vec![
            (
                Value::from("passenger_count"),
                Value::Integer(PASSENGERS[i].into()),
            ),
            (Value::from("fare_amount"), Value::from(FARES[i])),
            (Value::from("trip_distance"), Value::from(DISTANCES[i])),
        ]);
        rmpv::encode::write_value(&mut out, &map).unwrap();
    }
    drop(out);
    assert_taxi_stats("msgpack", path.to_str().unwrap(), (6, 2, 4));
}
