//! Unified read pipeline tests: `slice_rows_native`, `slice_cols_native`,
//! `filter_files_parallel_native`, `execute_sql` and `.bazan` packing must all
//! work on every format through `FormatHandler::open`. Previously slice only
//! handled parquet + delimiters and filter failed on json/feather.
use std::fs::File;
use std::sync::Arc;

use tempfile::tempdir;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use basaltic_red::engine::dynamic_filter::FilterRule;
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

fn write_csv(path: &std::path::Path) {
    let mut s = "passenger_count,fare_amount,trip_distance\n".to_string();
    for i in 0..PASSENGERS.len() {
        s.push_str(&format!(
            "{},{},{}\n",
            PASSENGERS[i], FARES[i], DISTANCES[i]
        ));
    }
    std::fs::write(path, s).unwrap();
}

fn write_json_array(path: &std::path::Path) {
    let mut s = "[".to_string();
    for i in 0..PASSENGERS.len() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "{{\"passenger_count\":{},\"fare_amount\":{},\"trip_distance\":{}}}",
            PASSENGERS[i], FARES[i], DISTANCES[i]
        ));
    }
    s.push(']');
    std::fs::write(path, s).unwrap();
}

fn write_feather(path: &std::path::Path) {
    let f = File::create(path).unwrap();
    let mut writer = arrow_ipc::writer::FileWriter::try_new(&f, &taxi_schema()).unwrap();
    writer.write(&taxi_batch()).unwrap();
    writer.finish().unwrap();
}

fn write_parquet(path: &std::path::Path) {
    let f = File::create(path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(f, taxi_schema(), None).unwrap();
    writer.write(&taxi_batch()).unwrap();
    writer.close().unwrap();
}

fn write_avro(path: &std::path::Path) {
    let schema = apache_avro::Schema::parse_str(
        r#"{"type":"record","name":"taxi","fields":[
            {"name":"passenger_count","type":"long"},
            {"name":"fare_amount","type":"double"},
            {"name":"trip_distance","type":"double"}
        ]}"#,
    )
    .unwrap();
    let mut out = File::create(path).unwrap();
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
}

fn write_msgpack(path: &std::path::Path) {
    use rmpv::Value;
    let mut out = File::create(path).unwrap();
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
}

fn write_orc(path: &std::path::Path) {
    let f = File::create(path).unwrap();
    let mut writer = orc_rust::ArrowWriterBuilder::new(f, taxi_schema())
        .try_build()
        .unwrap();
    writer.write(&taxi_batch()).unwrap();
    writer.close().unwrap();
}

fn write_xlsx(path: &std::path::Path) {
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
    workbook.save(path).unwrap();
}

fn engine() -> MatrixEngine {
    MatrixEngine::new(1, 9, 0.01, 100.0)
}

/// slice(1, 2) must return rows 1..3: passenger_count [2, 0], fare [-5.0, 20.0].
/// Columns are looked up by name because arrow_json sorts fields alphabetically.
fn assert_slice_rows(file: &str) {
    let batch = engine().slice_rows_native(file, 1, 2).unwrap();
    assert_eq!(batch.num_rows(), 2, "{file}");
    let pc = batch
        .column_by_name("passenger_count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(pc.value(0), 2, "{file}");
    assert_eq!(pc.value(1), 0, "{file}");
}

#[test]
fn slice_rows_json() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.json");
    write_json_array(&path);
    assert_slice_rows(path.to_str().unwrap());
}

#[test]
fn slice_rows_feather() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.feather");
    write_feather(&path);
    assert_slice_rows(path.to_str().unwrap());
}

#[test]
fn slice_rows_parquet() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.parquet");
    write_parquet(&path);
    assert_slice_rows(path.to_str().unwrap());
}

#[test]
fn slice_rows_avro() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.avro");
    write_avro(&path);
    assert_slice_rows(path.to_str().unwrap());
}

#[test]
fn slice_rows_msgpack() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.msgpack");
    write_msgpack(&path);
    assert_slice_rows(path.to_str().unwrap());
}

#[test]
fn slice_rows_csv() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.csv");
    write_csv(&path);
    assert_slice_rows(path.to_str().unwrap());
}

#[test]
fn slice_rows_orc() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.orc");
    write_orc(&path);
    assert_slice_rows(path.to_str().unwrap());
}

#[test]
fn slice_rows_xlsx() {
    // XlsxHandler maps every cell to Utf8, so the column is a StringArray.
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.xlsx");
    write_xlsx(&path);
    let batch = engine()
        .slice_rows_native(path.to_str().unwrap(), 1, 2)
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    let pc = batch
        .column_by_name("passenger_count")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(pc.value(0), "2");
    assert_eq!(pc.value(1), "0");
}

#[test]
fn slice_cols_projection() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.json");
    write_json_array(&path);
    let batch = engine()
        .slice_cols_native(
            path.to_str().unwrap(),
            &["passenger_count".to_string()],
            0,
            6,
        )
        .unwrap();
    assert_eq!(batch.num_rows(), 6);
    assert_eq!(batch.num_columns(), 1);
    let pc = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(pc.value(4), 12);
}

/// filter previously errored on json/feather (slice_rows_native couldn't read
/// them); now it streams through the format registry.
#[test]
fn filter_json_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.json");
    write_json_array(&path);

    let rules = vec![FilterRule::parse("passenger_count > 3").unwrap()];
    let summary = engine()
        .filter_files_parallel_native(path.to_str().unwrap(), &rules, None, None)
        .unwrap();

    assert_eq!(summary.total_files, 1);
    assert_eq!(summary.clean_rows, 2);
    assert_eq!(summary.trash_rows, 4);
}

/// ORC now streams through a real orc-rust reader (previously routed through
/// the Parquet reader and would fail on any genuine ORC file).
#[test]
fn filter_orc_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.orc");
    write_orc(&path);

    let rules = vec![FilterRule::parse("passenger_count > 3").unwrap()];
    let summary = engine()
        .filter_files_parallel_native(path.to_str().unwrap(), &rules, None, None)
        .unwrap();

    assert_eq!(summary.total_files, 1);
    assert_eq!(summary.clean_rows, 2);
    assert_eq!(summary.trash_rows, 4);
}

/// Pack a directory mixing csv + json + parquet, then slice the container.
#[test]
fn pack_mixed_dir_then_slice_bazan() {
    let dir = tempdir().unwrap();
    let input_dir = dir.path().join("db");
    std::fs::create_dir_all(&input_dir).unwrap();
    write_csv(&input_dir.join("a.csv"));
    write_json_array(&input_dir.join("b.json"));
    write_parquet(&input_dir.join("c.parquet"));

    let bazan = dir.path().join("mixed.bazan");
    let (num_entries, _) = engine()
        .pack_directory_to_bazan(&input_dir, &bazan)
        .unwrap();
    assert_eq!(num_entries, 3);

    let batch = engine()
        .slice_rows_native(bazan.to_str().unwrap(), 0, 6)
        .unwrap();
    assert_eq!(batch.num_rows(), 6);
    let pc = batch
        .column_by_name("passenger_count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(pc.value(0), 1);
}

#[tokio::test]
async fn sql_on_json_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.json");
    write_json_array(&path);

    let sql = format!(
        "SELECT passenger_count FROM '{}' WHERE fare_amount < 0",
        path.display()
    );
    let result = engine().execute_sql(&sql).await.unwrap();
    assert_eq!(result.num_rows(), 1);
    let pc = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(pc.value(0), 2);
}

#[tokio::test]
async fn sql_on_mixed_dir() {
    let dir = tempdir().unwrap();
    let input_dir = dir.path().join("db");
    std::fs::create_dir_all(&input_dir).unwrap();
    write_csv(&input_dir.join("a.csv"));
    write_json_array(&input_dir.join("b.json"));

    let sql = format!(
        "SELECT passenger_count FROM '{}' WHERE passenger_count > 3",
        input_dir.display()
    );
    let result = engine().execute_sql(&sql).await.unwrap();
    assert_eq!(result.num_rows(), 4);
}

#[tokio::test]
async fn sql_on_orc_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.orc");
    write_orc(&path);

    let sql = format!(
        "SELECT passenger_count FROM '{}' WHERE fare_amount < 0",
        path.display()
    );
    let result = engine().execute_sql(&sql).await.unwrap();
    assert_eq!(result.num_rows(), 1);
    let pc = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(pc.value(0), 2);
}

#[tokio::test]
async fn sql_on_xlsx_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.xlsx");
    write_xlsx(&path);

    let sql = format!(
        "SELECT passenger_count FROM '{}' WHERE fare_amount < 0.0",
        path.display()
    );
    let result = engine().execute_sql(&sql).await.unwrap();
    assert_eq!(result.num_rows(), 1);
}

/// Pack a dir mixing csv (typed) + orc (typed) then slice the container.
#[test]
fn pack_orc_into_bazan() {
    let dir = tempdir().unwrap();
    let input_dir = dir.path().join("db");
    std::fs::create_dir_all(&input_dir).unwrap();
    write_csv(&input_dir.join("a.csv"));
    write_orc(&input_dir.join("b.orc"));

    let bazan = dir.path().join("orc.bazan");
    let (num_entries, _) = engine()
        .pack_directory_to_bazan(&input_dir, &bazan)
        .unwrap();
    assert_eq!(num_entries, 2);

    let batch = engine()
        .slice_rows_native(bazan.to_str().unwrap(), 0, 6)
        .unwrap();
    assert_eq!(batch.num_rows(), 6);
    let pc = batch
        .column_by_name("passenger_count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(pc.value(0), 1);
}

/// Pack a dir of xlsx files then slice the container (all-Utf8 columns).
#[test]
fn pack_xlsx_into_bazan() {
    let dir = tempdir().unwrap();
    let input_dir = dir.path().join("db");
    std::fs::create_dir_all(&input_dir).unwrap();
    write_xlsx(&input_dir.join("a.xlsx"));
    write_xlsx(&input_dir.join("b.xlsx"));

    let bazan = dir.path().join("xlsx.bazan");
    let (num_entries, _) = engine()
        .pack_directory_to_bazan(&input_dir, &bazan)
        .unwrap();
    assert_eq!(num_entries, 2);

    let batch = engine()
        .slice_rows_native(bazan.to_str().unwrap(), 1, 1)
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    let pc = batch
        .column_by_name("passenger_count")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(pc.value(0), "2");
}
