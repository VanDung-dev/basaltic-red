use std::fs;
use tempfile::tempdir;

use basaltic_red::engine::formats::handler_for;
use basaltic_red::engine::MatrixEngine;

fn taxi_csv() -> &'static str {
    "passenger_count,fare_amount,trip_distance\n\
     1,15.5,2.5\n\
     2,-5.0,0.0\n\
     0,20.0,3.1\n\
     5,100.0,10.0\n\
     12,50.0,1.2\n\
     1,0.0,5.0\n"
}

fn assert_taxi_stats(ext: &str, bytes: &[u8], expected_total: usize, expected_clean: usize) {
    let dir = tempdir().unwrap();
    let path = dir.path().join(format!("data.{ext}"));
    fs::write(&path, bytes).unwrap();

    let handler = handler_for(ext).unwrap();
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let (total, clean, trash) = handler
        .process_file(&engine, path.to_str().unwrap(), 1024)
        .unwrap();

    assert_eq!(total, expected_total);
    assert_eq!(clean, expected_clean);
    assert_eq!(trash, expected_total - expected_clean);
}

#[test]
fn test_registry_dispatch_filters_csv() {
    assert_taxi_stats("csv", taxi_csv().as_bytes(), 6, 2);
    assert!(handler_for("nope").is_none());
}

#[test]
fn test_delimited_helper_via_txt_handler() {
    let txt = taxi_csv().replace(',', ";");
    assert_taxi_stats("txt", txt.as_bytes(), 6, 2);
}
