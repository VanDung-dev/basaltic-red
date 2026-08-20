use arrow::array::{Array, Float64Array, Int64Array, ListArray, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use basaltic_red::engine::dynamic_filter::FilterRule;
use basaltic_red::engine::MatrixEngine;

#[test]
fn test_dynamic_filter_under_64_rules_bitmask() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_arr = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
    let score_arr = Arc::new(Float64Array::from(vec![90.0, 45.0, 80.0, 10.0]));
    let name_arr = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]));

    let batch = RecordBatch::try_new(schema, vec![id_arr, score_arr, name_arr]).unwrap();

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    // Rule 0: score >= 50.0 (fails Bob, David -> bit 0)
    // Rule 1: id > 1 (fails Alice -> bit 1)
    let rules = vec![
        FilterRule::parse("score >= 50.0").unwrap(),
        FilterRule::parse("id > 1").unwrap(),
    ];

    let (clean, trash) = engine.filter_batch_dynamic(&batch, &rules).unwrap();

    // Clean: row 3 (Charlie: id=3, score=80.0) -> satisfies both
    assert_eq!(clean.num_rows(), 1);
    let clean_id = clean
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(clean_id.value(0), 3);

    // Trash: row 0 (Alice -> fails rule 1: code 2 = 1 << 1),
    //        row 1 (Bob -> fails rule 0: code 1 = 1 << 0),
    //        row 3 (David -> fails rule 0: code 1 = 1 << 0)
    assert_eq!(trash.num_rows(), 3);
    let err_col = trash
        .column_by_name("audit_error_code")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    assert_eq!(err_col.value(0), 2); // Alice
    assert_eq!(err_col.value(1), 1); // Bob
    assert_eq!(err_col.value(2), 1); // David
}

#[test]
fn test_dynamic_filter_over_64_rules_no_collision() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));

    // Row 0: id=1, val=10  (will fail Rule 0: val >= 100, but pass Rule 64: id >= 1)
    // Row 1: id=0, val=200 (will pass Rule 0: val >= 100, but fail Rule 64: id >= 1)
    // Row 2: id=0, val=10  (will fail BOTH Rule 0 AND Rule 64)
    // Row 3: id=1, val=200 (passes everything)
    let id_arr = Arc::new(Int64Array::from(vec![1, 0, 0, 1]));
    let val_arr = Arc::new(Int64Array::from(vec![10, 200, 10, 200]));

    let batch = RecordBatch::try_new(schema, vec![id_arr, val_arr]).unwrap();

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);

    // Generate 100 rules:
    // Rule 0: "val >= 100"
    // Rules 1..63: "val >= 0" (all rows pass)
    // Rule 64: "id >= 1" (Row 1 and Row 2 fail)
    // Rules 65..99: "val >= 0" (all rows pass)
    let mut rules = Vec::new();
    rules.push(FilterRule::parse("val >= 100").unwrap()); // Rule 0
    for _ in 1..64 {
        rules.push(FilterRule::parse("val >= 0").unwrap());
    }
    rules.push(FilterRule::parse("id >= 1").unwrap()); // Rule 64
    for _ in 65..100 {
        rules.push(FilterRule::parse("val >= 0").unwrap());
    }

    assert_eq!(rules.len(), 100);

    let (clean, trash) = engine.filter_batch_dynamic(&batch, &rules).unwrap();

    // Clean: only Row 3 (id=1, val=200)
    assert_eq!(clean.num_rows(), 1);

    // Trash: Row 0, Row 1, Row 2
    assert_eq!(trash.num_rows(), 3);

    // Check audit_violated_rules column
    let violated_col = trash.column_by_name("audit_violated_rules").unwrap();
    let list_arr = violated_col.as_any().downcast_ref::<ListArray>().unwrap();

    // Helper to get u32 list for row in trash
    let get_violations = |row: usize| -> Vec<u32> {
        let val_arr = list_arr.value(row);
        let u32_arr = val_arr.as_any().downcast_ref::<UInt32Array>().unwrap();
        (0..u32_arr.len()).map(|i| u32_arr.value(i)).collect()
    };

    // Row 0 (id=1, val=10): fails ONLY rule 0
    assert_eq!(get_violations(0), vec![0]);

    // Row 1 (id=0, val=200): fails ONLY rule 64 (demonstrates NO collision with rule 0!)
    assert_eq!(get_violations(1), vec![64]);

    // Row 2 (id=0, val=10): fails rule 0 AND rule 64
    assert_eq!(get_violations(2), vec![0, 64]);
}

#[test]
fn test_dynamic_filter_massive_rules_scaling() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("f_num", DataType::Float64, false),
    ]));

    let num_arr = Arc::new(Float64Array::from(vec![500.0, -1.0]));
    let batch = RecordBatch::try_new(schema, vec![num_arr]).unwrap();

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);

    // Create 500 rules, where rule 250 and rule 499 fail for row 1
    let mut rules = Vec::new();
    for i in 0..500 {
        if i == 250 || i == 499 {
            rules.push(FilterRule::parse("f_num >= 0.0").unwrap());
        } else {
            rules.push(FilterRule::parse("f_num >= -100.0").unwrap());
        }
    }

    let (clean, trash) = engine.filter_batch_dynamic(&batch, &rules).unwrap();
    assert_eq!(clean.num_rows(), 1);
    assert_eq!(trash.num_rows(), 1);

    let violated_col = trash.column_by_name("audit_violated_rules").unwrap();
    let list_arr = violated_col.as_any().downcast_ref::<ListArray>().unwrap();
    let val_arr = list_arr.value(0);
    let u32_arr = val_arr.as_any().downcast_ref::<UInt32Array>().unwrap();
    let violations: Vec<u32> = (0..u32_arr.len()).map(|i| u32_arr.value(i)).collect();

    assert_eq!(violations, vec![250, 499]);
}

#[test]
fn test_filter_batch_on_parquet_mini_with_70_rules() {
    use basaltic_red::engine::formats::handler_for;
    let path = "data/test_mini.parquet";
    if !std::path::Path::new(path).exists() {
        return;
    }
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let mut rules = vec![
        FilterRule::parse("passenger_count >= 1").unwrap(),
        FilterRule::parse("trip_distance > 0.0").unwrap(),
        FilterRule::parse("fare_amount > 0.0").unwrap(),
        FilterRule::parse("total_amount > 0.0").unwrap(),
    ];
    for i in 1..=66 {
        rules.push(FilterRule::parse(&format!("fare_amount >= {}.0", i)).unwrap());
    }
    let handler = handler_for("parquet").unwrap();
    let source = handler.open(path, 65536).unwrap();
    for batch_res in source.batches {
        let batch = batch_res.unwrap();
        let (clean, trash) = engine.filter_batch_dynamic(&batch, &rules).unwrap();
        println!("Clean rows: {}, Trash rows: {}", clean.num_rows(), trash.num_rows());
    }
}
