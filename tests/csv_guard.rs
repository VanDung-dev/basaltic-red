use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use basaltic_red::engine::csv_guard::sanitize_csv_batch;

fn batch_with(payloads: &[&str]) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("payload", DataType::Utf8, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(
                (0..payloads.len() as i64).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(payloads.to_vec())),
        ],
    )
    .unwrap()
}

#[test]
fn escapes_dangerous_cells_only() {
    let out = sanitize_csv_batch(&batch_with(&[
        "=cmd|' /C calc'!A0",
        "+2+2",
        "@SUM(A1:A2)",
        "-1+1",
        "-5.0",
        "plain",
    ]));
    let out_payloads: Vec<&str> = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.unwrap())
        .collect();

    assert_eq!(
        out_payloads,
        vec![
            "'=cmd|' /C calc'!A0",
            "'+2+2",
            "'@SUM(A1:A2)",
            "'-1+1",
            "-5.0",
            "plain",
        ]
    );
}

#[test]
fn leaves_numeric_columns_untouched() {
    let out = sanitize_csv_batch(&batch_with(&["=1+1", "x"]));
    let out_ids: Vec<i64> = out
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .map(|v| v.unwrap())
        .collect();
    assert_eq!(out_ids, vec![0, 1]);
}
