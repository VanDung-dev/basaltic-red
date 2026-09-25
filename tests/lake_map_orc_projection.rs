use std::fs::File;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use basaltic_red::engine::map::resolve_orc_range;
use basaltic_red::engine::MatrixEngine;
use tempfile::tempdir;

#[test]
fn mapped_orc_column_slice_preserves_row_range_order_and_repeats() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data.orc");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("left", DataType::Int64, false),
        Field::new("right", DataType::Int64, false),
    ]));
    let rows = 8usize;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
            Arc::new(Int64Array::from_iter_values(
                (100..100 + rows).map(|v| v as i64),
            )),
            Arc::new(Int64Array::from_iter_values(
                (200..200 + rows).map(|v| v as i64),
            )),
        ],
    )
    .unwrap();
    let file = File::create(&path).unwrap();
    let mut writer = orc_rust::ArrowWriterBuilder::new(file, schema)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    engine
        .create_lake_map_native(dir.path().to_str().unwrap(), false)
        .unwrap();
    let resolved = resolve_orc_range(&path, 2, 3).unwrap().unwrap();
    assert_eq!(resolved.offset, 2);

    let selected = [
        String::from("right"),
        String::from("left"),
        String::from("right"),
    ];
    let result = engine
        .slice_cols_native(path.to_str().unwrap(), &selected, 2, 3)
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["right", "left", "right"]
    );
    for (column, expected) in [
        (0, [202, 203, 204]),
        (1, [102, 103, 104]),
        (2, [202, 203, 204]),
    ] {
        let values = result
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &expected);
    }
}
