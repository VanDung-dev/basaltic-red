use std::fs;

use arrow::array::{Array, StringArray};
use basaltic_red::engine::formats::{read_delimited_range, DEFAULT_MAX_BATCH_SIZE};
use basaltic_red::engine::map::resolve_delimited_range;
use basaltic_red::engine::MatrixEngine;
use tempfile::tempdir;

fn string_values(batch: &arrow::array::RecordBatch, column: usize) -> Vec<Option<String>> {
    let values = batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..values.len())
        .map(|row| (!values.is_null(row)).then(|| values.value(row).to_string()))
        .collect()
}

#[test]
fn mapped_delimited_column_slices_project_in_requested_order_with_repeats() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);

    for (extension, delimiter) in [("csv", b','), ("psv", b'|'), ("txt", b';'), ("tsv", b'\t')] {
        let dir = tempdir().unwrap();
        let path = dir.path().join(format!("data.{extension}"));
        let delimiter = delimiter as char;
        let contents = if extension == "tsv" {
            format!(
                "id{delimiter}left{delimiter}right{delimiter}optional{delimiter}unused\n\
                 0{delimiter}l0{delimiter}r0{delimiter}\\N{delimiter}x\n\
                 1{delimiter}l1{delimiter}r1\n\
                 2{delimiter}l2{delimiter}r2{delimiter}ok{delimiter}z\n"
            )
        } else {
            format!(
                "id{delimiter}left{delimiter}right{delimiter}optional{delimiter}unused\n\
                 0{delimiter}l0{delimiter}r0{delimiter}\\N{delimiter}x\n\
                 1{delimiter}l1{delimiter}r1{delimiter}\\N{delimiter}y\n\
                 2{delimiter}l2{delimiter}r2{delimiter}ok{delimiter}z\n"
            )
        };
        fs::write(&path, contents).unwrap();

        engine
            .create_lake_map_native(dir.path().to_str().unwrap(), false)
            .unwrap();
        let checkpoint = resolve_delimited_range(&path, 0, 3, delimiter as u8)
            .unwrap()
            .unwrap();

        let selected = [
            String::from("right"),
            String::from("optional"),
            String::from("left"),
            String::from("right"),
        ];
        let batch = engine
            .slice_cols_native(path.to_str().unwrap(), &selected, 0, 3)
            .unwrap();
        let unprojected = read_delimited_range(
            path.to_str().unwrap(),
            checkpoint.byte_offset,
            checkpoint.offset,
            3,
            DEFAULT_MAX_BATCH_SIZE,
            delimiter as u8,
            extension == "tsv",
        )
        .unwrap();

        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["right", "optional", "left", "right"],
            "{extension} column order and repeats"
        );
        assert_eq!(
            string_values(&batch, 0),
            vec![Some("r0".into()), Some("r1".into()), Some("r2".into())],
            "{extension} projected right values"
        );
        assert_eq!(
            string_values(&batch, 1),
            string_values(&unprojected, 3),
            "{extension} TSV null/truncated-row behavior"
        );
        assert_eq!(
            string_values(&batch, 2),
            vec![Some("l0".into()), Some("l1".into()), Some("l2".into())],
            "{extension} projected left values"
        );
        assert_eq!(
            string_values(&batch, 3),
            vec![Some("r0".into()), Some("r1".into()), Some("r2".into())],
            "{extension} repeated projected values"
        );
    }
}
