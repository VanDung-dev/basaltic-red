use arrow::array::{ArrayRef, LargeStringArray, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

/// CSV Injection guard (OWASP): spreadsheet formulas start with `=`, `+`, `-`, `@`.
/// When such a cell is written to a CSV that a user later opens in Excel/Sheets,
/// it is evaluated as a formula on the victim's machine. Neutralize by prefixing `'`.
/// Numeric-looking negative values (`-5.0`) are left untouched.
fn sanitize_cell(v: &str) -> String {
    let first = v.chars().next();
    let dangerous = match first {
        Some('=') | Some('+') | Some('@') => true,
        Some('-') => v.parse::<f64>().is_err(),
        _ => false,
    };
    if dangerous {
        format!("'{}", v)
    } else {
        v.to_string()
    }
}

/// Return a copy of `batch` with dangerous string cells escaped for CSV output.
/// Only Utf8/LargeUtf8 columns are touched; numeric columns pass through unchanged.
pub fn sanitize_csv_batch(batch: &RecordBatch) -> RecordBatch {
    let arrays: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(array, field)| match field.data_type() {
            DataType::Utf8 => {
                let src = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 array");
                let escaped: StringArray = src.iter().map(|v| v.map(sanitize_cell)).collect();
                Arc::new(escaped) as ArrayRef
            }
            DataType::LargeUtf8 => {
                let src = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("LargeUtf8 array");
                let escaped: LargeStringArray = src.iter().map(|v| v.map(sanitize_cell)).collect();
                Arc::new(escaped) as ArrayRef
            }
            _ => array.clone(),
        })
        .collect();

    RecordBatch::try_new(batch.schema(), arrays).expect("sanitized batch schema")
}
