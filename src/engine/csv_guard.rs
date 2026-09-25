use arrow::array::{ArrayRef, LargeStringArray, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

/// CSV Injection guard (OWASP): spreadsheet formulas may start with `=`, `+`, `-`, or `@`.
/// When such a cell is written to a CSV that a user later opens in Excel/Sheets,
/// it is evaluated as a formula on the victim's machine. Neutralize by prefixing `'`.
/// Leading control characters are also escaped; numeric-looking negative values remain intact.
fn sanitize_cell(v: &str) -> String {
    let trimmed = v.trim_start_matches(char::is_whitespace);
    let first = trimmed.chars().next();
    let control_prefix = v
        .chars()
        .take_while(|ch| ch.is_whitespace())
        .any(|ch| matches!(ch, '\t' | '\r' | '\n'));
    let formula_prefix = match first {
        Some('=') | Some('+') | Some('@') | Some('＝') | Some('＋') | Some('＠') => true,
        Some('-') | Some('－') => trimmed.parse::<f64>().is_err(),
        _ => false,
    };
    let dangerous = control_prefix || formula_prefix;
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
