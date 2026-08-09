use arrow_array::builder::*;
use arrow_array::*;
use calamine::{open_workbook, Data, Reader, Xlsx};
use std::sync::Arc;

use super::{clamp_batch_size, FormatHandler, OpenedSource, RowChunker};
use crate::error::BazanError;
use arrow_schema::{DataType, Field, Schema};

/// Excel (.xlsx) Streaming Reader via Calamine
pub struct XlsxHandler;

impl FormatHandler for XlsxHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let batch_size = clamp_batch_size(batch_size);
        let mut workbook: Xlsx<_> = open_workbook(file_path)?;

        let range = match workbook.worksheet_range_at(0) {
            Some(Ok(r)) => r,
            _ => {
                return Err(BazanError::Message(format!(
                    "No sheet found in Excel workbook: {}",
                    file_path
                )))
            }
        };

        // First row as Header (scoped so the borrow on `range` ends before
        // `into_rows()` consumes it below).
        let header = match range.rows().next() {
            Some(h) => h,
            None => {
                return Ok(OpenedSource {
                    schema: Arc::new(Schema::empty()),
                    batches: Box::new(std::iter::empty()),
                })
            }
        };

        let col_names: Vec<String> = header
            .iter()
            .map(|cell| match cell {
                Data::String(s) => s.to_string(),
                other => other.to_string(),
            })
            .collect();

        let fields: Vec<Field> = col_names
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Calamine holds the whole sheet in memory anyway; convert each row
        // lazily via an owned iterator and let RowChunker buffer batch_size at a
        // time (no whole-sheet Vec<Vec<String>> duplicate).
        let data_rows = XlsxRows::new(range, 1); // skip the header row

        let chunker = RowChunker::new(
            data_rows.map(Ok),
            batch_size,
            schema.clone(),
            string_rows_to_record_batch,
        );

        Ok(OpenedSource {
            schema,
            batches: Box::new(chunker),
        })
    }
}

/// Owned lazy iterator over a calamine `Range<Data>`: yields one `Vec<String>`
/// row at a time by random access into the sheet, so only the current row's
/// strings are allocated instead of the whole sheet's.
struct XlsxRows {
    range: calamine::Range<Data>,
    row: usize,
    width: usize,
    total_rows: usize,
}

impl XlsxRows {
    fn new(range: calamine::Range<Data>, start_row: usize) -> Self {
        let width = range.width();
        let total_rows = range.height();
        XlsxRows {
            range,
            row: start_row,
            width,
            total_rows,
        }
    }
}

impl Iterator for XlsxRows {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.total_rows {
            return None;
        }
        let cells = (0..self.width)
            .map(|col| {
                self.range
                    .get((self.row, col))
                    .map(|cell| match cell {
                        Data::String(s) => s.clone(),
                        Data::Int(i) => i.to_string(),
                        Data::Float(f) => f.to_string(),
                        Data::Bool(b) => b.to_string(),
                        Data::Empty => String::new(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default()
            })
            .collect();
        self.row += 1;
        Some(cells)
    }
}

fn string_rows_to_record_batch(
    rows: &[Vec<String>],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, BazanError> {
    let n = rows.len();
    let num_cols = schema.fields().len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let mut builder = StringBuilder::with_capacity(n, n * 15);
        for row in rows {
            if let Some(val) = row.get(col_idx) {
                if val.is_empty() {
                    builder.append_null();
                } else {
                    builder.append_value(val);
                }
            } else {
                builder.append_null();
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
