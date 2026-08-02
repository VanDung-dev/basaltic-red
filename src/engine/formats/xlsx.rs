use pyo3::prelude::*;
use std::sync::Arc;
use calamine::{open_workbook, Data, Reader, Xlsx};
use arrow_array::builder::*;
use arrow_array::*;

use arrow_schema::{DataType, Field, Schema};
use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// Excel (.xlsx) Streaming Reader via Calamine
    pub fn process_xlsx_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path = file_path.to_string();

        let stats = py.detach(|| -> Result<(usize, usize, usize), anyhow::Error> {
            let mut workbook: Xlsx<_> = open_workbook(&path)?;

            let range = match workbook.worksheet_range_at(0) {
                Some(Ok(r)) => r,
                _ => anyhow::bail!("No sheet found in Excel workbook: {}", path),
            };

            let mut rows_iter = range.rows();

            // First row as Header
            let header = match rows_iter.next() {
                Some(h) => h,
                None => return Ok((0, 0, 0)),
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

            let mut total_rows = 0;
            let mut total_clean = 0;
            let mut total_trash = 0;

            let mut row_batch: Vec<Vec<String>> = Vec::with_capacity(batch_size);

            for row_cells in rows_iter {
                let string_row: Vec<String> = row_cells
                    .iter()
                    .map(|cell| match cell {
                        Data::String(s) => s.to_string(),
                        Data::Int(i) => i.to_string(),
                        Data::Float(f) => f.to_string(),
                        Data::Bool(b) => b.to_string(),
                        Data::Empty => String::new(),
                        other => other.to_string(),
                    })
                    .collect();


                row_batch.push(string_row);

                if row_batch.len() >= batch_size {
                    let batch = string_rows_to_record_batch(&row_batch, &schema)?;
                    let n = batch.num_rows();
                    total_rows += n;
                    let (c_b, t_b) = self.filter_batch_native(&batch, n);
                    total_clean += c_b.num_rows();
                    total_trash += t_b.num_rows();
                    row_batch.clear();
                }
            }

            if !row_batch.is_empty() {
                let batch = string_rows_to_record_batch(&row_batch, &schema)?;
                let n = batch.num_rows();
                total_rows += n;
                let (c_b, t_b) = self.filter_batch_native(&batch, n);
                total_clean += c_b.num_rows();
                total_trash += t_b.num_rows();
            }

            Ok((total_rows, total_clean, total_trash))
        });

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }
}

fn string_rows_to_record_batch(rows: &[Vec<String>], schema: &Arc<Schema>) -> Result<RecordBatch, anyhow::Error> {
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
