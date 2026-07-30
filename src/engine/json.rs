use pyo3::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use arrow::datatypes::Schema;
use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// JSON Streaming In-Memory Reader with Schema Inference
    pub fn process_json_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path = file_path.to_string();

        let stats = py.detach(|| -> Result<(usize, usize, usize), anyhow::Error> {
            let file = File::open(&path)?;
            let mut buf_reader = BufReader::new(file);

            let schema_res = arrow_json::reader::infer_json_schema(&mut buf_reader, Some(100));

            let file_for_reader = File::open(&path)?;
            let buf_reader_2 = BufReader::new(file_for_reader);

            let mut total_rows = 0;
            let mut total_clean = 0;
            let mut total_trash = 0;

            if let Ok((schema, _)) = schema_res {
                let reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
                    .with_batch_size(batch_size)
                    .build(buf_reader_2)?;

                return self.process_reader(reader);
            } else {
                // Streaming JSON Record Scanner for large pretty-printed/nested JSON files
                use std::io::BufRead;
                let mut reader = std::io::BufReader::with_capacity(1024 * 1024, File::open(&path)?);
                let mut chunk_buf = String::with_capacity(512 * 1024);
                let mut line = String::new();
                let mut cached_schema: Option<Arc<Schema>> = None;
                let mut line_count = 0;

                while reader.read_line(&mut line)? > 0 {
                    let trimmed = line.trim();
                    if trimmed.starts_with('{') && trimmed.ends_with('}') && trimmed.len() > 30 {
                        let clean_line = if trimmed.ends_with(',') { &trimmed[..trimmed.len() - 1] } else { trimmed };
                        chunk_buf.push_str(clean_line);
                        chunk_buf.push('\n');
                        line_count += 1;

                        if line_count >= batch_size {
                            if cached_schema.is_none() {
                                let mut c_infer = std::io::Cursor::new(chunk_buf.as_bytes());
                                if let Ok((s, _)) = arrow_json::reader::infer_json_schema(&mut c_infer, Some(100)) {
                                    cached_schema = Some(Arc::new(s));
                                }
                            }

                            if let Some(ref schema) = cached_schema {
                                let c_read = std::io::Cursor::new(chunk_buf.as_bytes());
                                if let Ok(b_reader) = arrow_json::ReaderBuilder::new(schema.clone()).with_batch_size(batch_size).build(c_read) {
                                    for batch_result in b_reader {
                                        if let Ok(batch) = batch_result {
                                            let batch_rows = batch.num_rows();
                                            total_rows += batch_rows;
                                            let (clean_b, trash_b) = self.filter_batch_native(&batch, batch_rows);
                                            total_clean += clean_b.num_rows();
                                            total_trash += trash_b.num_rows();
                                        }
                                    }
                                }
                            }
                            chunk_buf.clear();
                            line_count = 0;
                        }
                    }
                    line.clear();
                }

                if !chunk_buf.is_empty() {
                    if cached_schema.is_none() {
                        let mut c_infer = std::io::Cursor::new(chunk_buf.as_bytes());
                        if let Ok((s, _)) = arrow_json::reader::infer_json_schema(&mut c_infer, Some(100)) {
                            cached_schema = Some(Arc::new(s));
                        }
                    }

                    if let Some(ref schema) = cached_schema {
                        let c_read = std::io::Cursor::new(chunk_buf.as_bytes());
                        if let Ok(b_reader) = arrow_json::ReaderBuilder::new(schema.clone()).with_batch_size(batch_size).build(c_read) {
                            for batch_result in b_reader {
                                if let Ok(batch) = batch_result {
                                    let batch_rows = batch.num_rows();
                                    total_rows += batch_rows;
                                    let (clean_b, trash_b) = self.filter_batch_native(&batch, batch_rows);
                                    total_clean += clean_b.num_rows();
                                    total_trash += trash_b.num_rows();
                                }
                            }
                        }
                    }
                }
            }

            Ok((total_rows, total_clean, total_trash))
        });

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }
}
