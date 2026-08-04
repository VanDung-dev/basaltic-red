use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use apache_avro::Reader as AvroReader;
use apache_avro::types::Value;
use arrow_array::builder::*;
use arrow_array::*;

use arrow_schema::{DataType, Field, Schema};
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use super::FormatHandler;

/// Apache Avro Streaming Reader
pub struct AvroHandler;

impl FormatHandler for AvroHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        let file = BufReader::new(File::open(file_path)?);
        let reader = AvroReader::new(file)?;
        let avro_schema = reader.writer_schema().clone();

        // Extract fields from Avro Schema
        let mut fields = Vec::new();
        if let apache_avro::Schema::Record(ref record) = avro_schema {
            for f in &record.fields {
                let dt = match &f.schema {
                    apache_avro::Schema::Long => DataType::Int64,
                    apache_avro::Schema::Int => DataType::Int32,
                    apache_avro::Schema::Double => DataType::Float64,
                    apache_avro::Schema::Boolean => DataType::Boolean,
                    _ => DataType::Utf8,
                };
                fields.push(Field::new(&f.name, dt, true));
            }
        }
        let arrow_schema = Arc::new(Schema::new(fields));

        let mut total_rows = 0;
        let mut total_clean = 0;
        let mut total_trash = 0;

        let mut value_batch: Vec<Value> = Vec::with_capacity(batch_size);

        for value_res in reader {
            let value = value_res?;
            value_batch.push(value);

            if value_batch.len() >= batch_size {
                let batch = avro_values_to_record_batch(&value_batch, &arrow_schema)?;
                let n = batch.num_rows();
                total_rows += n;
                let (c_b, t_b) = engine.filter_batch_native(&batch, n);
                total_clean += c_b.num_rows();
                total_trash += t_b.num_rows();
                value_batch.clear();
            }
        }

        if !value_batch.is_empty() {
            let batch = avro_values_to_record_batch(&value_batch, &arrow_schema)?;
            let n = batch.num_rows();
            total_rows += n;
            let (c_b, t_b) = engine.filter_batch_native(&batch, n);
            total_clean += c_b.num_rows();
            total_trash += t_b.num_rows();
        }

        Ok((total_rows, total_clean, total_trash))
    }
}

fn avro_values_to_record_batch(values: &[Value], schema: &Arc<Schema>) -> Result<RecordBatch, BazanError> {
    let n = values.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (col_idx, field) in schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(n);
                for v in values {
                    if let Value::Record(ref fields) = v {
                        if let Some((_, val)) = fields.get(col_idx) {
                            if let Value::Long(num) = val {
                                builder.append_value(*num);
                                continue;
                            }
                        }
                    }
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(n);
                for v in values {
                    if let Value::Record(ref fields) = v {
                        if let Some((_, val)) = fields.get(col_idx) {
                            if let Value::Int(num) = val {
                                builder.append_value(*num);
                                continue;
                            }
                        }
                    }
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(n);
                for v in values {
                    if let Value::Record(ref fields) = v {
                        if let Some((_, val)) = fields.get(col_idx) {
                            if let Value::Double(num) = val {
                                builder.append_value(*num);
                                continue;
                            }
                        }
                    }
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(n);
                for v in values {
                    if let Value::Record(ref fields) = v {
                        if let Some((_, val)) = fields.get(col_idx) {
                            if let Value::Boolean(b) = val {
                                builder.append_value(*b);
                                continue;
                            }
                        }
                    }
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(n, n * 20);
                for v in values {
                    if let Value::Record(ref fields) = v {
                        if let Some((_, val)) = fields.get(col_idx) {
                            match val {
                                Value::String(s) => builder.append_value(s),
                                Value::Union(_, box_val) => {
                                    if let Value::String(s) = &**box_val {
                                        builder.append_value(s);
                                    } else {
                                        builder.append_null();
                                    }
                                }
                                _ => builder.append_null(),
                            }
                            continue;
                        }
                    }
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
