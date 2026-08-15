use apache_avro::types::Value;
use apache_avro::Reader as AvroReader;
use arrow_array::builder::*;
use arrow_array::*;
use arrow_schema::{DataType, Field, Schema};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use crate::engine::formats::plugins::base_templates::RowChunker;
use crate::engine::formats::{clamp_batch_size, FormatHandler, OpenedSource};
use crate::error::BazanError;

/// Apache Avro Streaming Reader (Tier 3 Adapter)
#[derive(Debug, Clone, Copy, Default)]
pub struct AvroHandler;

impl FormatHandler for AvroHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let batch_size = clamp_batch_size(batch_size);
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

        let rows = reader.map(|r| r.map_err(BazanError::from));
        let chunker = RowChunker::new(
            rows,
            batch_size,
            arrow_schema.clone(),
            avro_values_to_record_batch,
        );

        Ok(OpenedSource {
            schema: arrow_schema,
            batches: Box::new(chunker),
        })
    }
}

fn avro_values_to_record_batch(
    values: &[Value],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, BazanError> {
    let n = values.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (col_idx, field) in schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(n);
                for v in values {
                    if let Some((_, Value::Long(num))) = match v {
                        Value::Record(fields) => fields.get(col_idx),
                        _ => None,
                    } {
                        builder.append_value(*num);
                        continue;
                    }
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(n);
                for v in values {
                    if let Some((_, Value::Int(num))) = match v {
                        Value::Record(fields) => fields.get(col_idx),
                        _ => None,
                    } {
                        builder.append_value(*num);
                        continue;
                    }
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(n);
                for v in values {
                    if let Some((_, Value::Double(num))) = match v {
                        Value::Record(fields) => fields.get(col_idx),
                        _ => None,
                    } {
                        builder.append_value(*num);
                        continue;
                    }
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(n);
                for v in values {
                    if let Some((_, Value::Boolean(b))) = match v {
                        Value::Record(fields) => fields.get(col_idx),
                        _ => None,
                    } {
                        builder.append_value(*b);
                        continue;
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
