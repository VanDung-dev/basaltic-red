use arrow_array::builder::*;
use arrow_array::*;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use super::{clamp_batch_size, FormatHandler};
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use arrow_schema::{DataType, Field, Schema};

/// MessagePack (.msgpack) Binary JSON Reader
pub struct MsgpackHandler;

impl FormatHandler for MsgpackHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        let batch_size = clamp_batch_size(batch_size);
        let file = BufReader::new(File::open(file_path)?);
        let mut read = file;

        let mut total_rows = 0;
        let mut total_clean = 0;
        let mut total_trash = 0;

        let mut value_batch: Vec<rmpv::Value> = Vec::with_capacity(batch_size);
        let mut cached_schema: Option<Arc<Schema>> = None;

        while let Ok(val) = rmpv::decode::read_value(&mut read) {
            if cached_schema.is_none() {
                if let rmpv::Value::Map(ref entries) = val {
                    let mut fields = Vec::new();
                    for (k, v) in entries {
                        let key_str = k.as_str().unwrap_or("col").to_string();
                        let dt = match v {
                            rmpv::Value::Integer(_) => DataType::Int64,
                            rmpv::Value::F32(_) | rmpv::Value::F64(_) => DataType::Float64,
                            rmpv::Value::Boolean(_) => DataType::Boolean,
                            _ => DataType::Utf8,
                        };
                        fields.push(Field::new(key_str, dt, true));
                    }
                    cached_schema = Some(Arc::new(Schema::new(fields)));
                }
            }

            value_batch.push(val);

            if value_batch.len() >= batch_size {
                if let Some(ref schema) = cached_schema {
                    let batch = msgpack_values_to_record_batch(&value_batch, schema)?;
                    let n = batch.num_rows();
                    total_rows += n;
                    let (c_b, t_b) = engine.filter_batch_native(&batch, n);
                    total_clean += c_b.num_rows();
                    total_trash += t_b.num_rows();
                }
                value_batch.clear();
            }
        }

        if !value_batch.is_empty() {
            if let Some(ref schema) = cached_schema {
                let batch = msgpack_values_to_record_batch(&value_batch, schema)?;
                let n = batch.num_rows();
                total_rows += n;
                let (c_b, t_b) = engine.filter_batch_native(&batch, n);
                total_clean += c_b.num_rows();
                total_trash += t_b.num_rows();
            }
        }

        Ok((total_rows, total_clean, total_trash))
    }
}

fn msgpack_values_to_record_batch(
    values: &[rmpv::Value],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, BazanError> {
    let n = values.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let field_name = field.name().as_str();

        match field.data_type() {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(n);
                for v in values {
                    if let rmpv::Value::Map(ref entries) = v {
                        let mut found = false;
                        for (k, val) in entries {
                            if k.as_str() == Some(field_name) {
                                if let Some(num) = val.as_i64() {
                                    builder.append_value(num);
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if !found {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(n);
                for v in values {
                    if let rmpv::Value::Map(ref entries) = v {
                        let mut found = false;
                        for (k, val) in entries {
                            if k.as_str() == Some(field_name) {
                                if let Some(num) = val.as_f64() {
                                    builder.append_value(num);
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if !found {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(n);
                for v in values {
                    if let rmpv::Value::Map(ref entries) = v {
                        let mut found = false;
                        for (k, val) in entries {
                            if k.as_str() == Some(field_name) {
                                if let rmpv::Value::Boolean(b) = val {
                                    builder.append_value(*b);
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if !found {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(n, n * 20);
                for v in values {
                    if let rmpv::Value::Map(ref entries) = v {
                        let mut found = false;
                        for (k, val) in entries {
                            if k.as_str() == Some(field_name) {
                                if let Some(s) = val.as_str() {
                                    builder.append_value(s);
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if !found {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
