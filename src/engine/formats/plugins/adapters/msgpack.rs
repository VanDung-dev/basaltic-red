use arrow_array::builder::*;
use arrow_array::*;
use arrow_schema::{DataType, Field, Schema};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use crate::engine::formats::plugins::base_templates::RowChunker;
use crate::engine::formats::{clamp_batch_size, FormatHandler, OpenedSource};
use crate::error::BazanError;

/// MessagePack (.msgpack) Binary JSON Reader (Tier 3 Adapter)
#[derive(Debug, Clone, Copy, Default)]
pub struct MsgpackHandler;

impl FormatHandler for MsgpackHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let batch_size = clamp_batch_size(batch_size);
        let file = BufReader::new(File::open(file_path)?);
        let mut read = file;

        let mut values = std::iter::from_fn(move || rmpv::decode::read_value(&mut read).ok());

        // Schema is inferred from the first Map row; rows before it are dropped
        let mut schema: Option<Arc<Schema>> = None;
        let mut first: Option<rmpv::Value> = None;
        for val in values.by_ref() {
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
                schema = Some(Arc::new(Schema::new(fields)));
                first = Some(val);
                break;
            }
        }

        let schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
        let rows = first.into_iter().chain(values).map(Ok);
        let chunker = RowChunker::new(
            rows,
            batch_size,
            schema.clone(),
            msgpack_values_to_record_batch,
        );

        Ok(OpenedSource {
            schema,
            batches: Box::new(chunker),
        })
    }
}

fn msgpack_values_to_record_batch(
    values: &[rmpv::Value],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, BazanError> {
    let n = values.len();
    let num_cols = schema.fields().len();

    let mut col_index: HashMap<&str, usize> = HashMap::with_capacity(num_cols);
    for (i, field) in schema.fields().iter().enumerate() {
        col_index.insert(field.name(), i);
    }

    let mut cells: Vec<Vec<Option<&rmpv::Value>>> = vec![vec![None; n]; num_cols];
    for (row_i, val) in values.iter().enumerate() {
        if let rmpv::Value::Map(entries) = val {
            for (k, v) in entries {
                if let Some(key) = k.as_str() {
                    if let Some(&ci) = col_index.get(key) {
                        cells[ci][row_i] = Some(v);
                    }
                }
            }
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for (field, col) in schema.fields().iter().zip(cells) {
        match field.data_type() {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(n);
                for c in &col {
                    match c.and_then(|v| v.as_i64()) {
                        Some(num) => builder.append_value(num),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(n);
                for c in &col {
                    match c.and_then(|v| v.as_f64()) {
                        Some(num) => builder.append_value(num),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(n);
                for c in &col {
                    match c.and_then(|v| match v {
                        rmpv::Value::Boolean(b) => Some(*b),
                        _ => None,
                    }) {
                        Some(b) => builder.append_value(b),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(n, n * 20);
                for c in &col {
                    match c.and_then(|v| v.as_str()) {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
