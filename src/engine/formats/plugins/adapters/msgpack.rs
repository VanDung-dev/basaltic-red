use arrow_array::builder::*;
use arrow_array::*;
use arrow_schema::{DataType, Field, Schema};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use crate::engine::formats::plugins::base_templates::RowChunker;
use crate::engine::formats::{
    clamp_batch_size, read_range_from_source, FormatHandler, OpenedSource,
};
use crate::error::BazanError;

#[derive(Debug, Clone, Copy)]
pub(crate) struct MsgpackBlockLocation {
    pub first_row: usize,
    pub row_count: usize,
    pub first_byte: u64,
    pub total_byte_size: u64,
}

struct MsgpackValues<R> {
    reader: R,
    done: bool,
}

impl<R> MsgpackValues<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            done: false,
        }
    }
}

impl<R: BufRead> Iterator for MsgpackValues<R> {
    type Item = Result<rmpv::Value, BazanError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match self.reader.fill_buf() {
            Ok([]) => {
                self.done = true;
                None
            }
            Ok(_) => {
                let value = rmpv::decode::read_value(&mut self.reader).map_err(|error| {
                    BazanError::Message(format!("MessagePack decode error: {error}"))
                });
                if value.is_err() {
                    self.done = true;
                }
                Some(value)
            }
            Err(error) => {
                self.done = true;
                Some(Err(BazanError::Message(format!(
                    "MessagePack read error: {error}"
                ))))
            }
        }
    }
}

fn msgpack_schema_from_map(entries: &[(rmpv::Value, rmpv::Value)]) -> Arc<Schema> {
    let fields: Vec<Field> = entries
        .iter()
        .map(|(key, value)| {
            let key_str = key.as_str().unwrap_or("col").to_string();
            let data_type = match value {
                rmpv::Value::Integer(_) => DataType::Int64,
                rmpv::Value::F32(_) | rmpv::Value::F64(_) => DataType::Float64,
                rmpv::Value::Boolean(_) => DataType::Boolean,
                _ => DataType::Utf8,
            };
            Field::new(key_str, data_type, true)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

fn infer_msgpack_schema(file_path: &Path) -> Result<Arc<Schema>, BazanError> {
    for value in MsgpackValues::new(BufReader::new(File::open(file_path)?)) {
        match value? {
            rmpv::Value::Map(entries) => return Ok(msgpack_schema_from_map(&entries)),
            _ => {}
        }
    }
    Ok(Arc::new(Schema::empty()))
}

pub(crate) fn inspect_msgpack_blocks(
    file_path: &Path,
    checkpoint_stride_rows: usize,
) -> Result<Vec<MsgpackBlockLocation>, BazanError> {
    let mut file = File::open(file_path)?;
    let file_size = file.metadata()?.len();
    let mut first_byte = None;
    let mut first_row = 0usize;
    let mut row_count = 0usize;
    let mut row_groups = Vec::new();
    let mut found_schema = false;
    let mut last_valid_end = 0u64;

    loop {
        let object_start = file.stream_position()?;
        if object_start >= file_size {
            break;
        }
        let value = rmpv::decode::read_value(&mut file)
            .map_err(|error| BazanError::Message(format!("MessagePack decode error: {error}")))?;
        last_valid_end = file.stream_position()?;
        if !found_schema {
            if !matches!(value, rmpv::Value::Map(_)) {
                continue;
            }
            found_schema = true;
        }

        first_byte.get_or_insert(object_start);
        row_count += 1;
        if row_count == checkpoint_stride_rows {
            let start = first_byte.take().expect("MsgPack block has a row");
            row_groups.push(MsgpackBlockLocation {
                first_row,
                row_count,
                first_byte: start,
                total_byte_size: last_valid_end.saturating_sub(start),
            });
            first_row = first_row.saturating_add(row_count);
            row_count = 0;
        }
    }

    if row_count > 0 {
        let start = first_byte.expect("MsgPack block has a row");
        row_groups.push(MsgpackBlockLocation {
            first_row,
            row_count,
            first_byte: start,
            total_byte_size: last_valid_end.saturating_sub(start),
        });
    }

    Ok(row_groups)
}

pub fn read_msgpack_range(
    file_path: &str,
    byte_offset: u64,
    offset: usize,
    limit: usize,
    batch_size: usize,
) -> Result<RecordBatch, BazanError> {
    let path = Path::new(file_path);
    let schema = infer_msgpack_schema(path)?;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(byte_offset.min(file.metadata()?.len())))?;
    let rows = MsgpackValues::new(BufReader::new(file));
    let chunker = RowChunker::new(
        rows,
        clamp_batch_size(batch_size),
        schema.clone(),
        msgpack_values_to_record_batch,
    );

    read_range_from_source(
        OpenedSource {
            schema,
            batches: Box::new(chunker),
        },
        offset,
        limit,
    )
}

/// MessagePack (.msgpack) Binary JSON Reader (Tier 3 Adapter)
#[derive(Debug, Clone, Copy, Default)]
pub struct MsgpackHandler;

impl FormatHandler for MsgpackHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let batch_size = clamp_batch_size(batch_size);
        let file = BufReader::new(File::open(file_path)?);
        let mut values = MsgpackValues::new(file);

        // Schema is inferred from the first Map row; rows before it are dropped.
        let mut schema: Option<Arc<Schema>> = None;
        let mut first: Option<rmpv::Value> = None;
        for val in values.by_ref() {
            let val = val?;
            if let rmpv::Value::Map(ref entries) = val {
                schema = Some(msgpack_schema_from_map(entries));
                first = Some(val);
                break;
            }
        }

        let schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
        let rows = first.into_iter().map(Ok).chain(values);
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
