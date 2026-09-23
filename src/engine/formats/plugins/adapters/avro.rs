use apache_avro::types::Value;
use apache_avro::Reader as AvroReader;
use arrow_array::builder::*;
use arrow_array::*;
use arrow_schema::{DataType, Field, Schema};
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use crate::engine::formats::plugins::base_templates::RowChunker;
use crate::engine::formats::{
    clamp_batch_size, read_range_from_source, FormatHandler, OpenedSource,
};
use crate::error::BazanError;

#[derive(Debug, Clone, Copy)]
pub(crate) struct AvroBlockLocation {
    pub first_row: usize,
    pub row_count: usize,
    pub first_byte: u64,
    pub total_byte_size: u64,
    pub compressed_size: u64,
}

fn avro_error(message: impl Into<String>) -> BazanError {
    BazanError::Message(format!("Avro error: {}", message.into()))
}

fn read_avro_long(file: &mut File, offset: &mut u64) -> Result<Option<i64>, BazanError> {
    let mut value = 0u64;
    for shift in (0..=63).step_by(7) {
        let mut byte = [0u8; 1];
        match file.read(&mut byte)? {
            0 if shift == 0 => return Ok(None),
            0 => return Err(avro_error("truncated variable-length integer")),
            _ => {}
        }
        *offset = offset.saturating_add(1);

        let payload = byte[0] & 0x7f;
        if shift == 63 && payload > 1 {
            return Err(avro_error("variable-length integer is too large"));
        }
        value |= u64::from(payload) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(Some(((value >> 1) as i64) ^ -((value & 1) as i64)));
        }
    }

    Err(avro_error("variable-length integer is too long"))
}

fn required_avro_long(file: &mut File, offset: &mut u64) -> Result<i64, BazanError> {
    read_avro_long(file, offset)?.ok_or_else(|| avro_error("unexpected end of file"))
}

fn skip_avro_bytes(file: &mut File, offset: &mut u64, length: i64) -> Result<(), BazanError> {
    let length = u64::try_from(length).map_err(|_| avro_error("negative byte length"))?;
    let next = offset
        .checked_add(length)
        .ok_or_else(|| avro_error("byte offset overflow"))?;
    file.seek(SeekFrom::Start(next))?;
    *offset = next;
    Ok(())
}

fn read_avro_header(file_path: &Path) -> Result<(Vec<u8>, u64, [u8; 16]), BazanError> {
    let mut file = File::open(file_path)?;
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)?;
    if magic != [b'O', b'b', b'j', 1] {
        return Err(avro_error("invalid object container magic"));
    }

    let mut offset = magic.len() as u64;
    loop {
        let count = required_avro_long(&mut file, &mut offset)?;
        if count == 0 {
            break;
        }

        let entries = if count < 0 {
            let block_size = required_avro_long(&mut file, &mut offset)?;
            if block_size < 0 {
                return Err(avro_error("negative metadata block size"));
            }
            count
                .checked_abs()
                .ok_or_else(|| avro_error("metadata block count overflow"))? as u64
        } else {
            count as u64
        };

        for _ in 0..entries {
            let key_length = required_avro_long(&mut file, &mut offset)?;
            skip_avro_bytes(&mut file, &mut offset, key_length)?;
            let value_length = required_avro_long(&mut file, &mut offset)?;
            skip_avro_bytes(&mut file, &mut offset, value_length)?;
        }
    }

    let mut marker = [0u8; 16];
    file.read_exact(&mut marker)?;
    offset = offset.saturating_add(marker.len() as u64);

    file.seek(SeekFrom::Start(0))?;
    let mut header =
        vec![0u8; usize::try_from(offset).map_err(|_| avro_error("header is too large"))?];
    file.read_exact(&mut header)?;
    Ok((header, offset, marker))
}

pub(crate) fn inspect_avro_blocks(file_path: &Path) -> Result<Vec<AvroBlockLocation>, BazanError> {
    let (_, data_start, marker) = read_avro_header(file_path)?;
    let mut file = File::open(file_path)?;
    file.seek(SeekFrom::Start(data_start))?;
    let mut offset = data_start;
    let mut first_row = 0usize;
    let mut blocks = Vec::new();

    loop {
        let block_start = offset;
        let Some(row_count) = read_avro_long(&mut file, &mut offset)? else {
            break;
        };
        if row_count <= 0 {
            return Err(avro_error("data block row count must be positive"));
        }
        let compressed_size = required_avro_long(&mut file, &mut offset)?;
        if compressed_size < 0 {
            return Err(avro_error("negative data block size"));
        }
        skip_avro_bytes(&mut file, &mut offset, compressed_size)?;

        let mut block_marker = [0u8; 16];
        file.read_exact(&mut block_marker)?;
        offset = offset.saturating_add(block_marker.len() as u64);
        if block_marker != marker {
            return Err(avro_error("data block sync marker mismatch"));
        }

        let row_count = usize::try_from(row_count)
            .map_err(|_| avro_error("data block row count is too large"))?;
        blocks.push(AvroBlockLocation {
            first_row,
            row_count,
            first_byte: block_start,
            total_byte_size: offset.saturating_sub(block_start),
            compressed_size: compressed_size as u64,
        });
        first_row = first_row.saturating_add(row_count);
    }

    Ok(blocks)
}

fn avro_schema_to_arrow(avro_schema: &apache_avro::Schema) -> Arc<Schema> {
    let mut fields = Vec::new();
    if let apache_avro::Schema::Record(record) = avro_schema {
        for field in &record.fields {
            let data_type = match &field.schema {
                apache_avro::Schema::Long => DataType::Int64,
                apache_avro::Schema::Int => DataType::Int32,
                apache_avro::Schema::Double => DataType::Float64,
                apache_avro::Schema::Boolean => DataType::Boolean,
                _ => DataType::Utf8,
            };
            fields.push(Field::new(&field.name, data_type, true));
        }
    }
    Arc::new(Schema::new(fields))
}

pub fn read_avro_range(
    file_path: &str,
    byte_offset: u64,
    offset: usize,
    limit: usize,
    batch_size: usize,
) -> Result<RecordBatch, BazanError> {
    let path = Path::new(file_path);
    let (header, _, _) = read_avro_header(path)?;
    let mut file = File::open(path)?;
    let file_size = file.metadata()?.len();
    file.seek(SeekFrom::Start(byte_offset.min(file_size)))?;
    let reader = AvroReader::new(Cursor::new(header).chain(file))?;
    let arrow_schema = avro_schema_to_arrow(reader.writer_schema());
    let rows = reader.map(|result| result.map_err(BazanError::from));
    let chunker = RowChunker::new(
        rows,
        clamp_batch_size(batch_size),
        arrow_schema.clone(),
        avro_values_to_record_batch,
    );

    read_range_from_source(
        OpenedSource {
            schema: arrow_schema,
            batches: Box::new(chunker),
        },
        offset,
        limit,
    )
}

/// Apache Avro Streaming Reader (Tier 3 Adapter)
#[derive(Debug, Clone, Copy, Default)]
pub struct AvroHandler;

impl FormatHandler for AvroHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let batch_size = clamp_batch_size(batch_size);
        let file = BufReader::new(File::open(file_path)?);
        let reader = AvroReader::new(file)?;
        let arrow_schema = avro_schema_to_arrow(reader.writer_schema());

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
