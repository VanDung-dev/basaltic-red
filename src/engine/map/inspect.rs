use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, Read};
use std::path::Path;
use std::time::UNIX_EPOCH;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use orc_rust::ArrowReaderBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::file::metadata::PageIndexPolicy;

use crate::engine::formats::{
    inspect_avro_blocks, inspect_msgpack_blocks, is_dynamic_format, resolve_handler_for_file,
};
use crate::error::BazanError;

use super::{
    ColumnLocation, ColumnMinMax, FileStats, FingerprintPolicy, LakeMapEntry, LakeMapOptions,
    PageLocation, RowGroupLocation,
};

pub(super) fn is_parquet_path(file_path: &Path) -> bool {
    matches!(
        file_path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
            .as_deref(),
        Some("parquet") | Some("pq")
    )
}

pub(super) fn is_ndjson_path(file_path: &Path) -> bool {
    let extension = file_path
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase);
    match extension.as_deref() {
        Some("ndjson") => true,
        Some("json" | "jsonl") => !is_json_array_path(file_path),
        _ => false,
    }
}

pub(super) fn is_json_array_path(file_path: &Path) -> bool {
    let extension = file_path
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase);
    match extension.as_deref() {
        Some("json" | "jsonl") => first_json_token(file_path) == Some(b'['),
        _ => false,
    }
}

fn first_json_token(file_path: &Path) -> Option<u8> {
    let mut reader = io::BufReader::new(File::open(file_path).ok()?);
    let mut buffer = [0u8; 8192];
    loop {
        let count = reader.read(&mut buffer).ok()?;
        if count == 0 {
            return None;
        }
        if let Some(token) = buffer[..count]
            .iter()
            .copied()
            .find(|byte| !matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
        {
            return Some(token);
        }
    }
}

pub(super) fn is_orc_path(file_path: &Path) -> bool {
    file_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("orc"))
}

pub(super) fn is_avro_path(file_path: &Path) -> bool {
    file_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("avro"))
}

pub(super) fn is_msgpack_path(file_path: &Path) -> bool {
    file_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("msgpack"))
}

pub(super) fn is_xlsx_path(file_path: &Path) -> bool {
    file_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("xlsx"))
}

pub(super) fn is_arrow_ipc_path(file_path: &Path) -> bool {
    file_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "ipc" | "arrow" | "feather"
            )
        })
}

pub(super) fn delimited_delimiter(file_path: &Path) -> Option<u8> {
    match file_path.extension().and_then(|value| value.to_str()) {
        Some(value) if value.eq_ignore_ascii_case("csv") => Some(b','),
        Some(value) if value.eq_ignore_ascii_case("tsv") => Some(b'\t'),
        Some(value) if value.eq_ignore_ascii_case("psv") => Some(b'|'),
        Some(value) if value.eq_ignore_ascii_case("txt") => Some(b';'),
        _ => None,
    }
}

fn inspect_ndjson_row_groups(
    file_path: &Path,
    checkpoint_stride_rows: usize,
) -> Result<Vec<RowGroupLocation>, BazanError> {
    let mut reader = io::BufReader::new(File::open(file_path)?);
    let mut line = Vec::new();
    let mut byte_offset = 0u64;
    let mut first_row = 0usize;
    let mut row_count = 0usize;
    let mut block_start = None;
    let mut row_groups = Vec::new();

    loop {
        line.clear();
        let bytes_read = reader.read_until(b'\n', &mut line)?;
        if bytes_read == 0 {
            break;
        }

        let line_start = byte_offset;
        byte_offset = byte_offset.saturating_add(bytes_read as u64);
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }

        block_start.get_or_insert(line_start);
        row_count += 1;
        if row_count == checkpoint_stride_rows {
            let first_byte = block_start.take().expect("NDJSON block has a row");
            row_groups.push(RowGroupLocation {
                ordinal: row_groups.len(),
                first_row,
                row_count,
                first_byte: Some(first_byte),
                total_byte_size: byte_offset.saturating_sub(first_byte),
                compressed_size: 0,
                columns: Vec::new(),
            });
            first_row = first_row.saturating_add(row_count);
            row_count = 0;
        }
    }

    if row_count > 0 {
        let first_byte = block_start.expect("NDJSON block has a row");
        row_groups.push(RowGroupLocation {
            ordinal: row_groups.len(),
            first_row,
            row_count,
            first_byte: Some(first_byte),
            total_byte_size: byte_offset.saturating_sub(first_byte),
            compressed_size: 0,
            columns: Vec::new(),
        });
    }

    Ok(row_groups)
}

fn inspect_json_array_row_groups(
    file_path: &Path,
    checkpoint_stride_rows: usize,
) -> Result<Vec<RowGroupLocation>, BazanError> {
    let mut reader = io::BufReader::new(File::open(file_path)?);
    let mut buffer = [0u8; 64 * 1024];
    let mut absolute_offset = 0u64;
    let mut object_start = None;
    let mut first_row = 0usize;
    let mut row_count = 0usize;
    let mut block_start = None;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut row_groups = Vec::new();

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        for &byte in &buffer[..bytes_read] {
            let byte_offset = absolute_offset;
            absolute_offset = absolute_offset.saturating_add(1);

            if in_string {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_string = false;
                }
                continue;
            }

            match byte {
                b'"' => in_string = true,
                b'{' => {
                    if depth == 0 {
                        object_start = Some(byte_offset);
                    }
                    depth = depth.saturating_add(1);
                }
                b'}' if depth > 0 => {
                    depth -= 1;
                    if depth == 0 {
                        let Some(start) = object_start.take() else {
                            continue;
                        };
                        block_start.get_or_insert(start);
                        row_count += 1;
                        if row_count == checkpoint_stride_rows {
                            let first_byte = block_start.take().expect("JSON block has a row");
                            row_groups.push(RowGroupLocation {
                                ordinal: row_groups.len(),
                                first_row,
                                row_count,
                                first_byte: Some(first_byte),
                                total_byte_size: absolute_offset.saturating_sub(first_byte),
                                compressed_size: 0,
                                columns: Vec::new(),
                            });
                            first_row = first_row.saturating_add(row_count);
                            row_count = 0;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if depth != 0 || in_string {
        return Err(BazanError::Message(format!(
            "Invalid JSON array structure in {}",
            file_path.display()
        )));
    }

    if row_count > 0 {
        let first_byte = block_start.expect("JSON block has a row");
        row_groups.push(RowGroupLocation {
            ordinal: row_groups.len(),
            first_row,
            row_count,
            first_byte: Some(first_byte),
            total_byte_size: absolute_offset.saturating_sub(first_byte),
            compressed_size: 0,
            columns: Vec::new(),
        });
    }

    Ok(row_groups)
}

fn inspect_orc_row_groups(file_path: &Path) -> Result<Vec<RowGroupLocation>, BazanError> {
    let file = File::open(file_path)?;
    let reader = ArrowReaderBuilder::try_new(file)
        .map_err(|e| BazanError::Message(format!("ORC error: {e}")))?;
    let mut first_row = 0usize;
    let mut row_groups = Vec::new();

    for (ordinal, stripe) in reader.file_metadata().stripe_metadatas().iter().enumerate() {
        let row_count = usize::try_from(stripe.number_of_rows()).map_err(|_| {
            BazanError::Message(format!("Invalid ORC row count in stripe {ordinal}"))
        })?;
        let total_byte_size = stripe
            .index_length()
            .saturating_add(stripe.data_length())
            .saturating_add(stripe.footer_length());
        row_groups.push(RowGroupLocation {
            ordinal,
            first_row,
            row_count,
            first_byte: Some(stripe.offset()),
            total_byte_size,
            compressed_size: total_byte_size,
            columns: Vec::new(),
        });
        first_row = first_row.saturating_add(row_count);
    }

    Ok(row_groups)
}

fn inspect_avro_row_groups(file_path: &Path) -> Result<Vec<RowGroupLocation>, BazanError> {
    Ok(inspect_avro_blocks(file_path)?
        .into_iter()
        .enumerate()
        .map(|(ordinal, block)| RowGroupLocation {
            ordinal,
            first_row: block.first_row,
            row_count: block.row_count,
            first_byte: Some(block.first_byte),
            total_byte_size: block.total_byte_size,
            compressed_size: block.compressed_size,
            columns: Vec::new(),
        })
        .collect())
}

fn inspect_msgpack_row_groups(
    file_path: &Path,
    checkpoint_stride_rows: usize,
) -> Result<Vec<RowGroupLocation>, BazanError> {
    Ok(inspect_msgpack_blocks(file_path, checkpoint_stride_rows)?
        .into_iter()
        .enumerate()
        .map(|(ordinal, block)| RowGroupLocation {
            ordinal,
            first_row: block.first_row,
            row_count: block.row_count,
            first_byte: Some(block.first_byte),
            total_byte_size: block.total_byte_size,
            compressed_size: 0,
            columns: Vec::new(),
        })
        .collect())
}

fn inspect_xlsx_row_groups(
    total_rows: usize,
    checkpoint_stride_rows: usize,
) -> Vec<RowGroupLocation> {
    let mut row_groups = Vec::new();
    let mut first_row = 0usize;
    while first_row < total_rows {
        let row_count = checkpoint_stride_rows.min(total_rows - first_row);
        row_groups.push(RowGroupLocation {
            ordinal: row_groups.len(),
            first_row,
            row_count,
            first_byte: None,
            total_byte_size: 0,
            compressed_size: 0,
            columns: Vec::new(),
        });
        first_row = first_row.saturating_add(row_count);
    }
    row_groups
}

fn inspect_delimited_row_groups(
    file_path: &Path,
    checkpoint_stride_rows: usize,
) -> Result<Vec<RowGroupLocation>, BazanError> {
    let mut reader = io::BufReader::new(File::open(file_path)?);
    let mut buffer = [0u8; 64 * 1024];
    let mut absolute_offset = 0u64;
    let mut record_start = 0u64;
    let mut header_seen = false;
    let mut in_quotes = false;
    let mut quote_pending = false;
    let mut record_has_content = false;
    let mut first_row = 0usize;
    let mut row_count = 0usize;
    let mut block_start = None;
    let mut row_groups = Vec::new();

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        for &byte in &buffer[..bytes_read] {
            absolute_offset += 1;

            if byte != b'\n' && (byte != b'\r' || in_quotes) {
                record_has_content = true;
            }

            if quote_pending {
                if byte == b'"' {
                    quote_pending = false;
                    continue;
                }
                in_quotes = false;
                quote_pending = false;
            }

            if byte == b'"' {
                if in_quotes {
                    quote_pending = true;
                } else {
                    in_quotes = true;
                }
            } else if byte == b'\n' && !in_quotes {
                let record_end = absolute_offset;
                if record_has_content {
                    if header_seen {
                        block_start.get_or_insert(record_start);
                        row_count += 1;
                        if row_count == checkpoint_stride_rows {
                            let first_byte = block_start.take().expect("CSV block has a row");
                            row_groups.push(RowGroupLocation {
                                ordinal: row_groups.len(),
                                first_row,
                                row_count,
                                first_byte: Some(first_byte),
                                total_byte_size: record_end.saturating_sub(first_byte),
                                compressed_size: 0,
                                columns: Vec::new(),
                            });
                            first_row = first_row.saturating_add(row_count);
                            row_count = 0;
                        }
                    }
                    record_has_content = false;
                    if !header_seen {
                        header_seen = true;
                    }
                }
                record_start = record_end;
            }
        }
    }

    if header_seen {
        if record_start < absolute_offset && record_has_content {
            block_start.get_or_insert(record_start);
            row_count += 1;
        }
        if row_count > 0 {
            let first_byte = block_start.expect("CSV block has a row");
            row_groups.push(RowGroupLocation {
                ordinal: row_groups.len(),
                first_row,
                row_count,
                first_byte: Some(first_byte),
                total_byte_size: absolute_offset.saturating_sub(first_byte),
                compressed_size: 0,
                columns: Vec::new(),
            });
        }
    }

    Ok(row_groups)
}

fn inspect_parquet_row_groups(file_path: &Path) -> Result<Vec<RowGroupLocation>, BazanError> {
    let file = File::open(file_path)?;
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    let mut first_row = 0usize;
    let mut row_groups = Vec::with_capacity(builder.metadata().num_row_groups());
    let offset_indexes = builder.metadata().offset_index();

    for (ordinal, row_group) in builder.metadata().row_groups().iter().enumerate() {
        let row_count = usize::try_from(row_group.num_rows()).map_err(|_| {
            BazanError::Message(format!(
                "Invalid negative row count in Parquet row group {}: {}",
                ordinal,
                row_group.num_rows()
            ))
        })?;
        let columns = row_group
            .columns()
            .iter()
            .enumerate()
            .map(|(column_index, column)| {
                let (offset, length) = column.byte_range();
                let pages = offset_indexes
                    .and_then(|indexes| indexes.get(ordinal))
                    .and_then(|indexes| indexes.get(column_index))
                    .map(|index| {
                        index
                            .page_locations()
                            .iter()
                            .filter_map(|page| {
                                Some(PageLocation {
                                    first_row: usize::try_from(page.first_row_index).ok()?,
                                    offset: u64::try_from(page.offset).ok()?,
                                    length: u64::try_from(page.compressed_page_size).ok()?,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                ColumnLocation {
                    path: column.column_path().string(),
                    offset,
                    length,
                    pages,
                }
            })
            .collect::<Vec<_>>();
        let first_byte = columns.iter().map(|column| column.offset).min();

        row_groups.push(RowGroupLocation {
            ordinal,
            first_row,
            row_count,
            first_byte,
            total_byte_size: row_group.total_byte_size().max(0) as u64,
            compressed_size: row_group.compressed_size().max(0) as u64,
            columns,
        });
        first_row = first_row.saturating_add(row_count);
    }

    Ok(row_groups)
}

/// Helper to extract stats, row count, and physical row-group locations from a single data file.
pub(super) fn inspect_file_entry(
    root_dir: &Path,
    file_path: &Path,
    options: &LakeMapOptions,
) -> Result<LakeMapEntry, BazanError> {
    let rel = file_path
        .strip_prefix(root_dir)
        .unwrap_or(file_path)
        .to_string_lossy()
        .to_string();

    let meta = fs::metadata(file_path)?;
    let size_bytes = meta.len();
    let mtime_ms = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    // In content-fingerprint mode, bracket the full inspection with hashes so
    // the map never records locations/stats from bytes that changed mid-scan.
    let initial_content_hash = match &options.fingerprint {
        FingerprintPolicy::Metadata => None,
        FingerprintPolicy::Blake3 => Some(blake3_file_hash(file_path)?),
    };

    let file_str = file_path.to_str().unwrap_or("");
    let handler = resolve_handler_for_file(file_str).ok_or_else(|| {
        BazanError::Message(format!(
            "Unsupported format for map inspection: {}",
            file_str
        ))
    })?;
    let dynamic_override = file_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(is_dynamic_format);

    let source = handler.open(file_str, 64 * 1024)?;
    let mut total_rows = 0usize;
    let mut col_stats: HashMap<String, ColumnMinMax> = HashMap::new();
    let mut arrow_row_groups = Vec::new();

    for batch_res in source.batches {
        let batch = batch_res?;
        let batch_rows = batch.num_rows();
        if is_arrow_ipc_path(file_path) {
            arrow_row_groups.push(RowGroupLocation {
                ordinal: arrow_row_groups.len(),
                first_row: total_rows,
                row_count: batch_rows,
                first_byte: None,
                total_byte_size: 0,
                compressed_size: 0,
                columns: Vec::new(),
            });
        }
        total_rows += batch_rows;

        if batch_rows > 0 {
            for field in batch.schema().fields() {
                let name = field.name().clone();
                let col = batch.column_by_name(&name);

                if let Some(col) = col {
                    let selected = options
                        .stats_columns
                        .as_ref()
                        .is_none_or(|columns| columns.iter().any(|column| column == field.name()));
                    if !selected {
                        continue;
                    }

                    match field.data_type() {
                        DataType::Int64 => {
                            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                                if let (Some(min), Some(max)) = (
                                    arrow::compute::kernels::aggregate::min(arr),
                                    arrow::compute::kernels::aggregate::max(arr),
                                ) {
                                    let current = col_stats.entry(name).or_insert(ColumnMinMax {
                                        min: None,
                                        max: None,
                                        min_str: None,
                                        max_str: None,
                                    });
                                    current.min =
                                        Some(current.min.map_or(min as f64, |v| v.min(min as f64)));
                                    current.max =
                                        Some(current.max.map_or(max as f64, |v| v.max(max as f64)));
                                }
                            }
                        }
                        DataType::Float64 => {
                            if let Some(arr) =
                                col.as_any().downcast_ref::<arrow::array::Float64Array>()
                            {
                                if let (Some(min), Some(max)) = (
                                    arrow::compute::kernels::aggregate::min(arr),
                                    arrow::compute::kernels::aggregate::max(arr),
                                ) {
                                    let current = col_stats.entry(name).or_insert(ColumnMinMax {
                                        min: None,
                                        max: None,
                                        min_str: None,
                                        max_str: None,
                                    });
                                    current.min = Some(current.min.map_or(min, |v| v.min(min)));
                                    current.max = Some(current.max.map_or(max, |v| v.max(max)));
                                }
                            }
                        }
                        DataType::Utf8 => {
                            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                                if let (Some(min), Some(max)) = (
                                    arrow::compute::kernels::aggregate::min_string(arr),
                                    arrow::compute::kernels::aggregate::max_string(arr),
                                ) {
                                    let current = col_stats.entry(name).or_insert(ColumnMinMax {
                                        min: None,
                                        max: None,
                                        min_str: None,
                                        max_str: None,
                                    });
                                    if current.min_str.as_ref().is_none_or(|v| min < v.as_str()) {
                                        current.min_str = Some(min.to_string());
                                    }
                                    if current.max_str.as_ref().is_none_or(|v| max > v.as_str()) {
                                        current.max_str = Some(max.to_string());
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    let row_groups_json = if dynamic_override {
        "[]".to_string()
    } else if is_parquet_path(file_path) {
        serde_json::to_string(&inspect_parquet_row_groups(file_path)?)?
    } else if is_ndjson_path(file_path) {
        serde_json::to_string(&inspect_ndjson_row_groups(
            file_path,
            options.checkpoint_stride_rows.get(),
        )?)?
    } else if is_json_array_path(file_path) {
        serde_json::to_string(&inspect_json_array_row_groups(
            file_path,
            options.checkpoint_stride_rows.get(),
        )?)?
    } else if is_orc_path(file_path) {
        serde_json::to_string(&inspect_orc_row_groups(file_path)?)?
    } else if is_avro_path(file_path) {
        serde_json::to_string(&inspect_avro_row_groups(file_path)?)?
    } else if is_msgpack_path(file_path) {
        serde_json::to_string(&inspect_msgpack_row_groups(
            file_path,
            options.checkpoint_stride_rows.get(),
        )?)?
    } else if is_xlsx_path(file_path) {
        serde_json::to_string(&inspect_xlsx_row_groups(
            total_rows,
            options.checkpoint_stride_rows.get(),
        ))?
    } else if is_arrow_ipc_path(file_path) {
        serde_json::to_string(&arrow_row_groups)?
    } else if delimited_delimiter(file_path).is_some() {
        serde_json::to_string(&inspect_delimited_row_groups(
            file_path,
            options.checkpoint_stride_rows.get(),
        )?)?
    } else {
        "[]".to_string()
    };

    let stats = FileStats {
        total_rows,
        columns: col_stats,
    };
    let stats_json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
    let final_meta = fs::metadata(file_path)?;
    let final_mtime_ms = final_meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    if final_meta.len() != size_bytes || final_mtime_ms != mtime_ms {
        return Err(BazanError::Message(format!(
            "Source changed while building lake map: {}",
            file_path.display()
        )));
    }

    let content_hash = match initial_content_hash {
        None => None,
        Some(initial_hash) => {
            let final_hash = blake3_file_hash(file_path)?;
            if final_hash != initial_hash {
                return Err(BazanError::Message(format!(
                    "Source content changed while building lake map: {}",
                    file_path.display()
                )));
            }
            Some(final_hash)
        }
    };

    Ok(LakeMapEntry {
        rel_path: rel,
        size_bytes,
        mtime_ms,
        first_global_row: 0,
        total_rows,
        stats_json,
        row_groups_json,
        content_hash,
    })
}

pub(super) fn blake3_file_hash(file_path: &Path) -> Result<String, BazanError> {
    let mut file = File::open(file_path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}
