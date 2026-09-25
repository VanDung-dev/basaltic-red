use std::path::Path;

use crate::error::BazanError;

use super::{
    delimited_delimiter, is_arrow_ipc_path, is_avro_path, is_json_array_path, is_msgpack_path,
    is_ndjson_path, is_orc_path, is_parquet_path, is_xlsx_path, resolve_healthy_map_entry,
    ResolvedArrowIpcRange, ResolvedAvroRange, ResolvedCsvRange, ResolvedJsonArrayRange,
    ResolvedMsgpackRange, ResolvedNdjsonRange, ResolvedOrcRange, ResolvedParquetRange,
    ResolvedXlsxRange, RowGroupLocation,
};

/// Resolve a file-local row range to the Parquet row groups that contain it.
///
/// Returns `None` when no compatible, healthy map is available so callers can
/// preserve the normal streaming fallback for old maps and non-Parquet files.
pub fn resolve_parquet_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedParquetRange>, BazanError> {
    if !is_parquet_path(file_path) || limit == 0 {
        return Ok(None);
    }

    let Some(entry) = resolve_healthy_map_entry(file_path)? else {
        return Ok(None);
    };

    let row_groups: Vec<RowGroupLocation> = serde_json::from_str(&entry.row_groups_json)?;
    if row_groups.is_empty() {
        return Ok(None);
    }
    if offset >= entry.total_rows {
        return Ok(Some(ResolvedParquetRange {
            row_groups: Vec::new(),
            offset: 0,
        }));
    }

    let end = offset.saturating_add(limit).min(entry.total_rows);
    let selected: Vec<&RowGroupLocation> = row_groups
        .iter()
        .filter(|group| {
            let group_end = group.first_row.saturating_add(group.row_count);
            group.first_row < end && group_end > offset
        })
        .collect();
    let Some(first_group) = selected.first() else {
        return Ok(Some(ResolvedParquetRange {
            row_groups: Vec::new(),
            offset: 0,
        }));
    };

    Ok(Some(ResolvedParquetRange {
        row_groups: selected.iter().map(|group| group.ordinal).collect(),
        offset: offset.saturating_sub(first_group.first_row),
    }))
}

/// Resolve an NDJSON row range to the byte checkpoint containing its first row.
pub fn resolve_ndjson_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedNdjsonRange>, BazanError> {
    if !is_ndjson_path(file_path) || limit == 0 {
        return Ok(None);
    }

    let Some(entry) = resolve_healthy_map_entry(file_path)? else {
        return Ok(None);
    };

    let row_groups: Vec<RowGroupLocation> = serde_json::from_str(&entry.row_groups_json)?;
    if row_groups.is_empty() {
        return Ok(None);
    }
    if offset >= entry.total_rows {
        return Ok(Some(ResolvedNdjsonRange {
            byte_offset: entry.size_bytes,
            offset: 0,
        }));
    }

    let Some(group) = row_groups.iter().find(|group| {
        offset >= group.first_row && offset < group.first_row.saturating_add(group.row_count)
    }) else {
        return Ok(None);
    };
    let Some(byte_offset) = group.first_byte else {
        return Ok(None);
    };

    Ok(Some(ResolvedNdjsonRange {
        byte_offset,
        offset: offset.saturating_sub(group.first_row),
    }))
}

/// Resolve a JSON-array row range to the byte checkpoint containing its first object.
pub fn resolve_json_array_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedJsonArrayRange>, BazanError> {
    if !is_json_array_path(file_path) || limit == 0 {
        return Ok(None);
    }

    let Some(entry) = resolve_healthy_map_entry(file_path)? else {
        return Ok(None);
    };

    let row_groups: Vec<RowGroupLocation> = serde_json::from_str(&entry.row_groups_json)?;
    if row_groups.is_empty() {
        return Ok(None);
    }
    if offset >= entry.total_rows {
        return Ok(Some(ResolvedJsonArrayRange {
            byte_offset: entry.size_bytes,
            offset: 0,
        }));
    }

    let Some(group) = row_groups.iter().find(|group| {
        offset >= group.first_row && offset < group.first_row.saturating_add(group.row_count)
    }) else {
        return Ok(None);
    };
    let Some(byte_offset) = group.first_byte else {
        return Ok(None);
    };

    Ok(Some(ResolvedJsonArrayRange {
        byte_offset,
        offset: offset.saturating_sub(group.first_row),
    }))
}

/// Resolve an ORC row range to the stripe checkpoint containing its first row.
pub fn resolve_orc_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedOrcRange>, BazanError> {
    if !is_orc_path(file_path) || limit == 0 {
        return Ok(None);
    }

    let Some(entry) = resolve_healthy_map_entry(file_path)? else {
        return Ok(None);
    };

    let row_groups: Vec<RowGroupLocation> = serde_json::from_str(&entry.row_groups_json)?;
    if row_groups.is_empty() {
        return Ok(None);
    }
    if offset >= entry.total_rows {
        return Ok(Some(ResolvedOrcRange {
            byte_offset: entry.size_bytes,
            offset: 0,
        }));
    }

    let Some(group) = row_groups.iter().find(|group| {
        offset >= group.first_row && offset < group.first_row.saturating_add(group.row_count)
    }) else {
        return Ok(None);
    };
    let Some(byte_offset) = group.first_byte else {
        return Ok(None);
    };

    Ok(Some(ResolvedOrcRange {
        byte_offset,
        offset: offset.saturating_sub(group.first_row),
    }))
}

/// Resolve an Avro row range to the OCF block containing its first row.
pub fn resolve_avro_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedAvroRange>, BazanError> {
    if !is_avro_path(file_path) || limit == 0 {
        return Ok(None);
    }

    let Some(entry) = resolve_healthy_map_entry(file_path)? else {
        return Ok(None);
    };

    let row_groups: Vec<RowGroupLocation> = serde_json::from_str(&entry.row_groups_json)?;
    if row_groups.is_empty() {
        return Ok(None);
    }
    if offset >= entry.total_rows {
        return Ok(Some(ResolvedAvroRange {
            byte_offset: entry.size_bytes,
            offset: 0,
        }));
    }

    let Some(group) = row_groups.iter().find(|group| {
        offset >= group.first_row && offset < group.first_row.saturating_add(group.row_count)
    }) else {
        return Ok(None);
    };
    let Some(byte_offset) = group.first_byte else {
        return Ok(None);
    };

    Ok(Some(ResolvedAvroRange {
        byte_offset,
        offset: offset.saturating_sub(group.first_row),
    }))
}

/// Resolve a MessagePack object range to the checkpoint containing its first object.
pub fn resolve_msgpack_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedMsgpackRange>, BazanError> {
    if !is_msgpack_path(file_path) || limit == 0 {
        return Ok(None);
    }

    let Some(entry) = resolve_healthy_map_entry(file_path)? else {
        return Ok(None);
    };

    let row_groups: Vec<RowGroupLocation> = serde_json::from_str(&entry.row_groups_json)?;
    if row_groups.is_empty() {
        return Ok(None);
    }
    if offset >= entry.total_rows {
        return Ok(Some(ResolvedMsgpackRange {
            byte_offset: entry.size_bytes,
            offset: 0,
        }));
    }

    let Some(group) = row_groups.iter().find(|group| {
        offset >= group.first_row && offset < group.first_row.saturating_add(group.row_count)
    }) else {
        return Ok(None);
    };
    let Some(byte_offset) = group.first_byte else {
        return Ok(None);
    };

    Ok(Some(ResolvedMsgpackRange {
        byte_offset,
        offset: offset.saturating_sub(group.first_row),
    }))
}

/// Resolve an XLSX data-row range to a logical worksheet block.
pub fn resolve_xlsx_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedXlsxRange>, BazanError> {
    if !is_xlsx_path(file_path) || limit == 0 {
        return Ok(None);
    }

    let Some(entry) = resolve_healthy_map_entry(file_path)? else {
        return Ok(None);
    };

    let row_groups: Vec<RowGroupLocation> = serde_json::from_str(&entry.row_groups_json)?;
    if row_groups.is_empty() {
        return Ok(None);
    }
    if offset >= entry.total_rows {
        return Ok(Some(ResolvedXlsxRange {
            row_offset: entry.total_rows,
            offset: 0,
        }));
    }

    let Some(group) = row_groups.iter().find(|group| {
        offset >= group.first_row && offset < group.first_row.saturating_add(group.row_count)
    }) else {
        return Ok(None);
    };

    Ok(Some(ResolvedXlsxRange {
        row_offset: group.first_row,
        offset: offset.saturating_sub(group.first_row),
    }))
}

/// Resolve an Arrow IPC / Feather row range to its first RecordBatch.
pub fn resolve_arrow_ipc_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedArrowIpcRange>, BazanError> {
    if !is_arrow_ipc_path(file_path) || limit == 0 {
        return Ok(None);
    }

    let Some(entry) = resolve_healthy_map_entry(file_path)? else {
        return Ok(None);
    };
    if offset >= entry.total_rows {
        return Ok(None);
    }

    let row_groups: Vec<RowGroupLocation> = serde_json::from_str(&entry.row_groups_json)?;
    let Some(group) = row_groups.iter().find(|group| {
        offset >= group.first_row && offset < group.first_row.saturating_add(group.row_count)
    }) else {
        return Ok(None);
    };

    Ok(Some(ResolvedArrowIpcRange {
        batch_ordinal: group.ordinal,
        offset: offset.saturating_sub(group.first_row),
    }))
}

/// Resolve a CSV row range to the byte checkpoint containing its first record.
pub fn resolve_delimited_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
    delimiter: u8,
) -> Result<Option<ResolvedCsvRange>, BazanError> {
    if delimited_delimiter(file_path) != Some(delimiter) || limit == 0 {
        return Ok(None);
    }

    let Some(entry) = resolve_healthy_map_entry(file_path)? else {
        return Ok(None);
    };

    let row_groups: Vec<RowGroupLocation> = serde_json::from_str(&entry.row_groups_json)?;
    if row_groups.is_empty() {
        return Ok(None);
    }
    if offset >= entry.total_rows {
        return Ok(Some(ResolvedCsvRange {
            byte_offset: entry.size_bytes,
            offset: 0,
        }));
    }

    let Some(group) = row_groups.iter().find(|group| {
        offset >= group.first_row && offset < group.first_row.saturating_add(group.row_count)
    }) else {
        return Ok(None);
    };
    let Some(byte_offset) = group.first_byte else {
        return Ok(None);
    };

    Ok(Some(ResolvedCsvRange {
        byte_offset,
        offset: offset.saturating_sub(group.first_row),
    }))
}

pub fn resolve_csv_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedCsvRange>, BazanError> {
    resolve_delimited_range(file_path, offset, limit, b',')
}

pub fn resolve_tsv_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedCsvRange>, BazanError> {
    resolve_delimited_range(file_path, offset, limit, b'\t')
}

pub fn resolve_psv_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedCsvRange>, BazanError> {
    resolve_delimited_range(file_path, offset, limit, b'|')
}

pub fn resolve_txt_range(
    file_path: &Path,
    offset: usize,
    limit: usize,
) -> Result<Option<ResolvedCsvRange>, BazanError> {
    resolve_delimited_range(file_path, offset, limit, b';')
}
