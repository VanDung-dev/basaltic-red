use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufRead, Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, UNIX_EPOCH};

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use orc_rust::ArrowReaderBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::file::metadata::PageIndexPolicy;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::engine::formats::{
    inspect_avro_blocks, inspect_msgpack_blocks, is_dynamic_format, resolve_handler_for_file,
};
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use crate::utils::discover_data_files;

pub const DEFAULT_MAP_FILENAME: &str = ".br_map.bazan";
pub const LEGACY_MAP_FILENAME: &str = ".br_map.ipc";
const DEFAULT_CHECKPOINT_STRIDE_ROWS: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FingerprintPolicy {
    Metadata,
    Blake3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeMapOptions {
    pub checkpoint_stride_rows: NonZeroUsize,
    pub fingerprint: FingerprintPolicy,
    /// `None` means all currently supported stats; `Some([])` disables stats.
    pub stats_columns: Option<Vec<String>>,
}

impl Default for LakeMapOptions {
    fn default() -> Self {
        Self {
            checkpoint_stride_rows: NonZeroUsize::new(DEFAULT_CHECKPOINT_STRIDE_ROWS)
                .expect("default checkpoint stride is non-zero"),
            fingerprint: FingerprintPolicy::Metadata,
            stats_columns: None,
        }
    }
}

impl LakeMapOptions {
    pub fn new(
        checkpoint_stride_rows: usize,
        fingerprint: &str,
        stats_columns: Option<Vec<String>>,
    ) -> Result<Self, BazanError> {
        let checkpoint_stride_rows =
            NonZeroUsize::new(checkpoint_stride_rows).ok_or_else(|| {
                BazanError::Message("checkpoint_stride_rows must be greater than zero".to_string())
            })?;
        let fingerprint = match fingerprint {
            "metadata" => FingerprintPolicy::Metadata,
            "blake3" => FingerprintPolicy::Blake3,
            other => {
                return Err(BazanError::Message(format!(
                    "Unsupported map fingerprint policy: {other}"
                )))
            }
        };
        let stats_columns = stats_columns.map(|mut columns| {
            columns.sort();
            columns.dedup();
            columns
        });

        Ok(Self {
            checkpoint_stride_rows,
            fingerprint,
            stats_columns,
        })
    }

    fn from_schema_metadata(metadata: &HashMap<String, String>) -> Result<Self, BazanError> {
        match metadata.get("bazan.map_schema").map(String::as_str) {
            None | Some("1") | Some("2") => Ok(Self::default()),
            Some("3") => {
                let stride = metadata
                    .get("bazan.checkpoint_stride_rows")
                    .and_then(|value| value.parse::<usize>().ok())
                    .ok_or_else(|| {
                        BazanError::Message("Invalid map checkpoint stride metadata".to_string())
                    })?;
                let stats_columns = metadata
                    .get("bazan.stats_columns")
                    .ok_or_else(|| {
                        BazanError::Message("Missing map stats policy metadata".to_string())
                    })
                    .and_then(|value| {
                        serde_json::from_str::<Option<Vec<String>>>(value).map_err(BazanError::from)
                    })?;
                let fingerprint = metadata.get("bazan.fingerprint").ok_or_else(|| {
                    BazanError::Message("Missing map fingerprint policy metadata".to_string())
                })?;
                Self::new(stride, fingerprint, stats_columns)
            }
            Some(other) => Err(BazanError::Message(format!(
                "Unsupported lake map schema: {other}"
            ))),
        }
    }

    fn schema_metadata(&self) -> Result<HashMap<String, String>, BazanError> {
        Ok(HashMap::from([
            (
                "bazan.checkpoint_stride_rows".to_string(),
                self.checkpoint_stride_rows.get().to_string(),
            ),
            (
                "bazan.fingerprint".to_string(),
                match &self.fingerprint {
                    FingerprintPolicy::Metadata => "metadata",
                    FingerprintPolicy::Blake3 => "blake3",
                }
                .to_string(),
            ),
            (
                "bazan.stats_columns".to_string(),
                serde_json::to_string(&self.stats_columns)?,
            ),
        ]))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMinMax {
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub min_str: Option<String>,
    pub max_str: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStats {
    pub total_rows: usize,
    pub columns: HashMap<String, ColumnMinMax>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLocation {
    pub path: String,
    pub offset: u64,
    pub length: u64,
    #[serde(default)]
    pub pages: Vec<PageLocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageLocation {
    pub first_row: usize,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowGroupLocation {
    pub ordinal: usize,
    pub first_row: usize,
    pub row_count: usize,
    pub first_byte: Option<u64>,
    pub total_byte_size: u64,
    pub compressed_size: u64,
    pub columns: Vec<ColumnLocation>,
}

#[derive(Debug, Clone)]
pub struct LakeMapEntry {
    pub rel_path: String,
    pub size_bytes: u64,
    pub mtime_ms: i64,
    pub first_global_row: usize,
    pub total_rows: usize,
    pub stats_json: String,
    pub row_groups_json: String,
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LakeMap {
    pub entries: Vec<LakeMapEntry>,
    pub total_files: usize,
    pub total_rows: usize,
    pub total_bytes: u64,
    pub options: LakeMapOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorReport {
    pub status: String, // "HEALTHY" | "DRIFT_DETECTED" | "HEALED"
    pub total_files: usize,
    pub healthy_count: usize,
    pub modified_files: Vec<String>,
    pub unindexed_files: Vec<String>,
    pub missing_files: Vec<String>,
    pub healed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedParquetRange {
    pub row_groups: Vec<usize>,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedNdjsonRange {
    pub byte_offset: u64,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedJsonArrayRange {
    pub byte_offset: u64,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedOrcRange {
    pub byte_offset: u64,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAvroRange {
    pub byte_offset: u64,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMsgpackRange {
    pub byte_offset: u64,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedXlsxRange {
    pub row_offset: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedArrowIpcRange {
    pub batch_ordinal: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCsvRange {
    pub byte_offset: u64,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalRowLocation {
    pub rel_path: String,
    pub file_offset: usize,
    pub row_group: Option<usize>,
    pub row_in_group: Option<usize>,
    pub page_indexed: bool,
}

/// Helper function to format bytes into human-readable string
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Helper function to format speed in bytes/sec into human-readable string
pub fn format_bytes_speed(bytes_per_sec: f64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    if bytes_per_sec >= GB {
        format!("{:.2} GB/s", bytes_per_sec / GB)
    } else if bytes_per_sec >= MB {
        format!("{:.1} MB/s", bytes_per_sec / MB)
    } else if bytes_per_sec >= KB {
        format!("{:.1} KB/s", bytes_per_sec / KB)
    } else {
        format!("{:.0} B/s", bytes_per_sec)
    }
}

/// Real-time Progress Bar & Telemetry HUD for Map Generation
pub struct MapProgressTracker {
    total_files: usize,
    total_bytes: u64,
    processed_files: AtomicUsize,
    processed_bytes: AtomicU64,
    start_time: Instant,
    last_render_time: Mutex<Instant>,
    show_progress: bool,
}

impl MapProgressTracker {
    pub fn new(total_files: usize, total_bytes: u64, show_progress: bool) -> Self {
        let now = Instant::now();
        Self {
            total_files,
            total_bytes,
            processed_files: AtomicUsize::new(0),
            processed_bytes: AtomicU64::new(0),
            start_time: now,
            last_render_time: Mutex::new(now),
            show_progress,
        }
    }

    pub fn inc(&self, bytes: u64) {
        let files = self.processed_files.fetch_add(1, Ordering::Relaxed) + 1;
        let read_bytes = self.processed_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;

        if !self.show_progress {
            return;
        }

        let now = Instant::now();
        let mut last_render = self.last_render_time.lock().unwrap();
        // Throttle rendering to at most once every 30ms or when 100% complete
        if now.duration_since(*last_render).as_millis() >= 30 || files == self.total_files {
            *last_render = now;
            self.render(files, read_bytes, now);
        }
    }

    fn render(&self, files: usize, read_bytes: u64, now: Instant) {
        let elapsed = now.duration_since(self.start_time).as_secs_f64();
        let elapsed_secs = elapsed as u64;
        let elapsed_str = format!("{:02}:{:02}", elapsed_secs / 60, elapsed_secs % 60);

        let speed = if elapsed > 0.001 {
            (read_bytes as f64) / elapsed
        } else {
            0.0
        };

        let is_done = files == self.total_files;

        let eta_str = if is_done {
            "00:00".to_string()
        } else if speed > 0.0 && self.total_bytes > read_bytes {
            let rem_bytes = self.total_bytes - read_bytes;
            let eta_secs = (rem_bytes as f64 / speed) as u64;
            format!("{:02}:{:02}", eta_secs / 60, eta_secs % 60)
        } else {
            "00:00".to_string()
        };

        let pct = if self.total_bytes > 0 {
            ((read_bytes as f64 / self.total_bytes as f64) * 100.0).min(100.0)
        } else if self.total_files > 0 {
            ((files as f64 / self.total_files as f64) * 100.0).min(100.0)
        } else {
            100.0
        };

        let bar_width = 26;
        let filled_units = (pct / 100.0) * bar_width as f64;
        let full_blocks = filled_units as usize;
        let sub_idx = ((filled_units - full_blocks as f64) * 8.0) as usize;
        let sub_chars = ["", "▏", "▎", "▍", "▌", "▋", "▊", "▉"];
        let sub_char = if full_blocks < bar_width {
            sub_chars[sub_idx.min(7)]
        } else {
            ""
        };
        let empty_blocks =
            bar_width.saturating_sub(full_blocks + if !sub_char.is_empty() { 1 } else { 0 });

        let speed_str = format_bytes_speed(speed);
        let read_str = format_bytes(read_bytes);
        let total_str = format_bytes(self.total_bytes);

        if is_done {
            eprint!(
                "\r\x1b[2K\x1b[1;38;2;34;197;94m✨ basaltic-red\x1b[0m \x1b[90m›\x1b[0m \x1b[38;2;34;197;94m{}\x1b[0m \x1b[1;32m100.0%\x1b[0m \x1b[90m│\x1b[0m \x1b[1;37m{}/{} files\x1b[0m \x1b[90m│\x1b[0m \x1b[37m{}\x1b[0m \x1b[90m│\x1b[0m \x1b[38;2;168;85;247m{}\x1b[0m \x1b[90m│\x1b[0m \x1b[38;2;34;197;94mDone in {}\x1b[0m\n",
                "█".repeat(bar_width),
                files,
                self.total_files,
                total_str,
                speed_str,
                elapsed_str
            );
        } else {
            eprint!(
                "\r\x1b[2K\x1b[1;38;2;239;68;68m⚡ basaltic-red\x1b[0m \x1b[90m›\x1b[0m \x1b[38;2;56;189;248m{}{}\x1b[38;2;71;85;105m{}\x1b[0m \x1b[1;37m{:>5.1}%\x1b[0m \x1b[90m│\x1b[0m \x1b[36m{}/{}\x1b[90m files\x1b[0m \x1b[90m│\x1b[0m \x1b[37m{}\x1b[90m/{}\x1b[0m \x1b[90m│\x1b[0m \x1b[38;2;168;85;247m{}\x1b[0m \x1b[90m│\x1b[0m \x1b[90mETA\x1b[0m \x1b[33m{}\x1b[0m \x1b[90m[{}]\x1b[0m",
                "█".repeat(full_blocks),
                sub_char,
                "─".repeat(empty_blocks),
                pct,
                files,
                self.total_files,
                read_str,
                total_str,
                speed_str,
                eta_str,
                elapsed_str
            );
        }
        let _ = io::stderr().flush();
    }
}

impl LakeMap {
    pub fn new(entries: Vec<LakeMapEntry>) -> Self {
        Self::new_with_options(entries, LakeMapOptions::default())
    }

    pub fn new_with_options(mut entries: Vec<LakeMapEntry>, options: LakeMapOptions) -> Self {
        entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        let mut first_global_row = 0usize;
        for entry in &mut entries {
            entry.first_global_row = first_global_row;
            first_global_row = first_global_row.saturating_add(entry.total_rows);
        }

        let total_files = entries.len();
        let total_rows = entries.iter().map(|e| e.total_rows).sum();
        let total_bytes = entries.iter().map(|e| e.size_bytes).sum();
        Self {
            entries,
            total_files,
            total_rows,
            total_bytes,
            options,
        }
    }

    /// Convert LakeMap into an Arrow RecordBatch for Zero-Copy IPC serialization
    pub fn to_record_batch(&self) -> Result<RecordBatch, BazanError> {
        let mut schema_metadata = HashMap::from([
            ("bazan.kind".to_string(), "lake_map".to_string()),
            ("bazan.version".to_string(), "1".to_string()),
            ("bazan.payload".to_string(), "arrow_ipc".to_string()),
            ("bazan.map_schema".to_string(), "3".to_string()),
        ]);
        schema_metadata.extend(self.options.schema_metadata()?);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("rel_path", DataType::Utf8, false),
                Field::new("size_bytes", DataType::UInt64, false),
                Field::new("mtime_ms", DataType::Int64, false),
                Field::new("total_rows", DataType::UInt64, false),
                Field::new("stats_json", DataType::Utf8, false),
                Field::new("first_global_row", DataType::UInt64, false),
                Field::new("row_groups_json", DataType::Utf8, false),
                Field::new("content_hash", DataType::Utf8, true),
            ],
            schema_metadata,
        ));

        let rel_paths: Vec<&str> = self.entries.iter().map(|e| e.rel_path.as_str()).collect();
        let sizes: Vec<u64> = self.entries.iter().map(|e| e.size_bytes).collect();
        let mtimes: Vec<i64> = self.entries.iter().map(|e| e.mtime_ms).collect();
        let rows: Vec<u64> = self.entries.iter().map(|e| e.total_rows as u64).collect();
        let stats: Vec<&str> = self.entries.iter().map(|e| e.stats_json.as_str()).collect();
        let first_rows: Vec<u64> = self
            .entries
            .iter()
            .map(|e| e.first_global_row as u64)
            .collect();
        let row_groups: Vec<&str> = self
            .entries
            .iter()
            .map(|e| e.row_groups_json.as_str())
            .collect();
        let content_hashes: Vec<Option<&str>> = self
            .entries
            .iter()
            .map(|entry| entry.content_hash.as_deref())
            .collect();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(rel_paths)),
            Arc::new(UInt64Array::from(sizes)),
            Arc::new(Int64Array::from(mtimes)),
            Arc::new(UInt64Array::from(rows)),
            Arc::new(StringArray::from(stats)),
            Arc::new(UInt64Array::from(first_rows)),
            Arc::new(StringArray::from(row_groups)),
            Arc::new(StringArray::from(content_hashes)),
        ];

        RecordBatch::try_new(schema, columns).map_err(BazanError::from)
    }

    /// Load LakeMap from an Arrow RecordBatch
    pub fn from_record_batch(batch: &RecordBatch) -> Result<Self, BazanError> {
        let options = LakeMapOptions::from_schema_metadata(batch.schema().metadata())?;
        if batch.num_columns() < 5 || batch.num_columns() == 6 {
            return Err(BazanError::Message(format!(
                "Invalid lake map column count: {}",
                batch.num_columns()
            )));
        }
        if batch
            .schema()
            .metadata()
            .get("bazan.map_schema")
            .is_some_and(|version| version == "3")
            && batch.num_columns() < 8
        {
            return Err(BazanError::Message(
                "Lake map schema 3 requires the content_hash column".to_string(),
            ));
        }
        let rel_path_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| BazanError::Message("Invalid rel_path column".to_string()))?;
        let size_arr = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| BazanError::Message("Invalid size_bytes column".to_string()))?;
        let mtime_arr = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BazanError::Message("Invalid mtime_ms column".to_string()))?;
        let rows_arr = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| BazanError::Message("Invalid total_rows column".to_string()))?;
        let stats_arr = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| BazanError::Message("Invalid stats_json column".to_string()))?;
        let first_rows_arr = if batch.num_columns() >= 7 {
            Some(
                batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        BazanError::Message("Invalid first_global_row column".to_string())
                    })?,
            )
        } else {
            None
        };
        let row_groups_arr = if batch.num_columns() >= 7 {
            Some(
                batch
                    .column(6)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        BazanError::Message("Invalid row_groups_json column".to_string())
                    })?,
            )
        } else {
            None
        };
        let content_hash_arr = batch
            .schema()
            .index_of("content_hash")
            .ok()
            .map(|column_index| {
                batch
                    .column(column_index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| BazanError::Message("Invalid content_hash column".to_string()))
            })
            .transpose()?;

        let num_rows = batch.num_rows();
        let mut entries = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            entries.push(LakeMapEntry {
                rel_path: rel_path_arr.value(i).to_string(),
                size_bytes: size_arr.value(i),
                mtime_ms: mtime_arr.value(i),
                first_global_row: first_rows_arr.map(|arr| arr.value(i) as usize).unwrap_or(0),
                total_rows: rows_arr.value(i) as usize,
                stats_json: stats_arr.value(i).to_string(),
                row_groups_json: row_groups_arr
                    .map(|arr| arr.value(i).to_string())
                    .unwrap_or_else(|| "[]".to_string()),
                content_hash: content_hash_arr
                    .and_then(|array| (!array.is_null(i)).then(|| array.value(i).to_string())),
            });
        }

        Ok(Self::new_with_options(entries, options))
    }

    pub fn locate_global_row(
        &self,
        global_row: usize,
    ) -> Result<Option<GlobalRowLocation>, BazanError> {
        let Some(entry) = self.entries.iter().find(|entry| {
            global_row >= entry.first_global_row
                && global_row < entry.first_global_row.saturating_add(entry.total_rows)
        }) else {
            return Ok(None);
        };

        let file_offset = global_row.saturating_sub(entry.first_global_row);
        let row_groups: Vec<RowGroupLocation> = serde_json::from_str(&entry.row_groups_json)?;
        let Some(group) = row_groups.iter().find(|group| {
            file_offset >= group.first_row
                && file_offset < group.first_row.saturating_add(group.row_count)
        }) else {
            return Ok(Some(GlobalRowLocation {
                rel_path: entry.rel_path.clone(),
                file_offset,
                row_group: None,
                row_in_group: None,
                page_indexed: false,
            }));
        };

        Ok(Some(GlobalRowLocation {
            rel_path: entry.rel_path.clone(),
            file_offset,
            row_group: Some(group.ordinal),
            row_in_group: Some(file_offset.saturating_sub(group.first_row)),
            page_indexed: group.columns.iter().any(|column| !column.pages.is_empty()),
        }))
    }
}

fn is_parquet_path(file_path: &Path) -> bool {
    matches!(
        file_path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
            .as_deref(),
        Some("parquet") | Some("pq")
    )
}

fn is_ndjson_path(file_path: &Path) -> bool {
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

fn is_json_array_path(file_path: &Path) -> bool {
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

fn is_orc_path(file_path: &Path) -> bool {
    file_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("orc"))
}

fn is_avro_path(file_path: &Path) -> bool {
    file_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("avro"))
}

fn is_msgpack_path(file_path: &Path) -> bool {
    file_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("msgpack"))
}

fn is_xlsx_path(file_path: &Path) -> bool {
    file_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("xlsx"))
}

fn is_arrow_ipc_path(file_path: &Path) -> bool {
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

fn delimited_delimiter(file_path: &Path) -> Option<u8> {
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
fn inspect_file_entry(
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

fn blake3_file_hash(file_path: &Path) -> Result<String, BazanError> {
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

/// Build full LakeMap for a directory in parallel using Rayon with live progress bar
pub fn build_lake_map(dir_path: &Path) -> Result<LakeMap, BazanError> {
    build_lake_map_with_progress(dir_path, true)
}

/// Build full LakeMap for a directory with configurable live progress bar
pub fn build_lake_map_with_progress(
    dir_path: &Path,
    show_progress: bool,
) -> Result<LakeMap, BazanError> {
    build_lake_map_with_options(dir_path, show_progress, LakeMapOptions::default())
}

pub fn build_lake_map_with_options(
    dir_path: &Path,
    show_progress: bool,
    options: LakeMapOptions,
) -> Result<LakeMap, BazanError> {
    if !dir_path.exists() || !dir_path.is_dir() {
        return Err(BazanError::Message(format!(
            "Directory does not exist: {:?}",
            dir_path
        )));
    }

    let files = discover_data_files(dir_path, None)?;
    if files.is_empty() {
        return Ok(LakeMap::new_with_options(Vec::new(), options));
    }

    // Filter out existing map file itself and collect initial file sizes
    let valid_files_with_size: Vec<(PathBuf, u64)> = files
        .into_iter()
        .filter(|p| !is_map_sidecar(p))
        .map(|p| {
            let size = fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            (p, size)
        })
        .collect();

    let total_files = valid_files_with_size.len();
    let total_bytes: u64 = valid_files_with_size.iter().map(|(_, s)| *s).sum();

    let tracker = Arc::new(MapProgressTracker::new(
        total_files,
        total_bytes,
        show_progress,
    ));

    let entries: Result<Vec<LakeMapEntry>, BazanError> = valid_files_with_size
        .par_iter()
        .map(|(file, size)| {
            let res = inspect_file_entry(dir_path, file, &options);
            tracker.inc(*size);
            res
        })
        .collect();

    Ok(LakeMap::new_with_options(entries?, options))
}

/// Save LakeMap to Arrow IPC payload format (`.br_map.bazan`).
/// Uses atomic write-to-temp-and-rename to prevent corrupting open mmaps (avoiding SIGBUS)
pub fn save_lake_map_ipc(map: &LakeMap, output_path: &Path) -> Result<(), BazanError> {
    let output_path = crate::utils::validate_safe_path(output_path)?;
    let output_path = output_path.as_path();

    let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;

    let batch = map.to_record_batch()?;

    // Write to a unique temporary file in the same directory for atomic rename
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp_file_name = format!(
        ".{}.tmp.{}_{}",
        output_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("br_map"),
        pid,
        nanos
    );
    let tmp_path = parent.join(tmp_file_name);

    let write_res = (|| -> Result<(), BazanError> {
        let file = File::create(&tmp_path)?;
        let mut writer = FileWriter::try_new(file, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;
        Ok(())
    })();

    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    // Atomic rename replaces directory entry without truncating active mmaps
    if let Err(e) = fs::rename(&tmp_path, output_path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(BazanError::Io(e));
    }

    Ok(())
}

/// Load LakeMap from an Arrow IPC binary file using memory-mapped zero-copy I/O in < 0.05ms
pub fn load_lake_map_ipc(input_path: &Path) -> Result<LakeMap, BazanError> {
    let input_path = crate::utils::validate_safe_path(input_path)?;
    let file = File::open(&input_path)?;
    // Use OS memory-mapping for instant, zero-syscall virtual memory access
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let cursor = std::io::Cursor::new(mmap);
    let reader = FileReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    for batch_res in reader {
        batches.push(batch_res?);
    }

    if batches.is_empty() {
        return Ok(LakeMap::new(Vec::new()));
    }

    let schema = batches[0].schema();
    let unified_batch = arrow::compute::concat_batches(&schema, &batches)?;
    LakeMap::from_record_batch(&unified_batch)
}

/// Resolve the current map path used for writes: `dir/.br_map.bazan`.
pub fn resolve_map_path(dir_path: &Path) -> PathBuf {
    dir_path.join(DEFAULT_MAP_FILENAME)
}

fn is_map_sidecar(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some(DEFAULT_MAP_FILENAME | LEGACY_MAP_FILENAME)
    )
}

/// Find the nearest healthy map entry for a data file.
fn resolve_healthy_map_entry(file_path: &Path) -> Result<Option<LakeMapEntry>, BazanError> {
    let Some(mut map_root) = file_path.parent() else {
        return Ok(None);
    };

    loop {
        let map_path = resolve_map_path(map_root);
        if map_path.is_file() {
            let map = load_lake_map_ipc(&map_path)?;
            let Some(rel_path) = file_path.strip_prefix(map_root).ok() else {
                return Ok(None);
            };
            let rel_path = rel_path.to_string_lossy();
            let Some(entry) = map.entries.iter().find(|entry| entry.rel_path == rel_path) else {
                return Ok(None);
            };

            let metadata = fs::metadata(file_path)?;
            let mtime_ms = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_millis() as i64)
                .unwrap_or(0);
            if metadata.len() != entry.size_bytes || mtime_ms != entry.mtime_ms {
                return Ok(None);
            }

            return Ok(Some(entry.clone()));
        }

        let Some(parent) = map_root.parent() else {
            break;
        };
        if parent == map_root {
            break;
        }
        map_root = parent;
    }

    Ok(None)
}

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

/// Diagnose data lake map consistency and optionally auto-heal incremental drifts
pub fn doctor_lake_map(dir_path: &Path, auto_heal: bool) -> Result<DoctorReport, BazanError> {
    let map_file = resolve_map_path(dir_path);
    let mut existing_map = if map_file.is_file() {
        Some(load_lake_map_ipc(&map_file)?)
    } else {
        None
    };
    let options = existing_map
        .as_ref()
        .map(|map| map.options.clone())
        .unwrap_or_default();

    let files_on_disk = discover_data_files(dir_path, None)?;
    let valid_disk_files: Vec<PathBuf> = files_on_disk
        .into_iter()
        .filter(|p| !is_map_sidecar(p))
        .collect();

    let mut disk_map: HashMap<String, (PathBuf, u64, i64)> = HashMap::new();
    for p in &valid_disk_files {
        let rel = p
            .strip_prefix(dir_path)
            .unwrap_or(p)
            .to_string_lossy()
            .to_string();
        if let Ok(meta) = fs::metadata(p) {
            let size = meta.len();
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            disk_map.insert(rel, (p.clone(), size, mtime));
        }
    }

    let mut healthy_count = 0usize;
    let mut modified_files = Vec::new();
    let mut missing_files = Vec::new();
    let mut unindexed_files = Vec::new();

    let mut retained_entries: Vec<LakeMapEntry> = Vec::new();
    let mut indexed_rel_paths = HashSet::new();

    if let Some(map) = existing_map.take() {
        for entry in map.entries {
            indexed_rel_paths.insert(entry.rel_path.clone());
            if let Some((full_path, disk_size, disk_mtime)) = disk_map.get(&entry.rel_path) {
                let metadata_matches =
                    *disk_size == entry.size_bytes && *disk_mtime == entry.mtime_ms;
                let healthy = if metadata_matches {
                    match &options.fingerprint {
                        FingerprintPolicy::Metadata => true,
                        FingerprintPolicy::Blake3 => match entry.content_hash.as_deref() {
                            Some(expected) => blake3_file_hash(full_path)? == expected,
                            None => false,
                        },
                    }
                } else {
                    false
                };
                if healthy {
                    healthy_count += 1;
                    retained_entries.push(entry);
                } else {
                    modified_files.push(entry.rel_path.clone());
                }
            } else {
                missing_files.push(entry.rel_path.clone());
            }
        }
    }

    for rel_path in disk_map.keys() {
        if !indexed_rel_paths.contains(rel_path) {
            unindexed_files.push(rel_path.clone());
        }
    }

    let is_drifted = !modified_files.is_empty()
        || !missing_files.is_empty()
        || !unindexed_files.is_empty()
        || !map_file.is_file();

    let mut healed = false;

    if auto_heal && is_drifted {
        // Incremental re-index: inspect only modified & unindexed files
        let files_to_reindex: Vec<(PathBuf, u64)> = modified_files
            .iter()
            .chain(unindexed_files.iter())
            .filter_map(|rel| disk_map.get(rel).map(|(p, size, _)| (p.clone(), *size)))
            .collect();

        let total_heal_bytes: u64 = files_to_reindex.iter().map(|(_, s)| *s).sum();
        let tracker = Arc::new(MapProgressTracker::new(
            files_to_reindex.len(),
            total_heal_bytes,
            true,
        ));

        let new_entries: Result<Vec<LakeMapEntry>, BazanError> = files_to_reindex
            .par_iter()
            .map(|(p, size)| {
                let res = inspect_file_entry(dir_path, p, &options);
                tracker.inc(*size);
                res
            })
            .collect();

        retained_entries.extend(new_entries?);
        retained_entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));

        let healed_map = LakeMap::new_with_options(retained_entries, options);
        save_lake_map_ipc(&healed_map, &map_file)?;
        healed = true;
    }

    let status = if !is_drifted {
        "HEALTHY".to_string()
    } else if healed {
        "HEALED".to_string()
    } else {
        "DRIFT_DETECTED".to_string()
    };

    Ok(DoctorReport {
        status,
        total_files: disk_map.len(),
        healthy_count,
        modified_files,
        unindexed_files,
        missing_files,
        healed,
    })
}

impl MatrixEngine {
    /// Create or rebuild peer LakeMap `.br_map.bazan` for `dir_path`.
    pub fn create_lake_map_native(
        &self,
        dir_path: &str,
        show_progress: bool,
    ) -> Result<String, BazanError> {
        self.create_lake_map_native_with_options(dir_path, show_progress, LakeMapOptions::default())
    }

    pub fn create_lake_map_native_with_options(
        &self,
        dir_path: &str,
        show_progress: bool,
        options: LakeMapOptions,
    ) -> Result<String, BazanError> {
        let path = Path::new(dir_path);
        let map = build_lake_map_with_options(path, show_progress, options)?;
        let out_file = resolve_map_path(path);
        save_lake_map_ipc(&map, &out_file)?;
        Ok(out_file.to_string_lossy().to_string())
    }

    pub fn locate_lake_row_native(
        &self,
        dir_path: &str,
        global_row: usize,
    ) -> Result<Option<GlobalRowLocation>, BazanError> {
        let map = load_lake_map_ipc(&resolve_map_path(Path::new(dir_path)))?;
        map.locate_global_row(global_row)
    }

    /// Run doctor health check and optional auto-healing sync
    pub fn doctor_lake_map_native(
        &self,
        dir_path: &str,
        auto_heal: bool,
    ) -> Result<DoctorReport, BazanError> {
        doctor_lake_map(Path::new(dir_path), auto_heal)
    }
}
