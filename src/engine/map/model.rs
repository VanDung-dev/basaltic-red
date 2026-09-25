use std::collections::HashMap;
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::error::BazanError;

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
