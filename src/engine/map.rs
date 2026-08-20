use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::engine::formats::resolve_handler_for_file;
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use crate::utils::discover_data_files;

pub const DEFAULT_MAP_FILENAME: &str = ".br_map.ipc";

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

#[derive(Debug, Clone)]
pub struct LakeMapEntry {
    pub rel_path: String,
    pub size_bytes: u64,
    pub mtime_ms: i64,
    pub total_rows: usize,
    pub stats_json: String,
}

#[derive(Debug, Clone)]
pub struct LakeMap {
    pub entries: Vec<LakeMapEntry>,
    pub total_files: usize,
    pub total_rows: usize,
    pub total_bytes: u64,
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

impl LakeMap {
    pub fn new(entries: Vec<LakeMapEntry>) -> Self {
        let total_files = entries.len();
        let total_rows = entries.iter().map(|e| e.total_rows).sum();
        let total_bytes = entries.iter().map(|e| e.size_bytes).sum();
        Self {
            entries,
            total_files,
            total_rows,
            total_bytes,
        }
    }

    /// Convert LakeMap into an Arrow RecordBatch for Zero-Copy IPC serialization
    pub fn to_record_batch(&self) -> Result<RecordBatch, BazanError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("rel_path", DataType::Utf8, false),
            Field::new("size_bytes", DataType::UInt64, false),
            Field::new("mtime_ms", DataType::Int64, false),
            Field::new("total_rows", DataType::UInt64, false),
            Field::new("stats_json", DataType::Utf8, false),
        ]));

        let rel_paths: Vec<&str> = self.entries.iter().map(|e| e.rel_path.as_str()).collect();
        let sizes: Vec<u64> = self.entries.iter().map(|e| e.size_bytes).collect();
        let mtimes: Vec<i64> = self.entries.iter().map(|e| e.mtime_ms).collect();
        let rows: Vec<u64> = self.entries.iter().map(|e| e.total_rows as u64).collect();
        let stats: Vec<&str> = self.entries.iter().map(|e| e.stats_json.as_str()).collect();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(rel_paths)),
            Arc::new(UInt64Array::from(sizes)),
            Arc::new(Int64Array::from(mtimes)),
            Arc::new(UInt64Array::from(rows)),
            Arc::new(StringArray::from(stats)),
        ];

        RecordBatch::try_new(schema, columns).map_err(BazanError::from)
    }

    /// Load LakeMap from an Arrow RecordBatch
    pub fn from_record_batch(batch: &RecordBatch) -> Result<Self, BazanError> {
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

        let num_rows = batch.num_rows();
        let mut entries = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            entries.push(LakeMapEntry {
                rel_path: rel_path_arr.value(i).to_string(),
                size_bytes: size_arr.value(i),
                mtime_ms: mtime_arr.value(i),
                total_rows: rows_arr.value(i) as usize,
                stats_json: stats_arr.value(i).to_string(),
            });
        }

        Ok(Self::new(entries))
    }
}

/// Helper to extract stats and row count from a single data file
fn inspect_file_entry(root_dir: &Path, file_path: &Path) -> Result<LakeMapEntry, BazanError> {
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

    let file_str = file_path.to_str().unwrap_or("");
    let handler = resolve_handler_for_file(file_str).ok_or_else(|| {
        BazanError::Message(format!("Unsupported format for map inspection: {}", file_str))
    })?;

    let source = handler.open(file_str, 64 * 1024)?;
    let mut total_rows = 0usize;
    let mut col_stats: HashMap<String, ColumnMinMax> = HashMap::new();

    for batch_res in source.batches {
        let batch = batch_res?;
        let batch_rows = batch.num_rows();
        total_rows += batch_rows;

        // Extract sample min/max from first non-empty batch for fast pruning
        if col_stats.is_empty() && batch_rows > 0 {
            for field in batch.schema().fields() {
                let name = field.name().clone();
                let col = batch.column_by_name(&name);

                if let Some(col) = col {
                    match field.data_type() {
                        DataType::Int64 => {
                            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                                if let (Some(min), Some(max)) = (
                                    arrow::compute::kernels::aggregate::min(arr),
                                    arrow::compute::kernels::aggregate::max(arr),
                                ) {
                                    col_stats.insert(
                                        name,
                                        ColumnMinMax {
                                            min: Some(min as f64),
                                            max: Some(max as f64),
                                            min_str: None,
                                            max_str: None,
                                        },
                                    );
                                }
                            }
                        }
                        DataType::Float64 => {
                            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
                                if let (Some(min), Some(max)) = (
                                    arrow::compute::kernels::aggregate::min(arr),
                                    arrow::compute::kernels::aggregate::max(arr),
                                ) {
                                    col_stats.insert(
                                        name,
                                        ColumnMinMax {
                                            min: Some(min),
                                            max: Some(max),
                                            min_str: None,
                                            max_str: None,
                                        },
                                    );
                                }
                            }
                        }
                        DataType::Utf8 => {
                            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                                if let (Some(min), Some(max)) = (
                                    arrow::compute::kernels::aggregate::min_string(arr),
                                    arrow::compute::kernels::aggregate::max_string(arr),
                                ) {
                                    col_stats.insert(
                                        name,
                                        ColumnMinMax {
                                            min: None,
                                            max: None,
                                            min_str: Some(min.to_string()),
                                            max_str: Some(max.to_string()),
                                        },
                                    );
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    let stats = FileStats {
        total_rows,
        columns: col_stats,
    };
    let stats_json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());

    Ok(LakeMapEntry {
        rel_path: rel,
        size_bytes,
        mtime_ms,
        total_rows,
        stats_json,
    })
}

/// Build full LakeMap for a directory in parallel using Rayon
pub fn build_lake_map(dir_path: &Path) -> Result<LakeMap, BazanError> {
    if !dir_path.exists() || !dir_path.is_dir() {
        return Err(BazanError::Message(format!(
            "Directory does not exist: {:?}",
            dir_path
        )));
    }

    let files = discover_data_files(dir_path, None)?;
    if files.is_empty() {
        return Ok(LakeMap::new(Vec::new()));
    }

    // Filter out existing map file itself
    let valid_files: Vec<PathBuf> = files
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|s| !s.ends_with(".ipc") && !s.starts_with(".br_map"))
                .unwrap_or(true)
        })
        .collect();

    let entries: Vec<LakeMapEntry> = valid_files
        .par_iter()
        .filter_map(|file| inspect_file_entry(dir_path, file).ok())
        .collect();

    Ok(LakeMap::new(entries))
}

/// Save LakeMap to Arrow IPC binary format (`.br_map.ipc`)
pub fn save_lake_map_ipc(map: &LakeMap, output_path: &Path) -> Result<(), BazanError> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let batch = map.to_record_batch()?;
    let file = File::create(output_path)?;
    let mut writer = FileWriter::try_new(file, &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(())
}

/// Load LakeMap from an Arrow IPC binary file in < 1ms
pub fn load_lake_map_ipc(input_path: &Path) -> Result<LakeMap, BazanError> {
    let file = File::open(input_path)?;
    let reader = FileReader::try_new(file, None)?;
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

/// Resolve map path: either explicit or default peer file `dir/.br_map.ipc`
pub fn resolve_map_path(dir_path: &Path) -> PathBuf {
    dir_path.join(DEFAULT_MAP_FILENAME)
}

/// Diagnose data lake map consistency and optionally auto-heal incremental drifts
pub fn doctor_lake_map(dir_path: &Path, auto_heal: bool) -> Result<DoctorReport, BazanError> {
    let map_file = resolve_map_path(dir_path);
    let mut existing_map = if map_file.exists() {
        load_lake_map_ipc(&map_file).ok()
    } else {
        None
    };

    let files_on_disk = discover_data_files(dir_path, None)?;
    let valid_disk_files: Vec<PathBuf> = files_on_disk
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|s| !s.ends_with(".ipc") && !s.starts_with(".br_map"))
                .unwrap_or(true)
        })
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
            if let Some((_full_path, disk_size, disk_mtime)) = disk_map.get(&entry.rel_path) {
                if *disk_size == entry.size_bytes && *disk_mtime == entry.mtime_ms {
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

    for (rel_path, _) in &disk_map {
        if !indexed_rel_paths.contains(rel_path) {
            unindexed_files.push(rel_path.clone());
        }
    }

    let is_drifted = !modified_files.is_empty()
        || !missing_files.is_empty()
        || !unindexed_files.is_empty()
        || !map_file.exists();

    let mut healed = false;

    if auto_heal && is_drifted {
        // Incremental re-index: inspect only modified & unindexed files
        let files_to_reindex: Vec<PathBuf> = modified_files
            .iter()
            .chain(unindexed_files.iter())
            .filter_map(|rel| disk_map.get(rel).map(|(p, _, _)| p.clone()))
            .collect();

        let new_entries: Vec<LakeMapEntry> = files_to_reindex
            .par_iter()
            .filter_map(|p| inspect_file_entry(dir_path, p).ok())
            .collect();

        retained_entries.extend(new_entries);
        retained_entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));

        let healed_map = LakeMap::new(retained_entries);
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
    /// Create or rebuild peer Arrow IPC LakeMap `.br_map.ipc` for `dir_path`
    pub fn create_lake_map_native(&self, dir_path: &str) -> Result<String, BazanError> {
        let path = Path::new(dir_path);
        let map = build_lake_map(path)?;
        let out_file = resolve_map_path(path);
        save_lake_map_ipc(&map, &out_file)?;
        Ok(out_file.to_string_lossy().to_string())
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
