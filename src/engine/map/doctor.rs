use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use rayon::prelude::*;

use crate::error::BazanError;
use crate::utils::discover_data_files;

use super::{
    blake3_file_hash, inspect_file_entry, is_map_sidecar, load_lake_map_ipc, resolve_map_path,
    save_lake_map_ipc, DoctorReport, FingerprintPolicy, LakeMap, LakeMapEntry, MapProgressTracker,
};

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
