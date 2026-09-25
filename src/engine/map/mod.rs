use crate::engine::MatrixEngine;
use crate::error::BazanError;
use std::path::Path;

mod doctor;
mod inspect;
mod model;
mod ranges;
mod storage;

pub use doctor::doctor_lake_map;
pub use model::*;
pub use ranges::*;
pub use storage::*;

use inspect::{
    blake3_file_hash, delimited_delimiter, inspect_file_entry, is_arrow_ipc_path, is_avro_path,
    is_json_array_path, is_msgpack_path, is_ndjson_path, is_orc_path, is_parquet_path,
    is_xlsx_path,
};
use storage::{is_map_sidecar, resolve_healthy_map_entry};

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
