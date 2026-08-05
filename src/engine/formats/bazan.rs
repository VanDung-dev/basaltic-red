use std::path::Path;
use std::sync::Arc;

use super::{FormatHandler, OpenedSource};
use crate::engine::container::{read_bazan_entry_batch, read_bazan_manifest};
use crate::error::BazanError;

/// `.bazan` container reader. A container is a single file holding many packed
/// tables, so `open()` exposes all entries as one concatenated stream of
/// batches (the same "single table" view `execute_sql` uses).
pub struct BazanHandler;

impl FormatHandler for BazanHandler {
    fn open(&self, file_path: &str, _batch_size: usize) -> Result<OpenedSource, BazanError> {
        let path = Path::new(file_path);
        let manifest = read_bazan_manifest(path)?;

        if manifest.entries.is_empty() {
            return Err(BazanError::Message(format!(
                "No entries found inside .bazan container: '{}'",
                file_path
            )));
        }

        let mut batches = Vec::with_capacity(manifest.entries.len());
        for entry in &manifest.entries {
            batches.push(read_bazan_entry_batch(path, entry)?);
        }

        let schema = batches[0].schema();
        Ok(OpenedSource {
            schema: Arc::clone(&schema),
            batches: Box::new(batches.into_iter().map(Ok)),
        })
    }
}
