use super::{FormatHandler, OpenedSource};
use crate::engine::formats::csv::open_parquet;
use crate::error::BazanError;

/// Apache ORC Columnar Streaming Reader (using Parquet/Arrow Reader interface)
pub struct OrcHandler;

impl FormatHandler for OrcHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        open_parquet(file_path, batch_size)
    }
}
