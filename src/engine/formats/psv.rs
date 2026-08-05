use super::{FormatHandler, OpenedSource};
use crate::engine::formats::csv::open_delimited_csv;
use crate::error::BazanError;

/// PSV Streaming In-Memory Reader (Pipe-Separated Values)
pub struct PsvHandler;

impl FormatHandler for PsvHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        open_delimited_csv(file_path, batch_size, b'|')
    }
}
