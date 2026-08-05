use super::{FormatHandler, OpenedSource};
use crate::engine::formats::csv::open_delimited_csv;
use crate::error::BazanError;

/// TXT Streaming In-Memory Reader (Semicolon-Separated Values)
pub struct TxtHandler;

impl FormatHandler for TxtHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        open_delimited_csv(file_path, batch_size, b';')
    }
}
