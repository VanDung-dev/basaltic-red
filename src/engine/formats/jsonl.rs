use super::{FormatHandler, OpenedSource};
use crate::engine::formats::json::open_json_array;
use crate::error::BazanError;

/// JSONL Single-Line Compact JSON Array Reader ([{"id":1,...},{"id":2,...}])
pub struct JsonlHandler;

impl FormatHandler for JsonlHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        open_json_array(file_path, batch_size)
    }
}
