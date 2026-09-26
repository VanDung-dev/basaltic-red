use std::io::Seek;

use crate::engine::formats::common::csv::build_delimited_source;
use crate::engine::formats::{FormatHandler, OpenedSource};
use crate::error::BazanError;

/// Base Template for custom delimited formats (e.g. `|`, `~`, `;`, `^`, tab, custom char).
#[derive(Debug, Clone)]
pub struct DelimitedFormatHandler {
    pub delimiter: u8,
    pub has_header: bool,
}

impl DelimitedFormatHandler {
    pub fn new(delimiter: u8, has_header: bool) -> Self {
        Self {
            delimiter,
            has_header,
        }
    }
}

impl FormatHandler for DelimitedFormatHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let mut file = std::fs::File::open(file_path)?;
        let format = arrow::csv::reader::Format::default()
            .with_delimiter(self.delimiter)
            .with_header(self.has_header);

        let (schema, _) = format.infer_schema(&mut file, Some(100))?;
        let _ = file.rewind();

        build_delimited_source(
            file,
            schema,
            batch_size,
            self.delimiter,
            self.has_header,
            None,
        )
    }

    fn open_with_columns(
        &self,
        file_path: &str,
        batch_size: usize,
        columns: &[String],
    ) -> Result<OpenedSource, BazanError> {
        let mut file = std::fs::File::open(file_path)?;
        let format = arrow::csv::reader::Format::default()
            .with_delimiter(self.delimiter)
            .with_header(self.has_header);

        let (schema, _) = format.infer_schema(&mut file, Some(100))?;
        let _ = file.rewind();

        let mut indices = Vec::new();
        for name in columns {
            indices.push(schema.index_of(name).map_err(|_| {
                BazanError::Message(format!("Column '{}' not found in schema", name))
            })?);
        }

        build_delimited_source(
            file,
            schema,
            batch_size,
            self.delimiter,
            self.has_header,
            Some(indices),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headerless_open_includes_first_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.txt");
        std::fs::write(&path, "alice|31\nbob|42\n").unwrap();

        let source = DelimitedFormatHandler::new(b'|', false)
            .open(path.to_str().unwrap(), 1)
            .unwrap();
        let batches = source.batches.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
    }
}
