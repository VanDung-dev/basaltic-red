use std::fs::File;
use anyhow::Result;
use arrow_csv::WriterBuilder as CsvWriterBuilder;

use crate::gen::chunk_iter;
use crate::progress::ProgressItem;

/// Generates TSV format using tab '\t' delimiter
pub fn write_tsv(path: &str, seed: u64, total: u64, cols: usize, progress: &ProgressItem) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = CsvWriterBuilder::new()
        .with_delimiter(b'\t')
        .with_header(true)
        .build(file);

    for batch in chunk_iter(seed, total, cols) {

        let n = batch.num_rows();
        writer.write(&batch)?;
        progress.add_rows(n as u64);
        if let Ok(md) = std::fs::metadata(path) {
            progress.set_bytes(md.len());
        }
    }
    drop(writer);
    if let Ok(md) = std::fs::metadata(path) {
        progress.set_bytes(md.len());
    }
    Ok(())
}
