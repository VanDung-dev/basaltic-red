use anyhow::Result;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;

use crate::gen::{chunk_iter, schema};
use crate::progress::ProgressItem;

pub fn write_parquet(
    path: &str,
    seed: u64,
    total: u64,
    cols: usize,
    progress: &ProgressItem,
) -> Result<()> {
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();

    let sch = schema(cols);
    let mut writer = ArrowWriter::try_new(file, sch, Some(props))?;

    for batch in chunk_iter(seed, total, cols) {
        let n = batch.num_rows();
        writer.write(&batch)?;
        progress.add_rows(n as u64);
        if let Ok(md) = std::fs::metadata(path) {
            progress.set_bytes(md.len());
        }
    }
    writer.close()?;
    if let Ok(md) = std::fs::metadata(path) {
        progress.set_bytes(md.len());
    }
    Ok(())
}
