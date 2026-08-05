use anyhow::Result;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;

use crate::gen::{chunk_iter, schema};
use crate::progress::ProgressItem;

pub fn write_orc(
    path: &str,
    seed: u64,
    total: u64,
    cols: usize,
    progress: &ProgressItem,
) -> Result<()> {
    // Note: ORC uses columnar storage similar to Parquet with ZSTD compression
    let file = File::create(path)?;
    let sch = schema(cols);
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();
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
