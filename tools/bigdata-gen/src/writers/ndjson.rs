use std::fs::File;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_json::WriterBuilder as JsonWriterBuilder;

use crate::gen::{chunk_iter, schema};
use crate::progress::ProgressItem;

/// Newline Delimited JSON stream format (1 complete JSON object per line)
pub fn write_ndjson_stream(path: &str, seed: u64, total: u64, cols: usize, progress: &ProgressItem) -> Result<()> {
    let file = File::create(path)?;
    let sch = schema(cols);
    let mut writer: arrow_json::Writer<_, arrow_json::writer::LineDelimited> =
        JsonWriterBuilder::new().build(file);

    for batch in chunk_iter(seed, total, cols) {

        let n = batch.num_rows();
        let batch = cast_for_json(&batch, &sch);
        writer.write(&batch)?;
        progress.add_rows(n as u64);
        if let Ok(md) = std::fs::metadata(path) {
            progress.set_bytes(md.len());
        }
    }
    writer.finish()?;
    if let Ok(md) = std::fs::metadata(path) {
        progress.set_bytes(md.len());
    }
    Ok(())
}

fn cast_for_json(batch: &RecordBatch, sch: &SchemaRef) -> RecordBatch {
    let cols: Vec<Arc<dyn Array>> = sch
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let arr = batch.column(i);
            if arr.data_type() != f.data_type() {
                arrow::compute::kernels::cast::cast(arr, f.data_type()).unwrap()
            } else {
                arr.clone()
            }
        })
        .collect();
    RecordBatch::try_new(sch.clone(), cols).unwrap()
}
