pub mod avro;
pub mod csv;
pub mod feather;
pub mod json;
pub mod jsonl;
pub mod msgpack;
pub mod ndjson;
pub mod orc;
pub mod parquet;
pub mod psv;
pub mod tsv;
pub mod txt;
pub mod xlsx;

use anyhow::Result;
use std::sync::atomic::Ordering;

use crate::progress::ProgressItem;

pub fn run_format(
    fmt: &str,
    path: &str,
    seed: u64,
    total: u64,
    cols: usize,
    progress: &ProgressItem,
) -> Result<()> {
    progress.reset_started();
    match fmt {
        "csv" => csv::write_csv(path, seed, total, cols, progress)?,
        "tsv" => tsv::write_tsv(path, seed, total, cols, progress)?,
        "psv" => psv::write_psv(path, seed, total, cols, progress)?,
        "txt" => txt::write_txt(path, seed, total, cols, progress)?,
        "json" => json::write_json_pretty(path, seed, total, cols, progress)?,
        "jsonl" => jsonl::write_jsonl_single_line(path, seed, total, cols, progress)?,
        "ndjson" => ndjson::write_ndjson_stream(path, seed, total, cols, progress)?,
        "parquet" => parquet::write_parquet(path, seed, total, cols, progress)?,
        "feather" | "arrow" | "ipc" => feather::write_feather(path, seed, total, cols, progress)?,
        "avro" => avro::write_avro(path, seed, total, cols, progress)?,
        "xlsx" => xlsx::write_xlsx(path, seed, total, cols, progress)?,
        "orc" => orc::write_orc(path, seed, total, cols, progress)?,
        "msgpack" => msgpack::write_msgpack(path, seed, total, cols, progress)?,
        _ => anyhow::bail!("unsupported format: {}", fmt),
    }
    progress.mark_finished();
    progress.finished.store(true, Ordering::Relaxed);
    Ok(())
}
