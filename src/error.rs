use thiserror::Error;

/// Unified error taxonomy for the engine, CLI and Python SDK.
///
/// Every layer reports failures through this single type so error messages and
/// conversion behavior stay consistent. External crate errors are mapped with
/// `#[from]`; the Python boundary maps each variant to the matching Python
/// exception in one place (`engine::MatrixEngine` pyo3 methods).
#[derive(Debug, Error)]
pub enum BazanError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Avro error: {0}")]
    Avro(#[from] apache_avro::Error),
    #[error("Excel error: {0}")]
    Excel(#[from] calamine::XlsxError),
    #[error("Regex error: {0}")]
    Regex(#[from] regex::Error),
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("Glob pattern error: {0}")]
    GlobPattern(#[from] glob::PatternError),
    #[error("Glob walk error: {0}")]
    GlobWalk(#[from] glob::GlobError),
    #[error("Thread pool error: {0}")]
    ThreadPool(#[from] rayon::ThreadPoolBuildError),
    #[error("unsupported file format: .{0}")]
    UnsupportedFormat(String),
    #[error("{0}")]
    Message(String),
}
