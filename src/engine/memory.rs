use std::sync::OnceLock;

/// Unified row budget for streaming reads. Matches the pre-existing single-stream
/// ceiling (`DEFAULT_MAX_BATCH_SIZE = 1 << 20`) while bounding N parallel
/// streams: with `n` streams each one allocates `budget_batch_rows(n)` rows, so
/// total in-flight rows stay within budget instead of N × 1<<20.
pub const BUDGET_BATCH_ROWS: usize = 1 << 20;

/// Per-stream batch rows when `n` streams run concurrently.
pub fn budget_batch_rows(n: usize) -> usize {
    (BUDGET_BATCH_ROWS / n.max(1)).clamp(64, BUDGET_BATCH_ROWS)
}

/// Process-wide tokio runtime, reused by every SQL call instead of spawning a
/// new Runtime per call.
pub fn global_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("failed to start tokio runtime"))
}
