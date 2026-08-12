use std::sync::OnceLock;

/// Unified row budget for streaming reads. Matches the pre-existing single-stream
/// ceiling (`DEFAULT_MAX_BATCH_SIZE = 1 << 20`) while bounding N parallel
/// streams: with `n` streams each one allocates `budget_batch_rows(n)` rows, so
/// total in-flight rows stay within budget instead of N × 1<<20.
pub const BUDGET_BATCH_ROWS: usize = 1 << 20;

/// Safety factor for transient memory (mask + clean + trash buffers).
const SAFETY_FACTOR: f64 = 3.5;

/// Conservative average row byte estimate when the schema is not yet available
/// at the budget call site (used before the first batch is opened).
const EST_ROW_BYTES: f64 = 256.0;

/// Total RAM budget in bytes from `BASALTIC_RED_MAX_RAM_GB` (default 2 GB),
/// read once per process.
pub fn max_ram_bytes() -> usize {
    static MAX_RAM: OnceLock<usize> = OnceLock::new();
    *MAX_RAM.get_or_init(|| {
        let gb = std::env::var("BASALTIC_RED_MAX_RAM_GB")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(2.0);
        (gb * 1024.0 * 1024.0 * 1024.0) as usize
    })
}

/// Estimated bytes per row from a schema's field widths (sum over columns).
pub fn schema_row_bytes(schema: &arrow::datatypes::Schema) -> usize {
    let mut est = 0usize;
    for field in schema.fields() {
        est += field_bytes(field.data_type());
    }
    est.max(1)
}

fn field_bytes(dt: &arrow::datatypes::DataType) -> usize {
    use arrow::datatypes::DataType::*;
    match dt {
        Boolean | Int8 | Int16 | UInt8 | UInt16 | Null => 1,
        Int32 | UInt32 | Float32 | Date32 => 4,
        Int64 | UInt64 | Float64 | Date64 | Time32(_) | Time64(_) | Timestamp(_, _) | Duration(_) | Interval(_) => 8,
        Utf8 | LargeUtf8 | Binary | LargeBinary => 16,
        FixedSizeBinary(n) => (*n as usize).max(1),
        Decimal32(_, _) | Decimal64(_, _) => 8,
        Decimal128(_, _) => 16,
        Decimal256(_, _) => 32,
        List(f) | LargeList(f) => 16 + 8 * field_bytes(f.data_type()),
        FixedSizeList(f, n) => 16 + (*n as usize).max(1) * field_bytes(f.data_type()),
        Struct(fields) => fields.iter().map(|f| field_bytes(f.data_type())).sum(),
        Dictionary(_, v) => field_bytes(v),
        Map(_, _) | Union(_, _) | RunEndEncoded(_, _) => 32,
        Float16 | ListView(_) | LargeListView(_) | BinaryView | Utf8View => 16,
    }
}

/// Per-stream batch rows when `n` streams run concurrently, using a schema-based
/// row estimate when available (falls back to `EST_ROW_BYTES`).
///
/// `batch_bytes = budget / (n × safety)` then `batch_rows = batch_bytes / row_bytes`,
/// clamped to the single-stream ceiling `BUDGET_BATCH_ROWS`.
pub fn budget_batch_rows_for(n: usize, est_row_bytes: usize) -> usize {
    let streams = n.max(1);
    let batch_bytes = max_ram_bytes() as f64 / (streams as f64 * SAFETY_FACTOR);
    let rows = (batch_bytes / est_row_bytes.max(1) as f64) as usize;
    rows.clamp(64, BUDGET_BATCH_ROWS)
}

pub fn budget_batch_rows(n: usize) -> usize {
    budget_batch_rows_for(n, EST_ROW_BYTES as usize)
}

/// Process-wide tokio runtime, reused by every SQL call instead of spawning a
/// new Runtime per call.
pub fn global_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("failed to start tokio runtime"))
}

/// Process-wide rayon pool with `threads` workers (built once per distinct
/// thread count), so parallel jobs stop constructing a new pool per call.
pub fn global_rayon_pool(threads: usize) -> &'static rayon::ThreadPool {
    static POOLS: OnceLock<std::sync::Mutex<HashMap<usize, &'static rayon::ThreadPool>>> =
        OnceLock::new();
    use std::collections::HashMap;
    let mut pools = POOLS
        .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    pools
        .entry(threads)
        .or_insert_with(|| {
            // Leaked once per thread count: the pool is process-wide and lives
            // for the whole run.
            Box::leak(Box::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("failed to build rayon pool"),
            ))
        })
}
