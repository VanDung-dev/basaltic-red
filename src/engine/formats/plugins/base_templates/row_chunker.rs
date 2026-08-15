use std::sync::Arc;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use crate::error::BazanError;

/// Lazily chunk a stream of row-values into RecordBatches of at most `batch_size`.
/// Used by row-oriented handlers (avro, msgpack, xlsx) whose readers are not
/// arrow iterators.
pub struct RowChunker<I, T, F> {
    rows: I,
    buffer: Vec<T>,
    batch_size: usize,
    schema: Arc<Schema>,
    convert: F,
}

impl<I, T, F> RowChunker<I, T, F> {
    pub fn new(rows: I, batch_size: usize, schema: Arc<Schema>, convert: F) -> Self {
        Self {
            rows,
            buffer: Vec::with_capacity(batch_size.min(1024)),
            batch_size,
            schema,
            convert,
        }
    }
}

impl<I, T, F> Iterator for RowChunker<I, T, F>
where
    I: Iterator<Item = Result<T, BazanError>>,
    F: Fn(&[T], &Arc<Schema>) -> Result<RecordBatch, BazanError>,
{
    type Item = Result<RecordBatch, BazanError>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.buffer.len() < self.batch_size {
            match self.rows.next() {
                Some(Ok(row)) => self.buffer.push(row),
                Some(Err(e)) => return Some(Err(e)),
                None => break,
            }
        }
        if self.buffer.is_empty() {
            return None;
        }
        let result = (self.convert)(&self.buffer, &self.schema);
        self.buffer.clear();
        Some(result)
    }
}
