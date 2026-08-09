use super::{clamp_batch_size, FormatHandler, OpenedSource};
use crate::error::BazanError;
use arrow_schema::Schema;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::Arc;

/// Formatted Pretty Printed JSON Array Reader (Multi-line formatted JSON [ {\n  "id": 1 ... \n} ])
pub struct JsonHandler;

impl FormatHandler for JsonHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let batch_size = clamp_batch_size(batch_size);
        // First attempt native Arrow JSON reader
        let file = File::open(file_path)?;
        let mut buf_reader = BufReader::new(file);

        if let Ok((schema, _)) = arrow_json::reader::infer_json_schema(&mut buf_reader, Some(100)) {
            let file_for_reader = File::open(file_path)?;
            let buf_reader_2 = BufReader::new(file_for_reader);

            if let Ok(reader) = arrow_json::ReaderBuilder::new(Arc::new(schema.clone()))
                .with_batch_size(batch_size)
                .build(buf_reader_2)
            {
                return Ok(OpenedSource {
                    schema: Arc::new(schema),
                    batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
                });
            }
        }

        // Fallback: stream a top-level JSON array `[ {...}, {...} ]` in a single pass.
        open_json_array(file_path, batch_size)
    }
}

/// Streaming adapter that presents the elements of a top-level JSON array
/// (`[ {...}, {...} ]`, compact or multi-line) as a bare object stream
/// `{...} {...}`: the surrounding brackets and the top-level `,` separators
/// are stripped. Single pass, O(batch) memory, no full-file DOM. arrow-json's
/// tape decoder parses back-to-back values, so no separator is required.
pub(crate) struct JsonArrayStream<R: Read> {
    inner: R,
    buffer: [u8; 8192],
    filled: usize,
    pos: usize,
    started: bool,
    finished: bool,
    in_string: bool,
    escaped: bool,
    depth: usize,
}

impl<R: Read> JsonArrayStream<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            buffer: [0; 8192],
            filled: 0,
            pos: 0,
            started: false,
            finished: false,
            in_string: false,
            escaped: false,
            depth: 0,
        }
    }
}

/// Shared state machine for `filter`; fields are passed disjoint so the chunk
/// buffer (which aliases `JsonArrayStream.buffer`) can be borrowed separately.
fn filter_chunk(
    started: &mut bool,
    finished: &mut bool,
    in_string: &mut bool,
    escaped: &mut bool,
    depth: &mut usize,
    buf: &mut [u8],
) -> usize {
    let mut out = 0usize;
    let mut i = 0usize;
    while i < buf.len() {
        let b = buf[i];
        i += 1;

        if !*started {
            match b {
                b' ' | b'\t' | b'\r' | b'\n' => continue,
                b'[' => {
                    *started = true;
                    continue;
                }
                _ => *started = true, // not an array: pass through, parser errors
            }
        }
        if *finished {
            break;
        }

        if *in_string {
            buf[out] = b;
            out += 1;
            if *escaped {
                *escaped = false;
            } else if b == b'\\' {
                *escaped = true;
            } else if b == b'"' {
                *in_string = false;
            }
            continue;
        }

        match b {
            b'"' => {
                *in_string = true;
                buf[out] = b;
                out += 1;
            }
            b'[' | b'{' => {
                *depth += 1;
                buf[out] = b;
                out += 1;
            }
            b']' | b'}' => {
                if *depth > 0 {
                    *depth -= 1;
                    buf[out] = b;
                    out += 1;
                } else if b == b']' {
                    *finished = true;
                } else {
                    // stray top-level `}`: keep it, let the parser fail
                    buf[out] = b;
                    out += 1;
                }
            }
            b',' => {
                if *depth > 0 {
                    buf[out] = b;
                    out += 1;
                }
                // top-level `,` (element separator) is dropped
            }
            _ => {
                buf[out] = b;
                out += 1;
            }
        }
    }
    out
}

impl<R: Read> Read for JsonArrayStream<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.pos < self.filled {
                let n = (self.filled - self.pos).min(out.len());
                out[..n].copy_from_slice(&self.buffer[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            // Drain the buffer before honouring `finished`: filter_chunk may set
            // `finished` (closing `]`) in the same chunk that still holds data.
            if self.finished {
                return Ok(0);
            }
            let n = self.inner.read(&mut self.buffer)?;
            if n == 0 {
                self.finished = true;
                return Ok(0);
            }
            self.filled = filter_chunk(
                &mut self.started,
                &mut self.finished,
                &mut self.in_string,
                &mut self.escaped,
                &mut self.depth,
                &mut self.buffer[..n],
            );
            self.pos = 0;
            if self.filled == 0 {
                continue; // chunk was entirely separators — keep reading
            }
        }
    }
}

/// Open a JSON array (`[{...},{...}]`, compact or multi-line) as a streaming
/// single-pass cursor. Memory is O(batch), independent of file size.
pub(crate) fn open_json_array(
    file_path: &str,
    batch_size: usize,
) -> Result<OpenedSource, BazanError> {
    let batch_size = clamp_batch_size(batch_size);

    // Schema inference: stream the first 100 elements through the same adapter
    // (serde yields each top-level value of the stripped object stream).
    let file = File::open(file_path)?;
    let stream = JsonArrayStream::new(BufReader::new(file));
    let deser = serde_json::Deserializer::from_reader(stream);
    let values = deser
        .into_iter::<serde_json::Value>()
        .take(100)
        .map(|r| r.map_err(|e| arrow::error::ArrowError::JsonError(e.to_string())));
    let schema = arrow_json::reader::infer_json_schema_from_iterator(values)?;

    if schema.fields().is_empty() {
        return Ok(OpenedSource {
            schema: Arc::new(Schema::empty()),
            batches: Box::new(std::iter::empty()),
        });
    }

    // Streaming read from a fresh handle.
    let file = File::open(file_path)?;
    let stream = BufReader::new(JsonArrayStream::new(file));
    let reader = arrow_json::ReaderBuilder::new(Arc::new(schema.clone()))
        .with_batch_size(batch_size)
        .build(stream)?;

    Ok(OpenedSource {
        schema: Arc::new(schema),
        batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_all<R: Read>(mut r: R) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buf = [0u8; 7];
        loop {
            match r.read(&mut buf).unwrap() {
                0 => break,
                n => out.extend_from_slice(&buf[..n]),
            }
        }
        out
    }

    #[test]
    fn strips_top_level_array() {
        let input = "[{\"a\":1},\n  {\"a\":2,\"s\":\"x,]\"}, {\"a\":3}]";
        let s = JsonArrayStream::new(input.as_bytes());
        assert_eq!(
            read_all(s),
            b"{\"a\":1}\n  {\"a\":2,\"s\":\"x,]\"} {\"a\":3}"
        );
    }

    #[test]
    fn open_json_array_streams_rows() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("basaltic_json_stream_{}.json", std::process::id()));
        std::fs::write(
            &path,
            "[{\"passenger_count\":1,\"fare_amount\":15.5,\"trip_distance\":2.5},{\"passenger_count\":0,\"fare_amount\":-5.0,\"trip_distance\":0.0}]",
        )
        .unwrap();
        let src = open_json_array(path.to_str().unwrap(), 1024).unwrap();
        assert_eq!(src.schema.fields().len(), 3);
        let rows: usize = src.batches.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 2);
        let _ = std::fs::remove_file(&path);
    }
}
