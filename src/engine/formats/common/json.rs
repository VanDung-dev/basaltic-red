use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::Schema;

use crate::engine::formats::{
    clamp_batch_size, read_range_from_source, FormatHandler, OpenedSource,
};
use crate::error::BazanError;

/// JSON array or newline-delimited object reader (Tier 2 Common)
#[derive(Debug, Clone, Copy, Default)]
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

/// JSONL extension reader supporting object-per-line input and legacy top-level arrays.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonlHandler;

impl FormatHandler for JsonlHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        open_json_array(file_path, batch_size)
    }
}

/// NDJSON Newline Delimited Stream Reader (1 complete JSON object per line, no outer array brackets)
#[derive(Debug, Clone, Copy, Default)]
pub struct NdjsonHandler;

fn infer_ndjson_schema(file_path: &str) -> Result<Schema, BazanError> {
    let file = File::open(file_path)?;
    let mut buf_reader = BufReader::new(file);
    Ok(arrow_json::reader::infer_json_schema_from_iterator(
        arrow_json::reader::ValueIter::new(&mut buf_reader, Some(100)),
    )?)
}

pub fn read_ndjson_range(
    file_path: &str,
    byte_offset: u64,
    offset: usize,
    limit: usize,
    batch_size: usize,
) -> Result<RecordBatch, BazanError> {
    let batch_size = clamp_batch_size(batch_size);
    let schema = infer_ndjson_schema(file_path)?;
    let mut file = File::open(file_path)?;
    file.seek(SeekFrom::Start(byte_offset))?;
    let reader = arrow_json::ReaderBuilder::new(Arc::new(schema.clone()))
        .with_batch_size(batch_size)
        .build(BufReader::new(file))?;

    read_range_from_source(
        OpenedSource {
            schema: Arc::new(schema),
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        },
        offset,
        limit,
    )
}

fn infer_json_array_schema(file_path: &str) -> Result<Schema, BazanError> {
    let file = File::open(file_path)?;
    let stream = JsonArrayStream::new(BufReader::new(file));
    let deser = serde_json::Deserializer::from_reader(stream);
    let values = deser
        .into_iter::<serde_json::Value>()
        .take(100)
        .map(|r| r.map_err(|e| arrow::error::ArrowError::JsonError(e.to_string())));
    Ok(arrow_json::reader::infer_json_schema_from_iterator(values)?)
}

pub fn read_json_array_range(
    file_path: &str,
    byte_offset: u64,
    offset: usize,
    limit: usize,
    batch_size: usize,
) -> Result<RecordBatch, BazanError> {
    let batch_size = clamp_batch_size(batch_size);
    let schema = infer_json_array_schema(file_path)?;
    let mut file = File::open(file_path)?;
    file.seek(SeekFrom::Start(byte_offset))?;
    let reader = arrow_json::ReaderBuilder::new(Arc::new(schema.clone()))
        .with_batch_size(batch_size)
        .build(BufReader::new(JsonArrayStream::from_offset(file)))?;

    read_range_from_source(
        OpenedSource {
            schema: Arc::new(schema),
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        },
        offset,
        limit,
    )
}

impl FormatHandler for NdjsonHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let batch_size = clamp_batch_size(batch_size);
        let schema = infer_ndjson_schema(file_path)?;

        let file_for_reader = File::open(file_path)?;
        let buf_reader_2 = BufReader::new(file_for_reader);

        let reader = arrow_json::ReaderBuilder::new(Arc::new(schema.clone()))
            .with_batch_size(batch_size)
            .build(buf_reader_2)?;

        Ok(OpenedSource {
            schema: Arc::new(schema),
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        })
    }
}

/// Streaming adapter that presents the elements of a top-level JSON array
/// (`[ {...}, {...} ]`, compact or multi-line) as a bare object stream
/// `{...} {...}`. It validates the outer brackets and comma separators, then
/// strips them. Single pass, O(batch) memory, no full-file DOM. arrow-json's
/// tape decoder parses back-to-back values, so no separator is required.
pub(crate) struct JsonArrayStream<R: Read> {
    inner: R,
    buffer: [u8; 8192],
    filled: usize,
    pos: usize,
    started: bool,
    finished: bool,
    array_started: bool,
    array_has_value: bool,
    array_expect_value: bool,
    trailer_validated: bool,
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
            array_started: false,
            array_has_value: false,
            array_expect_value: false,
            trailer_validated: false,
            in_string: false,
            escaped: false,
            depth: 0,
        }
    }

    pub(crate) fn from_offset(inner: R) -> Self {
        Self {
            inner,
            buffer: [0; 8192],
            filled: 0,
            pos: 0,
            started: true,
            finished: false,
            array_started: true,
            array_has_value: false,
            array_expect_value: true,
            trailer_validated: false,
            in_string: false,
            escaped: false,
            depth: 0,
        }
    }
}

fn filter_chunk(
    started: &mut bool,
    finished: &mut bool,
    in_string: &mut bool,
    escaped: &mut bool,
    depth: &mut usize,
    array_started: &mut bool,
    array_has_value: &mut bool,
    array_expect_value: &mut bool,
    buf: &mut [u8],
) -> io::Result<usize> {
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
                    *array_started = true;
                    *array_expect_value = true;
                    continue;
                }
                _ => *started = true, // not an array: pass through, parser errors
            }
        }
        if *finished {
            if *array_started && !matches!(b, b' ' | b'\t' | b'\r' | b'\n') {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "non-whitespace after top-level JSON array",
                ));
            }
            continue;
        }

        if *array_started && *depth == 0 && !*in_string {
            match b {
                b' ' | b'\t' | b'\r' | b'\n' => {
                    buf[out] = b;
                    out += 1;
                }
                b'{' if *array_expect_value => {
                    *array_expect_value = false;
                    *depth = 1;
                    buf[out] = b;
                    out += 1;
                }
                b',' if !*array_expect_value && *array_has_value => {
                    *array_expect_value = true;
                }
                b']' if !*array_expect_value || !*array_has_value => {
                    *finished = true;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "JSON array rows must be objects separated by commas",
                    ));
                }
            }
            continue;
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
                    if *depth == 0 && *array_started {
                        *array_has_value = true;
                    }
                } else if b == b']' && *array_started {
                    *finished = true;
                } else {
                    // Leave unmatched closers for the JSON parser to reject.
                    buf[out] = b;
                    out += 1;
                }
            }
            b',' => {
                if *depth > 0 || !*array_started {
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
    Ok(out)
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
            if self.finished {
                if self.array_started && !self.trailer_validated {
                    let mut trailing = [0u8; 8192];
                    loop {
                        let n = self.inner.read(&mut trailing)?;
                        if n == 0 {
                            self.trailer_validated = true;
                            break;
                        }
                        if trailing[..n]
                            .iter()
                            .any(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
                        {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "non-whitespace after top-level JSON array",
                            ));
                        }
                    }
                }
                return Ok(0);
            }
            let n = self.inner.read(&mut self.buffer)?;
            if n == 0 {
                if self.array_started && !self.finished {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "top-level JSON array is missing its closing bracket",
                    ));
                }
                self.finished = true;
                return Ok(0);
            }
            self.filled = filter_chunk(
                &mut self.started,
                &mut self.finished,
                &mut self.in_string,
                &mut self.escaped,
                &mut self.depth,
                &mut self.array_started,
                &mut self.array_has_value,
                &mut self.array_expect_value,
                &mut self.buffer[..n],
            )?;
            self.pos = 0;
            if self.filled == 0 {
                continue;
            }
        }
    }
}

/// Open a JSON array (`[{...},{...}]`, compact or multi-line) as a streaming
/// single-pass cursor. Memory is O(batch), independent of file size.
pub fn open_json_array(file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
    let batch_size = clamp_batch_size(batch_size);
    let schema = infer_json_array_schema(file_path)?;

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
    fn rejects_unclosed_array_and_non_whitespace_after_array() {
        for input in [b"[{\"a\":1}".as_slice(), b"[{\"a\":1}] trailing"] {
            let mut stream = JsonArrayStream::new(input);
            let mut output = Vec::new();
            assert!(stream.read_to_end(&mut output).is_err(), "{input:?}");
        }
    }

    #[test]
    fn rejects_invalid_array_elements_and_separators() {
        for input in [
            b"[{\"a\":1}{\"a\":2}]".as_slice(),
            b"[{\"a\":1},]",
            b"[, {\"a\":1}]",
            b"[{\"a\":1},,{\"a\":2}]",
            b"[1]",
        ] {
            let mut stream = JsonArrayStream::new(input);
            let mut output = Vec::new();
            assert!(stream.read_to_end(&mut output).is_err(), "{input:?}");
        }
    }

    #[test]
    fn allows_json_whitespace_after_array() {
        let mut stream = JsonArrayStream::new(b"[{\"a\":1}] \r\n".as_slice());
        assert_eq!(read_all(stream), b"{\"a\":1}");
    }

    #[test]
    fn accepts_empty_array() {
        let mut stream = JsonArrayStream::new(b" [] ".as_slice());
        assert!(read_all(&mut stream).is_empty());
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

    #[test]
    fn open_json_array_preserves_rows_with_empty_objects() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "basaltic_json_empty_objects_{}.jsonl",
            std::process::id()
        ));
        std::fs::write(&path, "[{}, {}, {}]").unwrap();

        let src = open_json_array(path.to_str().unwrap(), 1024).unwrap();
        assert!(src.schema.fields().is_empty());
        let rows: usize = src.batches.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(rows, 3);

        let _ = std::fs::remove_file(&path);
    }
}
