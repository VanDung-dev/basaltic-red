use crate::engine::MatrixEngine;
use crate::error::BazanError;
use crate::utils::discover_data_files;
use arrow::array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub const HEADER_MAGIC: &[u8] = b"BAZAN01";
pub const FOOTER_MAGIC: &[u8] = b"BAZANEND";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BazanEntry {
    pub path: String,
    pub offset: u64,
    pub length: u64,
    pub format: String,
    pub num_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BazanManifest {
    pub version: u32,
    pub entries: Vec<BazanEntry>,
}

impl MatrixEngine {
    /// Pack an entire DB/Lakehouse directory tree into a single container file (.bazan)
    pub fn pack_directory_to_bazan(
        &self,
        input_dir: &Path,
        output_file: &Path,
    ) -> Result<(usize, u64), BazanError> {
        if !input_dir.exists() || !input_dir.is_dir() {
            return Err(BazanError::Message(format!(
                "Input directory does not exist or is not a directory: {:?}",
                input_dir
            )));
        }

        let files = discover_data_files(input_dir, None)?;
        if files.is_empty() {
            return Err(BazanError::Message(format!(
                "No valid data files found in directory: {:?}",
                input_dir
            )));
        }

        if let Some(parent) = output_file.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut out = File::create(output_file)?;

        // 1. Write Header Magic "BAZAN01" (7 bytes)
        out.write_all(HEADER_MAGIC)?;
        let mut current_offset = HEADER_MAGIC.len() as u64;

        let mut entries = Vec::with_capacity(files.len());

        for file_path in &files {
            let rel_path = file_path
                .strip_prefix(input_dir)
                .unwrap_or(file_path)
                .to_str()
                .ok_or_else(|| BazanError::Message("Invalid path string".to_string()))?
                .replace('\\', "/");

            let file_str = file_path.to_str().ok_or_else(|| {
                BazanError::Message(format!("Invalid non-UTF8 path: {:?}", file_path))
            })?;

            // Read file into standard RecordBatch
            let batch = self.slice_rows_native(file_str, 0, usize::MAX)?;
            let num_rows = batch.num_rows();

            // Convert RecordBatch to high-compression Parquet Bytes
            let mut parquet_buf = Vec::new();
            let props = parquet::file::properties::WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .build();
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                &mut parquet_buf,
                batch.schema(),
                Some(props),
            )?;
            writer.write(&batch)?;
            writer.close()?;

            let length = parquet_buf.len() as u64;

            // Write Stream Payload to container
            out.write_all(&parquet_buf)?;

            entries.push(BazanEntry {
                path: rel_path,
                offset: current_offset,
                length,
                format: "parquet".to_string(),
                num_rows,
            });

            current_offset += length;
        }

        // 2. Serialized Catalog Manifest JSON
        let manifest = BazanManifest {
            version: 1,
            entries,
        };
        let manifest_json = serde_json::to_string(&manifest)?;
        let manifest_bytes = manifest_json.as_bytes();
        let manifest_offset = current_offset;
        let manifest_length = manifest_bytes.len() as u64;

        // 3. Write Manifest JSON
        out.write_all(manifest_bytes)?;

        // 4. Write Manifest Offset (8 bytes u64 LE) + Manifest Length (8 bytes u64 LE)
        out.write_all(&manifest_offset.to_le_bytes())?;
        out.write_all(&manifest_length.to_le_bytes())?;

        // 5. Write Footer Magic "BAZANEND" (8 bytes)
        out.write_all(FOOTER_MAGIC)?;

        let total_file_size = out.metadata()?.len();

        Ok((manifest.entries.len(), total_file_size))
    }
}

/// Read Catalog Manifest from Footer Index of .bazan container file (microsecond speed)
pub fn read_bazan_manifest(bazan_path: &Path) -> Result<BazanManifest, BazanError> {
    let mut file = File::open(bazan_path)?;
    let file_size = file.metadata()?.len();

    // Footer structure: 8 bytes (manifest_offset) + 8 bytes (manifest_length) + 8 bytes (FOOTER_MAGIC) = 24 bytes
    if file_size < (HEADER_MAGIC.len() + 24) as u64 {
        return Err(BazanError::Message(
            "File size too small to be a valid .bazan container".to_string(),
        ));
    }

    // Read last 24 bytes
    file.seek(SeekFrom::End(-24))?;
    let mut footer_buf = [0u8; 24];
    file.read_exact(&mut footer_buf)?;

    let mut offset_bytes = [0u8; 8];
    let mut len_bytes = [0u8; 8];
    let mut magic_bytes = [0u8; 8];

    offset_bytes.copy_from_slice(&footer_buf[0..8]);
    len_bytes.copy_from_slice(&footer_buf[8..16]);
    magic_bytes.copy_from_slice(&footer_buf[16..24]);

    if magic_bytes != FOOTER_MAGIC {
        return Err(BazanError::Message(
            "Invalid .bazan file format: Footer magic mismatch".to_string(),
        ));
    }

    let manifest_offset = u64::from_le_bytes(offset_bytes);
    let manifest_length = u64::from_le_bytes(len_bytes);

    // Header magic must match a real container
    file.seek(SeekFrom::Start(0))?;
    let mut header_buf = [0u8; HEADER_MAGIC.len()];
    file.read_exact(&mut header_buf)?;
    if header_buf != HEADER_MAGIC {
        return Err(BazanError::Message(
            "Invalid .bazan file format: Header magic mismatch".to_string(),
        ));
    }

    // Bounds-check before allocating: a crafted length must not OOM the process
    if manifest_offset
        .checked_add(manifest_length)
        .is_none_or(|end| end > file_size)
    {
        return Err(BazanError::Message(format!(
            "Corrupt .bazan container: manifest offset {} + length {} exceeds file size {}",
            manifest_offset, manifest_length, file_size
        )));
    }

    // Read Manifest JSON
    file.seek(SeekFrom::Start(manifest_offset))?;
    let mut manifest_buf = vec![0u8; manifest_length as usize];
    file.read_exact(&mut manifest_buf)?;

    let manifest: BazanManifest = serde_json::from_slice(&manifest_buf)?;
    Ok(manifest)
}

/// Read byte stream directly from an entry in .bazan container into Arrow RecordBatch (supports Projection Pushdown)
pub fn read_bazan_entry_batch_projected(
    bazan_path: &Path,
    entry: &BazanEntry,
    projection: Option<&[usize]>,
) -> Result<RecordBatch, BazanError> {
    let mut file = File::open(bazan_path)?;
    let file_size = file.metadata()?.len();

    // Bounds-check before allocating: a crafted entry must not OOM the process
    if entry
        .offset
        .checked_add(entry.length)
        .is_none_or(|end| end > file_size)
    {
        return Err(BazanError::Message(format!(
            "Corrupt .bazan container: entry '{}' offset {} + length {} exceeds file size {}",
            entry.path, entry.offset, entry.length, file_size
        )));
    }

    file.seek(SeekFrom::Start(entry.offset))?;

    let mut buffer = vec![0u8; entry.length as usize];
    file.read_exact(&mut buffer)?;

    let bytes = Bytes::from(buffer);
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    if let Some(proj) = projection {
        let mask = parquet::arrow::ProjectionMask::roots(
            builder.parquet_schema(),
            proj.iter().copied(),
        );
        builder = builder.with_projection(mask);
    }
    let mut reader = builder.build()?;

    if let Some(batch_res) = reader.next() {
        Ok(batch_res?)
    } else {
        Err(BazanError::Message(format!(
            "Empty Parquet batch inside .bazan container for entry: {}",
            entry.path
        )))
    }
}

pub fn read_bazan_entry_batch(
    bazan_path: &Path,
    entry: &BazanEntry,
) -> Result<RecordBatch, BazanError> {
    read_bazan_entry_batch_projected(bazan_path, entry, None)
}

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::MemTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
pub struct BazanTableProvider {
    bazan_path: std::path::PathBuf,
    manifest: BazanManifest,
    schema: arrow::datatypes::SchemaRef,
}

impl BazanTableProvider {
    pub fn try_new(bazan_path: &Path) -> Result<Self, BazanError> {
        let manifest = read_bazan_manifest(bazan_path)?;
        if manifest.entries.is_empty() {
            return Err(BazanError::Message(
                "No entries in .bazan container".to_string(),
            ));
        }

        let sample_batch = read_bazan_entry_batch_projected(bazan_path, &manifest.entries[0], None)?;
        let schema = sample_batch.schema();

        Ok(Self {
            bazan_path: bazan_path.to_path_buf(),
            manifest,
            schema,
        })
    }

    pub fn manifest(&self) -> &BazanManifest {
        &self.manifest
    }
}

#[async_trait]
impl TableProvider for BazanTableProvider {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut batches = Vec::with_capacity(self.manifest.entries.len());
        for entry in &self.manifest.entries {
            let batch = read_bazan_entry_batch_projected(
                &self.bazan_path,
                entry,
                projection.map(|v| v.as_slice()),
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            batches.push(batch);
        }

        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}
