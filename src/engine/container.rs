use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;
use serde::{Serialize, Deserialize};
use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::engine::MatrixEngine;
use crate::utils::discover_data_files;

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
    /// Đóng gói toàn bộ cây thư mục CSDL/Lakehouse vào 1 file container duy nhất (.bazan)
    pub fn pack_directory_to_bazan(&self, input_dir: &Path, output_file: &Path) -> Result<(usize, u64)> {
        if !input_dir.exists() || !input_dir.is_dir() {
            return Err(anyhow!("Input directory does not exist or is not a directory: {:?}", input_dir));
        }

        let files = discover_data_files(input_dir, None)?;
        if files.is_empty() {
            return Err(anyhow!("No valid data files found in directory: {:?}", input_dir));
        }

        if let Some(parent) = output_file.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut out = File::create(output_file)?;
        
        // 1. Ghi Header Magic "BAZAN01" (7 bytes)
        out.write_all(HEADER_MAGIC)?;
        let mut current_offset = HEADER_MAGIC.len() as u64;

        let mut entries = Vec::with_capacity(files.len());

        for file_path in &files {
            let rel_path = file_path
                .strip_prefix(input_dir)
                .unwrap_or(file_path)
                .to_str()
                .ok_or_else(|| anyhow!("Invalid path string"))?
                .replace('\\', "/");

            let file_str = file_path.to_str().unwrap();
            
            // Đọc file thành RecordBatch chuẩn
            let batch = self.slice_rows_native(file_str, 0, usize::MAX)?;
            let num_rows = batch.num_rows();

            // Chuyển RecordBatch thành Parquet Bytes nén cao
            let mut parquet_buf = Vec::new();
            let props = parquet::file::properties::WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .build();
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut parquet_buf, batch.schema(), Some(props))?;
            writer.write(&batch)?;
            writer.close()?;

            let length = parquet_buf.len() as u64;

            // Ghi Stream Payload vào container
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

        // 3. Ghi Manifest JSON
        out.write_all(manifest_bytes)?;

        // 4. Ghi Manifest Offset (8 bytes u64 LE) + Manifest Length (8 bytes u64 LE)
        out.write_all(&manifest_offset.to_le_bytes())?;
        out.write_all(&manifest_length.to_le_bytes())?;

        // 5. Ghi Footer Magic "BAZANEND" (8 bytes)
        out.write_all(FOOTER_MAGIC)?;

        let total_file_size = out.metadata()?.len();

        Ok((manifest.entries.len(), total_file_size))
    }
}

/// Đọc Catalog Manifest từ Footer Index của file container .bazan (Tốc độ micro-giây)
pub fn read_bazan_manifest(bazan_path: &Path) -> Result<BazanManifest> {
    let mut file = File::open(bazan_path)?;
    let file_size = file.metadata()?.len();

    // Footer structure: 8 bytes (manifest_offset) + 8 bytes (manifest_length) + 8 bytes (FOOTER_MAGIC) = 24 bytes
    if file_size < (HEADER_MAGIC.len() + 24) as u64 {
        return Err(anyhow!("File size too small to be a valid .bazan container"));
    }

    // Đọc 24 bytes cuối cùng
    file.seek(SeekFrom::End(-24))?;
    let mut footer_buf = [0u8; 24];
    file.read_exact(&mut footer_buf)?;

    let mut offset_bytes = [0u8; 8];
    let mut len_bytes = [0u8; 8];
    let mut magic_bytes = [0u8; 8];

    offset_bytes.copy_from_slice(&footer_buf[0..8]);
    len_bytes.copy_from_slice(&footer_buf[8..16]);
    magic_bytes.copy_from_slice(&footer_buf[16..24]);

    if &magic_bytes != FOOTER_MAGIC {
        return Err(anyhow!("Invalid .bazan file format: Footer magic mismatch"));
    }

    let manifest_offset = u64::from_le_bytes(offset_bytes);
    let manifest_length = u64::from_le_bytes(len_bytes);

    // Read Manifest JSON
    file.seek(SeekFrom::Start(manifest_offset))?;
    let mut manifest_buf = vec![0u8; manifest_length as usize];
    file.read_exact(&mut manifest_buf)?;

    let manifest: BazanManifest = serde_json::from_slice(&manifest_buf)?;
    Ok(manifest)
}

/// Đọc trực tiếp byte stream của 1 bảng trong container .bazan và nạp vào Arrow RecordBatch (Zero-Copy Disk Extraction)
pub fn read_bazan_entry_batch(bazan_path: &Path, entry: &BazanEntry) -> Result<RecordBatch> {
    let mut file = File::open(bazan_path)?;
    file.seek(SeekFrom::Start(entry.offset))?;

    let mut buffer = vec![0u8; entry.length as usize];
    file.read_exact(&mut buffer)?;

    let bytes = Bytes::from(buffer);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let mut reader = builder.build()?;

    if let Some(batch_res) = reader.next() {
        Ok(batch_res?)
    } else {
        Err(anyhow!("Empty Parquet batch inside .bazan container for entry: {}", entry.path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_bazan_container_pack_inspect_and_read() -> Result<()> {
        let dir = tempdir()?;
        let input_dir = dir.path().join("input_db");
        let output_bazan = dir.path().join("test_db.bazan");

        std::fs::create_dir_all(input_dir.join("users"))?;
        std::fs::create_dir_all(input_dir.join("orders"))?;

        std::fs::write(input_dir.join("users/users.csv"), "id,name,age\n1,Alice,30\n2,Bob,25\n")?;
        std::fs::write(input_dir.join("orders/orders.csv"), "id,user_id,amount\n101,1,250.5\n102,2,100.0\n")?;

        let engine = MatrixEngine::new(1, 9, 0.01, 100.0);

        // 1. Pack
        let (total_entries, total_size) = engine.pack_directory_to_bazan(&input_dir, &output_bazan)?;
        assert_eq!(total_entries, 2);
        assert!(total_size > 0);

        // 2. Read Manifest (Inspect)
        let manifest = read_bazan_manifest(&output_bazan)?;
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.entries.len(), 2);

        // 3. Read Entry Batch Directly from .bazan file
        let users_entry = manifest.entries.iter().find(|e| e.path.contains("users")).unwrap();
        let users_batch = read_bazan_entry_batch(&output_bazan, users_entry)?;
        assert_eq!(users_batch.num_rows(), 2);

        Ok(())
    }
}
