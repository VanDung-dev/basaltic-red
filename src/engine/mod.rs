use pyo3::prelude::*;
use pyo3::{Py, types::PyAny};

pub mod csv;
pub mod dictionary;
pub mod filter;
pub mod json;
pub mod jsonl;
pub mod ndjson;
pub mod parquet;
pub mod psv;
pub mod tsv;
pub mod txt;

/// Core SIMD Matrix Engine supporting Audit Error Bitmasking for Matrix Trash & Parquet Streaming
#[pyclass]
pub struct MatrixEngine {
    pub min_passenger: i64,
    pub max_passenger: i64,
    pub min_fare: f64,
    pub max_speed_mph: f64,
}

#[pymethods]
impl MatrixEngine {
    #[new]
    #[pyo3(signature = (min_passenger=1, max_passenger=9, min_fare=0.01, max_speed_mph=100.0))]
    pub fn new(min_passenger: i64, max_passenger: i64, min_fare: f64, max_speed_mph: f64) -> Self {
        Self {
            min_passenger,
            max_passenger,
            min_fare,
            max_speed_mph,
        }
    }

    /// Filters a PyArrow RecordBatch into Clean RecordBatch and Trash RecordBatch (with Audit Error Bitmask)
    pub fn process_batch<'py>(
        &self,
        py: Python<'py>,
        batch_obj: &Bound<'py, PyAny>,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        use arrow::pyarrow::{FromPyArrow, ToPyArrow};
        use arrow::array::RecordBatch;

        let record_batch = RecordBatch::from_pyarrow_bound(batch_obj)?;
        let total_rows = record_batch.num_rows();

        let (clean_b, trash_b) = py.detach(|| {
            self.filter_batch_native(&record_batch, total_rows)
        });

        Ok((clean_b.to_pyarrow(py)?.into(), trash_b.to_pyarrow(py)?.into()))
    }

    /// Unified Smart Reader: Automatically detects file extension (.parquet, .csv, .tsv, .psv, .txt, .json, .jsonl, .ndjson)
    pub fn process_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path_obj = std::path::Path::new(file_path);
        let ext = path_obj.extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
        
        if ext == "csv" {
            self.process_csv_file(py, file_path, batch_size)
        } else if ext == "tsv" {
            self.process_tsv_file(py, file_path, batch_size)
        } else if ext == "psv" {
            self.process_psv_file(py, file_path, batch_size)
        } else if ext == "txt" {
            self.process_txt_file(py, file_path, batch_size)
        } else if ext == "json" {
            self.process_json_file(py, file_path, batch_size)
        } else if ext == "jsonl" {
            self.process_jsonl_file(py, file_path, batch_size)
        } else if ext == "ndjson" {
            self.process_ndjson_file(py, file_path, batch_size)
        } else if ext == "parquet" || ext == "pq" {
            self.process_parquet_file(py, file_path, batch_size)
        } else {
            Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unsupported file format: '.{}'. Supported formats: csv, tsv, psv, txt, json, jsonl, ndjson, parquet, pq",
                ext
            )))
        }
    }


    /// Enterprise Multi-File Partition Handler & Async Parquet Writer
    pub fn process_and_write_lake(
        &self,
        py: Python<'_>,
        input_dir: &str,
        clean_output_dir: &str,
        trash_output_dir: &str,
        partition_filter: Option<&str>,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize, usize)> {
        let in_dir = input_dir.to_string();
        let clean_dir = clean_output_dir.to_string();
        let trash_dir = trash_output_dir.to_string();
        let filter_str = partition_filter.map(|s| s.to_string());

        let stats = py.detach(|| -> Result<(usize, usize, usize, usize), anyhow::Error> {
            self.process_and_write_lake_native(&in_dir, &clean_dir, &trash_dir, filter_str.as_deref(), batch_size)
        });

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }

    /// Clean Gold Table Generator & Versioning
    pub fn generate_gold_table(
        &self,
        py: Python<'_>,
        input_dir: &str,
        gold_output_dir: &str,
        table_version: &str,
        partition_filter: Option<&str>,
        batch_size: usize,
    ) -> PyResult<(usize, usize, String)> {
        let in_dir = input_dir.to_string();
        let gold_dir = gold_output_dir.to_string();
        let ver_str = table_version.to_string();
        let filter_str = partition_filter.map(|s| s.to_string());

        let res = py.detach(|| -> Result<(usize, usize, String), anyhow::Error> {
            self.generate_gold_table_native(&in_dir, &gold_dir, &ver_str, filter_str.as_deref(), batch_size)
        });

        match res {
            Ok(val) => Ok(val),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }

    /// Exposes preview_parquet_sample to Python
    pub fn preview_sample<'py>(
        &self,
        py: Python<'py>,
        file_path: &str,
        limit_rows: usize,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        self.preview_parquet_sample(py, file_path, limit_rows)
    }

    /// Exposes export_data_dictionary_md to Python
    pub fn export_data_dictionary_md(
        &self,
        py: Python<'_>,
        target_path: &str,
        output_path: &str,
    ) -> PyResult<String> {
        self.export_data_dictionary_md_inner(py, target_path, output_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::sync::Arc;
    use arrow::array::{Float64Array, Int64Array, UInt64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use ::parquet::arrow::ArrowWriter;

    use tempfile::{TempDir, NamedTempFile};
    use crate::filter::{ERR_INVALID_PASSENGER, ERR_INVALID_FARE};

    #[test]
    fn test_core_simd_matrix_filter_with_audit_codes() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("passenger_count", DataType::Int64, false),
            Field::new("fare_amount", DataType::Float64, false),
            Field::new("trip_distance", DataType::Float64, false),
        ]));

        let passenger_array = Arc::new(Int64Array::from(vec![1, 2, 0, 5, 12, 1]));
        let fare_array = Arc::new(Float64Array::from(vec![15.5, -5.0, 20.0, 100.0, 50.0, 0.0]));
        let distance_array = Arc::new(Float64Array::from(vec![2.5, 0.0, 3.1, 10.0, 1.2, 5.0]));

        let batch = RecordBatch::try_new(schema, vec![passenger_array, fare_array, distance_array]).unwrap();
        
        let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
        let (clean_b, trash_b) = engine.filter_batch_native(&batch, 6);

        // 2 clean rows (row 0 and row 3)
        assert_eq!(clean_b.num_rows(), 2);
        // 4 trash rows (row 1, 2, 4, 5)
        assert_eq!(trash_b.num_rows(), 4);

        // Verify Trash Batch contains audit_error_code column
        let err_col = trash_b.column_by_name("audit_error_code").unwrap();
        let err_arr = err_col.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(err_arr.len(), 4);
        
        // Row 1 (fare -5.0) -> ERR_INVALID_FARE (0x02)
        assert_eq!(err_arr.value(0), ERR_INVALID_FARE);
        // Row 2 (passenger 0) -> ERR_INVALID_PASSENGER (0x01)
        assert_eq!(err_arr.value(1), ERR_INVALID_PASSENGER);
    }

    #[test]
    fn test_multi_threaded_async_parquet_writer() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("passenger_count", DataType::Int64, false),
            Field::new("fare_amount", DataType::Float64, false),
            Field::new("trip_distance", DataType::Float64, false),
        ]));

        let passenger_array = Arc::new(Int64Array::from(vec![1, 2, 0, 5, 12, 1]));
        let fare_array = Arc::new(Float64Array::from(vec![15.5, -5.0, 20.0, 100.0, 50.0, 0.0]));
        let distance_array = Arc::new(Float64Array::from(vec![2.5, 0.0, 3.1, 10.0, 1.2, 5.0]));

        let batch = RecordBatch::try_new(schema.clone(), vec![passenger_array, fare_array, distance_array]).unwrap();

        // Create input directory structure: temp_in/year=2023/month=01/part1.parquet and month=02/part2.parquet
        let in_dir = TempDir::new().unwrap();
        let clean_dir = TempDir::new().unwrap();
        let trash_dir = TempDir::new().unwrap();

        let month1_dir = in_dir.path().join("year=2023").join("month=01");
        let month2_dir = in_dir.path().join("year=2023").join("month=02");
        std::fs::create_dir_all(&month1_dir).unwrap();
        std::fs::create_dir_all(&month2_dir).unwrap();

        let file1_path = month1_dir.join("part1.parquet");
        let file2_path = month2_dir.join("part2.parquet");

        let f1 = File::create(&file1_path).unwrap();
        let mut w1 = ArrowWriter::try_new(f1, schema.clone(), None).unwrap();
        w1.write(&batch).unwrap();
        w1.close().unwrap();

        let f2 = File::create(&file2_path).unwrap();
        let mut w2 = ArrowWriter::try_new(f2, schema, None).unwrap();
        w2.write(&batch).unwrap();
        w2.close().unwrap();

        let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
        let in_path_str = in_dir.path().to_str().unwrap();
        let clean_path_str = clean_dir.path().to_str().unwrap();
        let trash_path_str = trash_dir.path().to_str().unwrap();

        // Run process_and_write_lake_native
        let (files, total_rows, clean_rows, trash_rows) = engine
            .process_and_write_lake_native(in_path_str, clean_path_str, trash_path_str, None, 1024)
            .unwrap();

        assert_eq!(files, 2);
        assert_eq!(total_rows, 12);
        assert_eq!(clean_rows, 4);
        assert_eq!(trash_rows, 8);

        // Verify output directory Hive structure exists
        let clean_part1 = clean_dir.path().join("year=2023").join("month=01").join("part1.parquet");
        let trash_part1 = trash_dir.path().join("year=2023").join("month=01").join("part1.parquet");

        assert!(clean_part1.exists());
        assert!(trash_part1.exists());

        // Verify written clean file content using streaming reader
        let clean_file = File::open(&clean_part1).unwrap();
        let clean_builder = ParquetRecordBatchReaderBuilder::try_new(clean_file).unwrap();
        let mut clean_reader = clean_builder.build().unwrap();
        let clean_read_batch = clean_reader.next().unwrap().unwrap();
        assert_eq!(clean_read_batch.num_rows(), 2);

        // Verify written trash file content contains audit_error_code column
        let trash_file = File::open(&trash_part1).unwrap();
        let trash_builder = ParquetRecordBatchReaderBuilder::try_new(trash_file).unwrap();
        let mut trash_reader = trash_builder.build().unwrap();
        let trash_read_batch = trash_reader.next().unwrap().unwrap();
        assert_eq!(trash_read_batch.num_rows(), 4);
        assert!(trash_read_batch.column_by_name("audit_error_code").is_some());
    }

    #[test]
    fn test_duckdb_preview_sample_extraction() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("passenger_count", DataType::Int64, false),
            Field::new("fare_amount", DataType::Float64, false),
            Field::new("trip_distance", DataType::Float64, false),
        ]));

        let passenger_array = Arc::new(Int64Array::from(vec![1, 2, 0, 5, 12, 1]));
        let fare_array = Arc::new(Float64Array::from(vec![15.5, -5.0, 20.0, 100.0, 50.0, 0.0]));
        let distance_array = Arc::new(Float64Array::from(vec![2.5, 0.0, 3.1, 10.0, 1.2, 5.0]));

        let batch = RecordBatch::try_new(schema.clone(), vec![passenger_array, fare_array, distance_array]).unwrap();

        let file = NamedTempFile::new().unwrap();
        let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let file_path = file.path().to_str().unwrap();
        let file_reader = File::open(file_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file_reader).unwrap().with_batch_size(10);
        let mut reader = builder.build().unwrap();
        let sample_batch = reader.next().unwrap().unwrap();

        let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
        let (clean_b, trash_b) = engine.filter_batch_native(&sample_batch, sample_batch.num_rows());

        assert_eq!(clean_b.num_rows(), 2);
        assert_eq!(trash_b.num_rows(), 4);
    }

    #[test]
    fn test_clean_gold_table_generator_and_manifest() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("passenger_count", DataType::Int64, false),
            Field::new("fare_amount", DataType::Float64, false),
            Field::new("trip_distance", DataType::Float64, false),
        ]));

        let passenger_array = Arc::new(Int64Array::from(vec![1, 2, 0, 5, 12, 1]));
        let fare_array = Arc::new(Float64Array::from(vec![15.5, -5.0, 20.0, 100.0, 50.0, 0.0]));
        let distance_array = Arc::new(Float64Array::from(vec![2.5, 0.0, 3.1, 10.0, 1.2, 5.0]));

        let batch = RecordBatch::try_new(schema.clone(), vec![passenger_array, fare_array, distance_array]).unwrap();

        let in_dir = TempDir::new().unwrap();
        let gold_dir = TempDir::new().unwrap();

        let month_dir = in_dir.path().join("year=2023").join("month=01");
        std::fs::create_dir_all(&month_dir).unwrap();

        let file_path = month_dir.join("part1.parquet");
        let f = File::create(&file_path).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
        let in_str = in_dir.path().to_str().unwrap();
        let gold_str = gold_dir.path().to_str().unwrap();

        let (files, gold_rows, manifest_path) = engine
            .generate_gold_table_native(in_str, gold_str, "v1.0.0", None, 1024)
            .unwrap();

        assert_eq!(files, 1);
        assert_eq!(gold_rows, 2);

        // Verify _gold_metadata.json exists and contains correct version string
        let manifest_file = std::path::Path::new(&manifest_path);
        assert!(manifest_file.exists());

        let manifest_text = std::fs::read_to_string(manifest_file).unwrap();
        assert!(manifest_text.contains("\"version\": \"v1.0.0\""));
        assert!(manifest_text.contains("\"total_gold_rows\": 2"));
    }
}
