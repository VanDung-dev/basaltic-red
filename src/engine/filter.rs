use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow::compute::{filter_record_batch, not};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use crate::engine::MatrixEngine;
use crate::filter::{ERR_INVALID_FARE, ERR_INVALID_PASSENGER, ERR_INVALID_SPEED};

impl MatrixEngine {
    pub fn filter_batch_native(
        &self,
        record_batch: &RecordBatch,
        total_rows: usize,
    ) -> (RecordBatch, RecordBatch) {
        let passenger_col = record_batch
            .column_by_name("passenger_count")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

        let fare_col = record_batch
            .column_by_name("fare_amount")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());

        let distance_col = record_batch
            .column_by_name("trip_distance")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());

        let mut clean_mask_builder = Vec::with_capacity(total_rows);
        let mut error_code_builder = Vec::with_capacity(total_rows);

        for i in 0..total_rows {
            let mut err_flags: u64 = 0;

            // 1. Validate passenger_count
            if let Some(p_arr) = passenger_col {
                if !p_arr.is_valid(i)
                    || p_arr.value(i) < self.min_passenger
                    || p_arr.value(i) > self.max_passenger
                {
                    err_flags |= ERR_INVALID_PASSENGER;
                }
            }

            // 2. Validate fare_amount
            if let Some(f_arr) = fare_col {
                if !f_arr.is_valid(i) || f_arr.value(i) < self.min_fare {
                    err_flags |= ERR_INVALID_FARE;
                }
            }

            // 3. Validate speed anomaly (if valid trip_distance and fare_amount exist)
            if let Some(d_arr) = distance_col {
                if d_arr.is_valid(i) && d_arr.value(i) > 0.0 {
                    // Non-zero distance with invalid fare flags speed/fare anomaly
                    if err_flags & ERR_INVALID_FARE != 0 {
                        err_flags |= ERR_INVALID_SPEED;
                    }
                }
            }

            let is_clean = err_flags == 0;
            clean_mask_builder.push(is_clean);
            error_code_builder.push(err_flags);
        }

        let clean_bitmask = BooleanArray::from(clean_mask_builder);
        let trash_bitmask = not(&clean_bitmask).unwrap();

        let clean_batch = filter_record_batch(record_batch, &clean_bitmask).unwrap();

        // Build Trash Batch with attached audit_error_code column
        let trash_filtered_base = filter_record_batch(record_batch, &trash_bitmask).unwrap();
        let error_code_arr = UInt64Array::from(error_code_builder);
        let trash_error_codes = arrow::compute::filter(&error_code_arr, &trash_bitmask).unwrap();

        // Append "audit_error_code" column to Trash Batch
        let mut trash_fields = trash_filtered_base.schema().fields().to_vec();
        trash_fields.push(Field::new("audit_error_code", DataType::UInt64, false).into());
        let trash_schema = Arc::new(Schema::new(trash_fields));

        let mut trash_columns = trash_filtered_base.columns().to_vec();
        trash_columns.push(trash_error_codes);

        let trash_batch = RecordBatch::try_new(trash_schema, trash_columns).unwrap();

        (clean_batch, trash_batch)
    }
}
