use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow::compute::kernels::cmp::{eq, gt, lt};
use arrow::compute::kernels::bitwise::bitwise_shift_left_scalar;
use arrow::compute::kernels::numeric::add;
use arrow::compute::{and, cast, filter, filter_record_batch, is_null, not, or};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// Vectorized (SIMD) audit filter: every per-row mask is built with Arrow
    /// compute kernels on whole columns; the only scalar work left is the final
    /// `concat_batches`/`filter` steps. `audit_error_code` is a weighted sum of
    /// the three violation masks, so it stays fully vectorized.
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

        // 1. passenger_invalid: null OR value < min_passenger OR value > max_passenger.
        // `is_null` is the definite anchor so `or` (Kleene) yields a null-free mask.
        let passenger_invalid = match passenger_col {
            Some(p) => {
                let below = lt(p, &Int64Array::new_scalar(self.min_passenger));
                let above = gt(p, &Int64Array::new_scalar(self.max_passenger));
                or(&is_null(p).unwrap(), &or(&below.unwrap(), &above.unwrap()).unwrap()).unwrap()
            }
            None => BooleanArray::from(vec![false; total_rows]),
        };

        // 2. fare_invalid: null OR value < min_fare.
        let fare_invalid = match fare_col {
            Some(f) => {
                let below = lt(f, &Float64Array::new_scalar(self.min_fare));
                or(&is_null(f).unwrap(), &below.unwrap()).unwrap()
            }
            None => BooleanArray::from(vec![false; total_rows]),
        };

        // 3. speed anomaly: fare invalid AND distance valid AND distance > 0.
        let speed_invalid = match distance_col {
            Some(d) => {
                let gt_zero = gt(d, &Float64Array::new_scalar(0.0));
                let dist_ok = and(&not(&is_null(d).unwrap()).unwrap(), &gt_zero.unwrap()).unwrap();
                and(&fare_invalid, &dist_ok).unwrap()
            }
            None => BooleanArray::from(vec![false; total_rows]),
        };

        // audit_error_code = passenger * 1 + fare * 2 + speed * 4  (vectorized)
        let p_mask = cast(&passenger_invalid, &DataType::UInt64).unwrap();
        let f_mask = cast(&fare_invalid, &DataType::UInt64).unwrap();
        let f_mask = f_mask.as_any().downcast_ref::<UInt64Array>().unwrap().clone();
        let s_mask = cast(&speed_invalid, &DataType::UInt64).unwrap();
        let s_mask = s_mask.as_any().downcast_ref::<UInt64Array>().unwrap().clone();
        let f2 = bitwise_shift_left_scalar(&f_mask, 1).unwrap();
        let s4 = bitwise_shift_left_scalar(&s_mask, 2).unwrap();
        let err = add(&add(&p_mask, &f2).unwrap(), &s4).unwrap();
        let err = err
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64 error code array")
            .clone();

        let clean_bitmask = eq(&err, &UInt64Array::new_scalar(0)).unwrap();
        let trash_bitmask = not(&clean_bitmask).unwrap();

        let clean_batch = filter_record_batch(record_batch, &clean_bitmask).unwrap();

        // Build Trash Batch with attached audit_error_code column
        let trash_filtered_base = filter_record_batch(record_batch, &trash_bitmask).unwrap();
        let trash_error_codes = filter(&err, &trash_bitmask).unwrap();

        // Append "audit_error_code" column to Trash Batch
        let mut trash_fields = trash_filtered_base.schema().fields().to_vec();
        trash_fields.push(Field::new("audit_error_code", DataType::UInt64, false).into());
        let trash_schema = Arc::new(Schema::new(trash_fields));

        let mut trash_columns = trash_filtered_base.columns().to_vec();
        trash_columns.push(Arc::new(trash_error_codes) as _);

        let trash_batch = RecordBatch::try_new(trash_schema, trash_columns).unwrap();

        (clean_batch, trash_batch)
    }
}
