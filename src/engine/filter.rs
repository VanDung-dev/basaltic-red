use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use arrow::compute::kernels::bitwise::bitwise_shift_left_scalar;
use arrow::compute::kernels::cmp::{eq, gt, lt};
use arrow::compute::kernels::numeric::add;
use arrow::compute::{cast, filter, filter_record_batch, is_null, not, or};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use crate::engine::MatrixEngine;

fn timestamp_seconds(array: &dyn Array, row: usize) -> Option<f64> {
    if array.is_null(row) {
        return None;
    }

    match array.data_type() {
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .map(|values| values.value(row) as f64),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|values| values.value(row) as f64 / 1_000.0),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|values| values.value(row) as f64 / 1_000_000.0),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|values| values.value(row) as f64 / 1_000_000_000.0),
        _ => None,
    }
}

fn nulls_are_invalid(mask: &BooleanArray) -> BooleanArray {
    BooleanArray::from_iter(mask.iter().map(|value| Some(value.unwrap_or(true))))
}

fn speed_invalid_mask(
    record_batch: &RecordBatch,
    distance_col: Option<&Float64Array>,
    fare_invalid: &BooleanArray,
    max_speed_mph: f64,
    total_rows: usize,
) -> BooleanArray {
    let Some(distance) = distance_col else {
        return BooleanArray::from(vec![false; total_rows]);
    };

    let pickup = record_batch.column_by_name("tpep_pickup_datetime");
    let dropoff = record_batch.column_by_name("tpep_dropoff_datetime");

    let invalid = (0..total_rows)
        .map(|row| {
            let distance_is_positive = !distance.is_null(row) && distance.value(row) > 0.0;
            if !distance_is_positive {
                return false;
            }

            match (pickup, dropoff) {
                (Some(pickup), Some(dropoff)) => {
                    let Some(start) = timestamp_seconds(pickup.as_ref(), row) else {
                        return false;
                    };
                    let Some(end) = timestamp_seconds(dropoff.as_ref(), row) else {
                        return false;
                    };
                    let duration_hours = (end - start) / 3_600.0;
                    duration_hours <= 0.0
                        || distance.value(row) / duration_hours > max_speed_mph
                }
                // Keep the legacy distance/fare anomaly for non-taxi schemas
                // that do not expose timestamps needed for a speed calculation.
                _ => fare_invalid.value(row),
            }
        })
        .collect::<Vec<_>>();

    BooleanArray::from(invalid)
}

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
        let fare_col = record_batch
            .column_by_name("fare_amount")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());

        let distance_col = record_batch
            .column_by_name("trip_distance")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());

        // 1. passenger_invalid: null OR value < min_passenger OR value > max_passenger.
        // `is_null` is the definite anchor so `or` (Kleene) yields a null-free mask.
        let passenger_invalid = match record_batch.column_by_name("passenger_count") {
            Some(c) if c.as_any().is::<Int64Array>() => {
                let p = c.as_any().downcast_ref::<Int64Array>().unwrap();
                let below = lt(p, &Int64Array::new_scalar(self.min_passenger));
                let above = gt(p, &Int64Array::new_scalar(self.max_passenger));
                nulls_are_invalid(
                    &or(
                        &is_null(p).unwrap(),
                        &or(&below.unwrap(), &above.unwrap()).unwrap(),
                    )
                    .unwrap(),
                )
            }
            Some(c) if c.as_any().is::<Float64Array>() => {
                let p = c.as_any().downcast_ref::<Float64Array>().unwrap();
                let below = lt(p, &Float64Array::new_scalar(self.min_passenger as f64));
                let above = gt(p, &Float64Array::new_scalar(self.max_passenger as f64));
                nulls_are_invalid(
                    &or(
                        &is_null(p).unwrap(),
                        &or(&below.unwrap(), &above.unwrap()).unwrap(),
                    )
                    .unwrap(),
                )
            }
            _ => BooleanArray::from(vec![false; total_rows]),
        };

        // 2. fare_invalid: null OR value < min_fare.
        let fare_invalid = match fare_col {
            Some(f) => {
                let below = lt(f, &Float64Array::new_scalar(self.min_fare));
                nulls_are_invalid(&or(&is_null(f).unwrap(), &below.unwrap()).unwrap())
            }
            None => BooleanArray::from(vec![false; total_rows]),
        };

        // 3. Speed anomaly from timestamps when available; otherwise use the
        // legacy distance/fare anomaly for non-taxi schemas.
        let speed_invalid = speed_invalid_mask(
            record_batch,
            distance_col,
            &fare_invalid,
            self.max_speed_mph,
            total_rows,
        );

        // audit_error_code = passenger * 1 + fare * 2 + speed * 4  (vectorized)
        let p_mask = cast(&passenger_invalid, &DataType::UInt64).unwrap();
        let f_mask = cast(&fare_invalid, &DataType::UInt64).unwrap();
        let f_mask = f_mask
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .clone();
        let s_mask = cast(&speed_invalid, &DataType::UInt64).unwrap();
        let s_mask = s_mask
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .clone();
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
