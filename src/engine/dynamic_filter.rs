use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, Scalar, StringArray, UInt64Array,
};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::kernels::bitwise::{bitwise_or, bitwise_shift_left_scalar};
use arrow::compute::{and, cast, filter, filter_record_batch, is_null, not, or};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use crate::engine::MatrixEngine;
use crate::error::BazanError;

#[derive(Debug, Clone)]
pub enum Operator {
    Gt,  // >
    Gte, // >=
    Lt,  // <
    Lte, // <=
    Eq,  // ==
    Neq, // !=
}

#[derive(Debug, Clone)]
pub struct FilterRule {
    pub col_name: String,
    pub op: Operator,
    pub val_str: String,
}

impl FilterRule {
    pub fn parse(expr: &str) -> Result<Self, BazanError> {
        let expr = expr.trim();
        let ops = [
            (">=", Operator::Gte),
            ("<=", Operator::Lte),
            ("==", Operator::Eq),
            ("!=", Operator::Neq),
            (">", Operator::Gt),
            ("<", Operator::Lt),
        ];

        for (op_str, op) in ops {
            if let Some(idx) = expr.find(op_str) {
                let col_name = expr[..idx].trim().to_string();
                let val_str = expr[idx + op_str.len()..]
                    .trim()
                    .trim_matches('\'')
                    .trim_matches('"')
                    .to_string();
                return Ok(FilterRule {
                    col_name,
                    op,
                    val_str,
                });
            }
        }

        Err(BazanError::Message(format!(
            "Invalid rule expression: '{}'. Format example: 'age >= 18'",
            expr
        )))
    }
}

impl MatrixEngine {
    /// Evaluate dynamic rules on a RecordBatch and split into Clean and Trash RecordBatches.
    /// Supports arbitrary number of rules (> 64) with multi-chunk SIMD bitmasking
    /// without bit collisions.
    pub fn filter_batch_dynamic(
        &self,
        batch: &RecordBatch,
        rules: &[FilterRule],
    ) -> Result<(RecordBatch, RecordBatch), BazanError> {
        let total_rows = batch.num_rows();
        let mut clean_mask = BooleanArray::from(vec![true; total_rows]);

        let num_chunks = ((rules.len() + 63) / 64).max(1);
        let mut error_chunks: Vec<UInt64Array> = (0..num_chunks)
            .map(|_| UInt64Array::from(vec![0u64; total_rows]))
            .collect();

        for (rule_idx, rule) in rules.iter().enumerate() {
            if let Some(col) = batch.column_by_name(&rule.col_name) {
                let violation = match col.data_type() {
                    DataType::Int64 => col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .and_then(|_| rule.val_str.parse::<i64>().ok())
                        .map(|target| violation_int(col.as_ref(), target, &rule.op)),
                    DataType::Int32 => col
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .and_then(|_| rule.val_str.parse::<i32>().ok())
                        .map(|target| violation_int32(col.as_ref(), target, &rule.op)),
                    DataType::UInt64 => col
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .and_then(|_| rule.val_str.parse::<u64>().ok())
                        .map(|target| violation_u64(col.as_ref(), target, &rule.op)),
                    DataType::UInt32 => col
                        .as_any()
                        .downcast_ref::<arrow::array::UInt32Array>()
                        .and_then(|_| rule.val_str.parse::<u32>().ok())
                        .map(|target| violation_u32(col.as_ref(), target, &rule.op)),
                    DataType::Float64 => col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .and_then(|_| rule.val_str.parse::<f64>().ok())
                        .map(|target| violation_float(col.as_ref(), target, &rule.op)),
                    DataType::Float32 => col
                        .as_any()
                        .downcast_ref::<arrow::array::Float32Array>()
                        .and_then(|_| rule.val_str.parse::<f32>().ok())
                        .map(|target| violation_float32(col.as_ref(), target, &rule.op)),
                    DataType::Utf8 => col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|_| violation_str(col.as_ref(), &rule.val_str, &rule.op)),
                    DataType::LargeUtf8 => col
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                        .map(|_| violation_large_str(col.as_ref(), &rule.val_str, &rule.op)),
                    _ => None,
                };

                if let Some(violation) = violation {
                    // clean = clean AND NOT violation; violation is null-free so AND stays null-free.
                    clean_mask = and(&clean_mask, &not(&violation)?)?;
                    let one_hot = cast(&violation, &DataType::UInt64)?;
                    let one_hot = one_hot
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .expect("UInt64 one-hot array")
                        .clone();
                    let chunk_idx = rule_idx / 64;
                    let shift = (rule_idx % 64) as u64;
                    let contrib = bitwise_shift_left_scalar(&one_hot, shift)?;
                    error_chunks[chunk_idx] = bitwise_or(&error_chunks[chunk_idx], &contrib)?;
                }
            }
        }

        let clean_bool = clean_mask;
        let trash_bool = not(&clean_bool)?;

        let clean_batch = filter_record_batch(batch, &clean_bool)?;
        let trash_filtered_base = filter_record_batch(batch, &trash_bool)?;

        let trash_error_codes_c0 = filter(&error_chunks[0], &trash_bool)?;

        let mut trash_fields = trash_filtered_base.schema().fields().to_vec();
        let mut trash_columns = trash_filtered_base.columns().to_vec();

        // Primary audit_error_code column (first 64 rules chunk) for 100% backward compatibility
        trash_fields.push(Field::new("audit_error_code", DataType::UInt64, true).into());
        trash_columns.push(trash_error_codes_c0);

        // When rules exceed 64, append an audit_violated_rules List<UInt32> column
        // detailing all violated rule indices per trash row.
        if rules.len() > 64 {
            let mut list_builder = arrow::array::builder::ListBuilder::new(
                arrow::array::builder::UInt32Builder::new(),
            );

            for row_idx in 0..total_rows {
                if trash_bool.value(row_idx) {
                    for (c, chunk) in error_chunks.iter().enumerate() {
                        let mut val = if chunk.is_null(row_idx) { 0 } else { chunk.value(row_idx) };
                        while val > 0 {
                            let bit = val.trailing_zeros();
                            let rule_id = (c * 64 + bit as usize) as u32;
                            list_builder.values().append_value(rule_id);
                            val &= val - 1; // Clear lowest set bit
                        }
                    }
                    list_builder.append(true);
                }
            }

            let violated_rules_array = Arc::new(list_builder.finish());
            trash_fields.push(
                Field::new(
                    "audit_violated_rules",
                    DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
                    true,
                )
                .into(),
            );
            trash_columns.push(violated_rules_array);
        }

        let trash_schema = Arc::new(Schema::new(trash_fields));
        let trash_batch = RecordBatch::try_new(trash_schema, trash_columns)?;

        Ok((clean_batch, trash_batch))
    }
}

/// Violation mask: null OR NOT(cmp(col, target)) — `is_null` anchors the OR so the
/// result is null-free and safe to reuse as a boolean filter.
fn violation_int(col: &dyn Array, target: i64, op: &Operator) -> BooleanArray {
    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
    let cmp = match op {
        Operator::Gt => gt(arr, &Int64Array::new_scalar(target)),
        Operator::Gte => gt_eq(arr, &Int64Array::new_scalar(target)),
        Operator::Lt => lt(arr, &Int64Array::new_scalar(target)),
        Operator::Lte => lt_eq(arr, &Int64Array::new_scalar(target)),
        Operator::Eq => eq(arr, &Int64Array::new_scalar(target)),
        Operator::Neq => neq(arr, &Int64Array::new_scalar(target)),
    };
    or(&is_null(arr).unwrap(), &not(&cmp.unwrap()).unwrap()).unwrap()
}

fn violation_int32(col: &dyn Array, target: i32, op: &Operator) -> BooleanArray {
    let arr = col.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
    let cmp = match op {
        Operator::Gt => gt(arr, &arrow::array::Int32Array::new_scalar(target)),
        Operator::Gte => gt_eq(arr, &arrow::array::Int32Array::new_scalar(target)),
        Operator::Lt => lt(arr, &arrow::array::Int32Array::new_scalar(target)),
        Operator::Lte => lt_eq(arr, &arrow::array::Int32Array::new_scalar(target)),
        Operator::Eq => eq(arr, &arrow::array::Int32Array::new_scalar(target)),
        Operator::Neq => neq(arr, &arrow::array::Int32Array::new_scalar(target)),
    };
    or(&is_null(arr).unwrap(), &not(&cmp.unwrap()).unwrap()).unwrap()
}

fn violation_u64(col: &dyn Array, target: u64, op: &Operator) -> BooleanArray {
    let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
    let cmp = match op {
        Operator::Gt => gt(arr, &UInt64Array::new_scalar(target)),
        Operator::Gte => gt_eq(arr, &UInt64Array::new_scalar(target)),
        Operator::Lt => lt(arr, &UInt64Array::new_scalar(target)),
        Operator::Lte => lt_eq(arr, &UInt64Array::new_scalar(target)),
        Operator::Eq => eq(arr, &UInt64Array::new_scalar(target)),
        Operator::Neq => neq(arr, &UInt64Array::new_scalar(target)),
    };
    or(&is_null(arr).unwrap(), &not(&cmp.unwrap()).unwrap()).unwrap()
}

fn violation_u32(col: &dyn Array, target: u32, op: &Operator) -> BooleanArray {
    let arr = col.as_any().downcast_ref::<arrow::array::UInt32Array>().unwrap();
    let cmp = match op {
        Operator::Gt => gt(arr, &arrow::array::UInt32Array::new_scalar(target)),
        Operator::Gte => gt_eq(arr, &arrow::array::UInt32Array::new_scalar(target)),
        Operator::Lt => lt(arr, &arrow::array::UInt32Array::new_scalar(target)),
        Operator::Lte => lt_eq(arr, &arrow::array::UInt32Array::new_scalar(target)),
        Operator::Eq => eq(arr, &arrow::array::UInt32Array::new_scalar(target)),
        Operator::Neq => neq(arr, &arrow::array::UInt32Array::new_scalar(target)),
    };
    or(&is_null(arr).unwrap(), &not(&cmp.unwrap()).unwrap()).unwrap()
}

fn violation_float(col: &dyn Array, target: f64, op: &Operator) -> BooleanArray {
    let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
    let cmp = match op {
        Operator::Gt => gt(arr, &Float64Array::new_scalar(target)),
        Operator::Gte => gt_eq(arr, &Float64Array::new_scalar(target)),
        Operator::Lt => lt(arr, &Float64Array::new_scalar(target)),
        Operator::Lte => lt_eq(arr, &Float64Array::new_scalar(target)),
        Operator::Eq => eq(arr, &Float64Array::new_scalar(target)),
        Operator::Neq => neq(arr, &Float64Array::new_scalar(target)),
    };
    or(&is_null(arr).unwrap(), &not(&cmp.unwrap()).unwrap()).unwrap()
}

fn violation_float32(col: &dyn Array, target: f32, op: &Operator) -> BooleanArray {
    let arr = col.as_any().downcast_ref::<arrow::array::Float32Array>().unwrap();
    let cmp = match op {
        Operator::Gt => gt(arr, &arrow::array::Float32Array::new_scalar(target)),
        Operator::Gte => gt_eq(arr, &arrow::array::Float32Array::new_scalar(target)),
        Operator::Lt => lt(arr, &arrow::array::Float32Array::new_scalar(target)),
        Operator::Lte => lt_eq(arr, &arrow::array::Float32Array::new_scalar(target)),
        Operator::Eq => eq(arr, &arrow::array::Float32Array::new_scalar(target)),
        Operator::Neq => neq(arr, &arrow::array::Float32Array::new_scalar(target)),
    };
    or(&is_null(arr).unwrap(), &not(&cmp.unwrap()).unwrap()).unwrap()
}

fn violation_str(col: &dyn Array, target: &str, op: &Operator) -> BooleanArray {
    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
    let scalar = Scalar::new(StringArray::from(vec![target.to_string()]));
    let cmp = match op {
        Operator::Gt => gt(arr, &scalar),
        Operator::Gte => gt_eq(arr, &scalar),
        Operator::Lt => lt(arr, &scalar),
        Operator::Lte => lt_eq(arr, &scalar),
        Operator::Eq => eq(arr, &scalar),
        Operator::Neq => neq(arr, &scalar),
    };
    or(&is_null(arr).unwrap(), &not(&cmp.unwrap()).unwrap()).unwrap()
}

fn violation_large_str(col: &dyn Array, target: &str, op: &Operator) -> BooleanArray {
    let arr = col.as_any().downcast_ref::<arrow::array::LargeStringArray>().unwrap();
    let scalar = Scalar::new(arrow::array::LargeStringArray::from(vec![target.to_string()]));
    let cmp = match op {
        Operator::Gt => gt(arr, &scalar),
        Operator::Gte => gt_eq(arr, &scalar),
        Operator::Lt => lt(arr, &scalar),
        Operator::Lte => lt_eq(arr, &scalar),
        Operator::Eq => eq(arr, &scalar),
        Operator::Neq => neq(arr, &scalar),
    };
    or(&is_null(arr).unwrap(), &not(&cmp.unwrap()).unwrap()).unwrap()
}
