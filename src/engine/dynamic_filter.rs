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
    /// Per-rule violation masks are built with vectorized kernels; the only loop left
    /// walks the rules (not the rows).
    pub fn filter_batch_dynamic(
        &self,
        batch: &RecordBatch,
        rules: &[FilterRule],
    ) -> Result<(RecordBatch, RecordBatch), BazanError> {
        let total_rows = batch.num_rows();
        let mut clean_mask = BooleanArray::from(vec![true; total_rows]);
        let mut error_code_arr = UInt64Array::from(vec![0u64; total_rows]);

        for (rule_idx, rule) in rules.iter().enumerate() {
            if let Some(col) = batch.column_by_name(&rule.col_name) {
                let violation = match col.data_type() {
                    DataType::Int64 => col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .and_then(|_| rule.val_str.parse::<i64>().ok())
                        .map(|target| violation_int(&col, target, &rule.op)),
                    DataType::Float64 => col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .and_then(|_| rule.val_str.parse::<f64>().ok())
                        .map(|target| violation_float(&col, target, &rule.op)),
                    DataType::Utf8 => col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|_| violation_str(&col, &rule.val_str, &rule.op)),
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
                    let shift = (rule_idx % 64) as u64;
                    let contrib = bitwise_shift_left_scalar(&one_hot, shift)?;
                    error_code_arr = bitwise_or(&error_code_arr, &contrib)?;
                }
            }
        }

        let trash_bitmask = not(&clean_mask)?;

        let clean_batch = filter_record_batch(batch, &clean_mask)?;
        let trash_filtered_base = filter_record_batch(batch, &trash_bitmask)?;
        let trash_error_codes = filter(&error_code_arr, &trash_bitmask)?;

        let mut trash_fields = trash_filtered_base.schema().fields().to_vec();
        trash_fields.push(Field::new("audit_error_code", DataType::UInt64, false).into());
        let trash_schema = Arc::new(Schema::new(trash_fields));

        let mut trash_columns = trash_filtered_base.columns().to_vec();
        trash_columns.push(trash_error_codes);

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
