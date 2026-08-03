use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array, RecordBatch};
use arrow::compute::{filter_record_batch, not};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use anyhow::{anyhow, Result};

use crate::engine::MatrixEngine;

#[derive(Debug, Clone)]
pub enum Operator {
    Gt,     // >
    Gte,    // >=
    Lt,     // <
    Lte,    // <=
    Eq,     // ==
    Neq,    // !=
}

#[derive(Debug, Clone)]
pub struct FilterRule {
    pub col_name: String,
    pub op: Operator,
    pub val_str: String,
}

impl FilterRule {
    pub fn parse(expr: &str) -> Result<Self> {
        let expr = expr.trim();
        let ops = [(">=", Operator::Gte), ("<=", Operator::Lte), ("==", Operator::Eq), ("!=", Operator::Neq), (">", Operator::Gt), ("<", Operator::Lt)];

        for (op_str, op) in ops {
            if let Some(idx) = expr.find(op_str) {
                let col_name = expr[..idx].trim().to_string();
                let val_str = expr[idx + op_str.len()..].trim().trim_matches('\'').trim_matches('"').to_string();
                return Ok(FilterRule { col_name, op, val_str });
            }
        }

        Err(anyhow!("Invalid rule expression: '{}'. Format example: 'age >= 18'", expr))
    }
}

impl MatrixEngine {
    /// Evaluate dynamic rules on a RecordBatch and split into Clean and Trash RecordBatches
    pub fn filter_batch_dynamic(&self, batch: &RecordBatch, rules: &[FilterRule]) -> Result<(RecordBatch, RecordBatch)> {
        let total_rows = batch.num_rows();
        let mut clean_mask_builder = vec![true; total_rows];
        let mut error_code_builder = vec![0u64; total_rows];

        for (rule_idx, rule) in rules.iter().enumerate() {
            let bitmask_flag = 1u64 << (rule_idx % 64);
            
            if let Some(col) = batch.column_by_name(&rule.col_name) {
                let dt = col.data_type();
                match dt {
                    DataType::Int64 => {
                        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                            if let Ok(target) = rule.val_str.parse::<i64>() {
                                for i in 0..total_rows {
                                    if !arr.is_valid(i) || !eval_cmp(arr.value(i), target, &rule.op) {
                                        clean_mask_builder[i] = false;
                                        error_code_builder[i] |= bitmask_flag;
                                    }
                                }
                            }
                        }
                    }
                    DataType::Float64 => {
                        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                            if let Ok(target) = rule.val_str.parse::<f64>() {
                                for i in 0..total_rows {
                                    if !arr.is_valid(i) || !eval_cmp(arr.value(i), target, &rule.op) {
                                        clean_mask_builder[i] = false;
                                        error_code_builder[i] |= bitmask_flag;
                                    }
                                }
                            }
                        }
                    }
                    DataType::Utf8 => {
                        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                            let target = &rule.val_str;
                            for i in 0..total_rows {
                                if !arr.is_valid(i) || !eval_cmp(arr.value(i), target.as_str(), &rule.op) {
                                    clean_mask_builder[i] = false;
                                    error_code_builder[i] |= bitmask_flag;
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        let clean_bitmask = BooleanArray::from(clean_mask_builder);
        let trash_bitmask = not(&clean_bitmask)?;

        let clean_batch = filter_record_batch(batch, &clean_bitmask)?;
        let trash_filtered_base = filter_record_batch(batch, &trash_bitmask)?;

        let error_code_arr = UInt64Array::from(error_code_builder);
        let trash_error_codes = arrow::compute::filter(&error_code_arr, &trash_bitmask)?;

        let mut trash_fields = trash_filtered_base.schema().fields().to_vec();
        trash_fields.push(Field::new("audit_error_code", DataType::UInt64, false).into());
        let trash_schema = Arc::new(Schema::new(trash_fields));

        let mut trash_columns = trash_filtered_base.columns().to_vec();
        trash_columns.push(trash_error_codes);

        let trash_batch = RecordBatch::try_new(trash_schema, trash_columns)?;

        Ok((clean_batch, trash_batch))
    }
}

fn eval_cmp<T: PartialOrd>(val: T, target: T, op: &Operator) -> bool {
    match op {
        Operator::Gt => val > target,
        Operator::Gte => val >= target,
        Operator::Lt => val < target,
        Operator::Lte => val <= target,
        Operator::Eq => val == target,
        Operator::Neq => val != target,
    }
}
