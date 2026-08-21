use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use crate::engine::MatrixEngine;
use crate::error::BazanError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

macro_rules! eval_primitive_rule {
    ($arr_type:ty, $target_type:ty, $col:expr, $rule:expr, $total_rows:expr, $bit:expr, $target_chunk:expr, $clean_bits:expr) => {
        if let Some(arr) = $col.as_any().downcast_ref::<$arr_type>() {
            if let Ok(target) = $rule.val_str.parse::<$target_type>() {
                let values = arr.values();
                let nulls = arr.nulls();
                if let Some(null_buf) = nulls {
                    match $rule.op {
                        Operator::Gt => {
                            for i in 0..$total_rows {
                                if null_buf.is_null(i) || !(values[i] > target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                        Operator::Gte => {
                            for i in 0..$total_rows {
                                if null_buf.is_null(i) || !(values[i] >= target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                        Operator::Lt => {
                            for i in 0..$total_rows {
                                if null_buf.is_null(i) || !(values[i] < target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                        Operator::Lte => {
                            for i in 0..$total_rows {
                                if null_buf.is_null(i) || !(values[i] <= target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                        Operator::Eq => {
                            for i in 0..$total_rows {
                                if null_buf.is_null(i) || !(values[i] == target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                        Operator::Neq => {
                            for i in 0..$total_rows {
                                if null_buf.is_null(i) || !(values[i] != target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                    }
                } else {
                    // Fast path without null checks (100% LLVM auto-vectorizable)
                    match $rule.op {
                        Operator::Gt => {
                            for i in 0..$total_rows {
                                if !(values[i] > target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                        Operator::Gte => {
                            for i in 0..$total_rows {
                                if !(values[i] >= target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                        Operator::Lt => {
                            for i in 0..$total_rows {
                                if !(values[i] < target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                        Operator::Lte => {
                            for i in 0..$total_rows {
                                if !(values[i] <= target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                        Operator::Eq => {
                            for i in 0..$total_rows {
                                if !(values[i] == target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                        Operator::Neq => {
                            for i in 0..$total_rows {
                                if !(values[i] != target) {
                                    $target_chunk[i] |= $bit;
                                    $clean_bits[i] = false;
                                }
                            }
                        }
                    }
                }
            }
        }
    };
}

macro_rules! eval_string_rule {
    ($arr_type:ty, $col:expr, $rule:expr, $total_rows:expr, $bit:expr, $target_chunk:expr, $clean_bits:expr) => {
        if let Some(arr) = $col.as_any().downcast_ref::<$arr_type>() {
            let target = &$rule.val_str;
            let nulls = arr.nulls();
            if let Some(null_buf) = nulls {
                for i in 0..$total_rows {
                    if null_buf.is_null(i) {
                        $target_chunk[i] |= $bit;
                        $clean_bits[i] = false;
                    } else {
                        let val = arr.value(i);
                        let passed = match $rule.op {
                            Operator::Gt => val > target.as_str(),
                            Operator::Gte => val >= target.as_str(),
                            Operator::Lt => val < target.as_str(),
                            Operator::Lte => val <= target.as_str(),
                            Operator::Eq => val == target.as_str(),
                            Operator::Neq => val != target.as_str(),
                        };
                        if !passed {
                            $target_chunk[i] |= $bit;
                            $clean_bits[i] = false;
                        }
                    }
                }
            } else {
                for i in 0..$total_rows {
                    let val = arr.value(i);
                    let passed = match $rule.op {
                        Operator::Gt => val > target.as_str(),
                        Operator::Gte => val >= target.as_str(),
                        Operator::Lt => val < target.as_str(),
                        Operator::Lte => val <= target.as_str(),
                        Operator::Eq => val == target.as_str(),
                        Operator::Neq => val != target.as_str(),
                    };
                    if !passed {
                        $target_chunk[i] |= $bit;
                        $clean_bits[i] = false;
                    }
                }
            }
        }
    };
}

impl MatrixEngine {
    /// Evaluate dynamic rules on a RecordBatch and split into Clean and Trash RecordBatches.
    /// Supports arbitrary number of rules (> 64) with direct in-place zero-allocation bitmasking
    /// without bit collisions.
    pub fn filter_batch_dynamic(
        &self,
        batch: &RecordBatch,
        rules: &[FilterRule],
    ) -> Result<(RecordBatch, RecordBatch), BazanError> {
        let total_rows = batch.num_rows();
        let mut clean_bits = vec![true; total_rows];

        let num_chunks = ((rules.len() + 63) / 64).max(1);
        let mut error_chunks_raw: Vec<Vec<u64>> = vec![vec![0u64; total_rows]; num_chunks];

        for (rule_idx, rule) in rules.iter().enumerate() {
            let chunk_idx = rule_idx / 64;
            let bit = 1u64 << (rule_idx % 64);
            let target_chunk = &mut error_chunks_raw[chunk_idx];

            if let Some(col) = batch.column_by_name(&rule.col_name) {
                match col.data_type() {
                    DataType::Int64 => {
                        eval_primitive_rule!(Int64Array, i64, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    DataType::Int32 => {
                        eval_primitive_rule!(Int32Array, i32, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    DataType::Int16 => {
                        eval_primitive_rule!(Int16Array, i16, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    DataType::Int8 => {
                        eval_primitive_rule!(Int8Array, i8, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    DataType::UInt64 => {
                        eval_primitive_rule!(UInt64Array, u64, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    DataType::UInt32 => {
                        eval_primitive_rule!(UInt32Array, u32, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    DataType::UInt16 => {
                        eval_primitive_rule!(UInt16Array, u16, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    DataType::UInt8 => {
                        eval_primitive_rule!(UInt8Array, u8, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    DataType::Float64 => {
                        eval_primitive_rule!(Float64Array, f64, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    DataType::Float32 => {
                        eval_primitive_rule!(Float32Array, f32, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    DataType::Utf8 => {
                        eval_string_rule!(StringArray, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    DataType::LargeUtf8 => {
                        eval_string_rule!(LargeStringArray, col, rule, total_rows, bit, target_chunk, clean_bits);
                    }
                    _ => {}
                }
            }
        }

        let clean_bool = BooleanArray::from(clean_bits.clone());
        let trash_bool = BooleanArray::from_iter(clean_bits.iter().map(|&c| !c));

        let clean_batch = filter_record_batch(batch, &clean_bool)?;
        let trash_filtered_base = filter_record_batch(batch, &trash_bool)?;

        // Primary audit_error_code column (first 64 rules chunk) for 100% backward compatibility
        let mut trash_error_c0_builder = arrow::array::UInt64Builder::with_capacity(trash_filtered_base.num_rows());
        for (i, &is_clean) in clean_bits.iter().enumerate() {
            if !is_clean {
                trash_error_c0_builder.append_value(error_chunks_raw[0][i]);
            }
        }
        let trash_error_codes_c0 = Arc::new(trash_error_c0_builder.finish());

        let mut trash_fields = trash_filtered_base.schema().fields().to_vec();
        let mut trash_columns = trash_filtered_base.columns().to_vec();

        trash_fields.push(Field::new("audit_error_code", DataType::UInt64, true).into());
        trash_columns.push(trash_error_codes_c0);

        // When rules exceed 64, append an audit_violated_rules List<UInt32> column
        if rules.len() > 64 {
            let mut list_builder = arrow::array::builder::ListBuilder::new(
                arrow::array::builder::UInt32Builder::new(),
            );

            for (row_idx, &is_clean) in clean_bits.iter().enumerate() {
                if !is_clean {
                    for (c, chunk) in error_chunks_raw.iter().enumerate() {
                        let mut val = chunk[row_idx];
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
