use crate::error::Error;
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray},
    compute::{
        self,
        kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq},
    },
    record_batch::RecordBatch,
};

use sqlparser::ast::{BinaryOperator, Expr, Value};
use std::sync::Arc;

type Result<T> = std::result::Result<T, Error>;

pub fn filter_record_batch(batch: &RecordBatch, filter_expr: &Expr) -> Result<RecordBatch> {
    let filter_array = sql_expr_to_arrow_filter(batch, filter_expr)?;
    Ok(compute::filter_record_batch(batch, &filter_array)?)
}

fn sql_expr_to_arrow_filter(batch: &RecordBatch, expr: &Expr) -> Result<BooleanArray> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_arr = evaluate_expr(batch, left)?;
            let right_arr = evaluate_expr(batch, right)?;
            apply_comparison_op(left_arr, right_arr, op)
        }
        Expr::Identifier(ident) => {
            let column = batch.column_by_name(&ident.value).unwrap();
            Ok(compute::is_not_null(column.as_ref())?)
        }
        Expr::IsNull(expr) => {
            let arr = evaluate_expr(batch, expr)?;
            Ok(compute::is_null(arr.as_ref())?)
        }
        Expr::IsNotNull(expr) => {
            let arr = evaluate_expr(batch, expr)?;
            Ok(compute::is_not_null(arr.as_ref())?)
        }
        _ => unimplemented!(),
    }
}

fn apply_comparison_op(
    left: ArrayRef,
    right: ArrayRef,
    op: &BinaryOperator,
) -> Result<BooleanArray> {
    //FIXME: a bunch of unwrap
    match op {
        BinaryOperator::Eq => Ok(eq(&left, &right)?),
        BinaryOperator::NotEq => Ok(neq(&left, &right)?),
        BinaryOperator::Lt => Ok(lt(&left, &right)?),
        BinaryOperator::LtEq => Ok(lt_eq(&left, &right)?),
        BinaryOperator::Gt => Ok(gt(&left, &right)?),
        BinaryOperator::GtEq => Ok(gt_eq(&left, &right)?),
        BinaryOperator::And => {
            let left_bool = left.as_any().downcast_ref::<BooleanArray>().unwrap();
            let right_bool = right.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(compute::and(left_bool, right_bool)?)
        }
        BinaryOperator::Or => {
            let left_bool = left.as_any().downcast_ref::<BooleanArray>().unwrap();
            let right_bool = right.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(compute::or(left_bool, right_bool)?)
        }
        _ => unimplemented!(),
    }
}

fn evaluate_expr(batch: &RecordBatch, expr: &Expr) -> Result<ArrayRef> {
    match expr {
        Expr::Identifier(ident) => Ok(batch
            .column_by_name(&ident.value)
            .map(|col| col.clone())
            .unwrap()),
        Expr::Value(ref value) => match &value.value {
            Value::Number(n, _) => {
                if let Ok(int_val) = n.parse::<i32>() {
                    Ok(Arc::new(Int32Array::from_value(int_val, batch.num_rows())))
                } else if let Ok(float_val) = n.parse::<f64>() {
                    Ok(Arc::new(Float64Array::from_value(
                        float_val,
                        batch.num_rows(),
                    )))
                } else {
                    Err(Error::NotSupportedSql("could not parse value".to_string()))
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(Arc::new(StringArray::from(vec![
                    s.as_str();
                    batch.num_rows()
                ])))
            }
            _ => Err(Error::NotSupportedSql(
                "Unsupported literal value".to_string(),
            )),
        },
        _ => Err(Error::NotSupportedSql(
            "Unsupported expression for evaluation".to_string(),
        )),
    }
}
