use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray,
};
use arrow::compute::{self, kernels::cmp};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType};

use crate::plan::{BoundExpr, BoundOp, OutputColumn};
use crate::Error;

/// Evaluates the residual filter and returns only matching rows.
pub fn filter_record_batch(batch: &RecordBatch, expr: &BoundExpr) -> Result<RecordBatch, Error> {
    let mask = evaluate(batch, expr)?;
    let mask = as_boolean(&mask)?;
    Ok(compute::filter_record_batch(batch, mask)?)
}

/// Reduces a scanned batch to the SQL output columns, in plan order.
pub fn project_output(batch: &RecordBatch, output: &[OutputColumn]) -> Result<RecordBatch, Error> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(output.len());
    let mut columns = Vec::with_capacity(output.len());

    for column in output {
        let index = schema.index_of(&column.name)?;
        fields.push(schema.field(index).clone());
        columns.push(batch.column(index).clone());
    }

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(fields)),
        columns,
    )?)
}

fn evaluate(batch: &RecordBatch, expr: &BoundExpr) -> Result<ArrayRef, Error> {
    match expr {
        BoundExpr::Column { name, .. } => batch.column_by_name(name).cloned().ok_or_else(|| {
            Error::Internal(format!(
                "planned column `{name}` is missing from the scanned batch"
            ))
        }),
        BoundExpr::Literal(datum) => literal_array(datum, batch.num_rows()),
        BoundExpr::Binary { op, left, right } => {
            let left = evaluate(batch, left)?;
            let right = evaluate(batch, right)?;
            match op {
                BoundOp::And => Ok(Arc::new(compute::and_kleene(
                    as_boolean(&left)?,
                    as_boolean(&right)?,
                )?)),
                BoundOp::Or => Ok(Arc::new(compute::or_kleene(
                    as_boolean(&left)?,
                    as_boolean(&right)?,
                )?)),
                BoundOp::Eq => Ok(Arc::new(cmp::eq(&left, &right)?)),
                BoundOp::NotEq => Ok(Arc::new(cmp::neq(&left, &right)?)),
                BoundOp::Lt => Ok(Arc::new(cmp::lt(&left, &right)?)),
                BoundOp::LtEq => Ok(Arc::new(cmp::lt_eq(&left, &right)?)),
                BoundOp::Gt => Ok(Arc::new(cmp::gt(&left, &right)?)),
                BoundOp::GtEq => Ok(Arc::new(cmp::gt_eq(&left, &right)?)),
            }
        }
        BoundExpr::IsNull { expr, negated } => {
            let array = evaluate(batch, expr)?;
            let result = if *negated {
                compute::is_not_null(array.as_ref())?
            } else {
                compute::is_null(array.as_ref())?
            };
            Ok(Arc::new(result))
        }
    }
}

fn as_boolean(array: &ArrayRef) -> Result<&BooleanArray, Error> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            Error::Internal(format!(
                "expected a boolean array during filter evaluation, got {:?}",
                array.data_type()
            ))
        })
}

fn literal_array(datum: &Datum, len: usize) -> Result<ArrayRef, Error> {
    match datum.data_type() {
        PrimitiveType::Boolean => match datum.literal() {
            PrimitiveLiteral::Boolean(value) => Ok(Arc::new(BooleanArray::from(vec![*value; len]))),
            other => Err(unexpected_literal(datum, other)),
        },
        PrimitiveType::Int => match datum.literal() {
            PrimitiveLiteral::Int(value) => Ok(Arc::new(Int32Array::from_value(*value, len))),
            other => Err(unexpected_literal(datum, other)),
        },
        PrimitiveType::Long => match datum.literal() {
            PrimitiveLiteral::Long(value) => Ok(Arc::new(Int64Array::from_value(*value, len))),
            other => Err(unexpected_literal(datum, other)),
        },
        PrimitiveType::Float => match datum.literal() {
            PrimitiveLiteral::Float(value) => Ok(Arc::new(Float32Array::from_value(value.0, len))),
            other => Err(unexpected_literal(datum, other)),
        },
        PrimitiveType::Double => match datum.literal() {
            PrimitiveLiteral::Double(value) => Ok(Arc::new(Float64Array::from_value(value.0, len))),
            other => Err(unexpected_literal(datum, other)),
        },
        PrimitiveType::String => match datum.literal() {
            PrimitiveLiteral::String(value) => {
                Ok(Arc::new(StringArray::from(vec![value.as_str(); len])))
            }
            other => Err(unexpected_literal(datum, other)),
        },
        PrimitiveType::Date => match datum.literal() {
            PrimitiveLiteral::Int(days) => Ok(Arc::new(Date32Array::from_value(*days, len))),
            other => Err(unexpected_literal(datum, other)),
        },
        other => Err(Error::Internal(format!(
            "literal type {other:?} reached execution"
        ))),
    }
}

fn unexpected_literal(datum: &Datum, literal: &PrimitiveLiteral) -> Error {
    Error::Internal(format!(
        "literal value {literal:?} does not match declared type {:?}",
        datum.data_type()
    ))
}
