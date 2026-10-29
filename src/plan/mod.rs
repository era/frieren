use std::collections::HashSet;

use iceberg::expr::{BinaryExpression, Predicate, PredicateOperator, Reference};
use iceberg::spec::{Datum, NestedField, PrimitiveLiteral, PrimitiveType, Schema, Type};
use iceberg::TableIdent;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, Query, Select, SelectItem, SetExpr, UnaryOperator,
    Value, WildcardAdditionalOptions,
};

/// Errors produced while resolving a SQL query against an Iceberg schema.
#[derive(Debug, thiserror::Error, Clone, PartialEq)]
pub enum BindError {
    #[error("column `{column}` does not exist in table `{table}`")]
    UnknownColumn { column: String, table: String },

    #[error("type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("type `{0}` is not supported yet")]
    UnsupportedType(String),

    #[error("expression is not supported yet: {0}")]
    UnsupportedExpression(String),

    #[error("SELECT item is not supported yet: {0}")]
    UnsupportedSelectItem(String),

    #[error("literal `{value}` is not valid for type `{target}`")]
    InvalidLiteral { value: String, target: String },
}

/// A resolved output column. The name is the schema name; aliases are not bound yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputColumn {
    pub field_id: i32,
    pub name: String,
}

/// A typed, resolved expression. No SQL strings survive in this tree.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpr {
    Column {
        field_id: i32,
        name: String,
        data_type: PrimitiveType,
    },
    Literal(Datum),
    Binary {
        op: BoundOp,
        left: Box<BoundExpr>,
        right: Box<BoundExpr>,
    },
    IsNull {
        expr: Box<BoundExpr>,
        negated: bool,
    },
}

impl BoundExpr {
    pub fn data_type(&self) -> PrimitiveType {
        match self {
            BoundExpr::Column { data_type, .. } => data_type.clone(),
            BoundExpr::Literal(datum) => datum.data_type().clone(),
            BoundExpr::Binary { .. } | BoundExpr::IsNull { .. } => PrimitiveType::Boolean,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

impl BoundOp {
    fn is_comparison(self) -> bool {
        !matches!(self, BoundOp::And | BoundOp::Or)
    }

    fn flip(self) -> Self {
        match self {
            BoundOp::Eq => BoundOp::Eq,
            BoundOp::NotEq => BoundOp::NotEq,
            BoundOp::Lt => BoundOp::Gt,
            BoundOp::LtEq => BoundOp::GtEq,
            BoundOp::Gt => BoundOp::Lt,
            BoundOp::GtEq => BoundOp::LtEq,
            BoundOp::And => BoundOp::And,
            BoundOp::Or => BoundOp::Or,
        }
    }

    fn from_sql(op: &BinaryOperator) -> Result<Self, BindError> {
        match op {
            BinaryOperator::Eq => Ok(BoundOp::Eq),
            BinaryOperator::NotEq => Ok(BoundOp::NotEq),
            BinaryOperator::Lt => Ok(BoundOp::Lt),
            BinaryOperator::LtEq => Ok(BoundOp::LtEq),
            BinaryOperator::Gt => Ok(BoundOp::Gt),
            BinaryOperator::GtEq => Ok(BoundOp::GtEq),
            BinaryOperator::And => Ok(BoundOp::And),
            BinaryOperator::Or => Ok(BoundOp::Or),
            other => Err(BindError::UnsupportedExpression(format!(
                "operator `{other}`"
            ))),
        }
    }

    fn to_predicate_operator(self) -> Option<PredicateOperator> {
        match self {
            BoundOp::Eq => Some(PredicateOperator::Eq),
            BoundOp::NotEq => Some(PredicateOperator::NotEq),
            BoundOp::Lt => Some(PredicateOperator::LessThan),
            BoundOp::LtEq => Some(PredicateOperator::LessThanOrEq),
            BoundOp::Gt => Some(PredicateOperator::GreaterThan),
            BoundOp::GtEq => Some(PredicateOperator::GreaterThanOrEq),
            BoundOp::And | BoundOp::Or => None,
        }
    }
}

/// The part of a query that the engine knows how to execute.
#[derive(Debug, Clone)]
pub struct BoundScan {
    /// The snapshot this query is pinned to. `None` means the table has no snapshot.
    pub snapshot_id: Option<i64>,
    pub schema_id: i32,
    /// Field IDs to read, in output order followed by residual-only inputs.
    pub projection: Vec<i32>,
    /// The part of the filter Iceberg can evaluate.
    pub predicate: Option<Predicate>,
    /// The part of the filter the engine must evaluate itself.
    pub residual: Option<BoundExpr>,
}

#[derive(Debug, Clone)]
pub struct BoundPlan {
    pub scan: BoundScan,
    /// Columns returned to the user, in SELECT order.
    pub output: Vec<OutputColumn>,
}

/// Resolves a `SELECT` against one pinned Iceberg schema.
pub struct Binder<'a> {
    table: TableIdent,
    schema: &'a Schema,
    snapshot_id: Option<i64>,
}

impl<'a> Binder<'a> {
    pub fn new(table: TableIdent, schema: &'a Schema, snapshot_id: Option<i64>) -> Self {
        Self {
            table,
            schema,
            snapshot_id,
        }
    }

    pub fn bind(self, query: &Query) -> Result<BoundPlan, BindError> {
        if query.with.is_some() {
            return unsupported("WITH");
        }
        if query.order_by.is_some() {
            return unsupported("ORDER BY");
        }
        if query.limit.is_some() || !query.limit_by.is_empty() || query.offset.is_some() {
            return unsupported("LIMIT/OFFSET");
        }
        if query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
        {
            return unsupported("query clause");
        }

        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            _ => return unsupported("only a plain SELECT is supported"),
        };
        self.bind_select(select)
    }

    fn bind_select(self, select: &Select) -> Result<BoundPlan, BindError> {
        validate_select(select)?;

        let mut output = Vec::new();
        let mut scan_ids = Vec::new();
        let mut seen = HashSet::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let column = self.resolve_ident(ident)?;
                    let (field_id, name) = match column {
                        BoundExpr::Column { field_id, name, .. } => (field_id, name),
                        _ => unreachable!("resolve_ident always returns a column"),
                    };
                    push_column(&mut output, &mut scan_ids, &mut seen, field_id, name);
                }
                SelectItem::Wildcard(options) if is_plain_wildcard(options) => {
                    for field in self.schema.as_struct().fields() {
                        let column = self.column_for_field(field)?;
                        let (field_id, name) = match column {
                            BoundExpr::Column { field_id, name, .. } => (field_id, name),
                            _ => unreachable!("column_for_field always returns a column"),
                        };
                        push_column(&mut output, &mut scan_ids, &mut seen, field_id, name);
                    }
                }
                other => return Err(BindError::UnsupportedSelectItem(other.to_string())),
            }
        }

        let filter = select
            .selection
            .as_ref()
            .map(|expr| self.bind_expr(expr))
            .transpose()?;

        let (predicate, residual) = match filter {
            Some(expr) => split(&expr),
            None => (None, None),
        };

        if let Some(residual) = &residual {
            let mut residual_ids = Vec::new();
            collect_columns(residual, &mut residual_ids);
            for field_id in residual_ids {
                if seen.insert(field_id) {
                    scan_ids.push(field_id);
                }
            }
        }

        Ok(BoundPlan {
            scan: BoundScan {
                snapshot_id: self.snapshot_id,
                schema_id: self.schema.schema_id(),
                projection: scan_ids,
                predicate,
                residual,
            },
            output,
        })
    }

    fn resolve_ident(&self, ident: &Ident) -> Result<BoundExpr, BindError> {
        let field = if ident.quote_style.is_some() {
            self.schema.field_by_name(&ident.value)
        } else {
            self.schema.field_by_name_case_insensitive(&ident.value)
        }
        .ok_or_else(|| BindError::UnknownColumn {
            column: ident.value.clone(),
            table: self.table.to_string(),
        })?;

        self.column_for_field(field)
    }

    fn column_for_field(&self, field: &NestedField) -> Result<BoundExpr, BindError> {
        match field.field_type.as_ref() {
            Type::Primitive(data_type) => Ok(BoundExpr::Column {
                field_id: field.id,
                name: field.name.clone(),
                data_type: data_type.clone(),
            }),
            other => Err(BindError::UnsupportedType(format!("{other:?}"))),
        }
    }

    fn bind_expr(&self, expr: &Expr) -> Result<BoundExpr, BindError> {
        match expr {
            Expr::Identifier(ident) => self.resolve_ident(ident),
            Expr::Value(value) => Ok(BoundExpr::Literal(literal_from_value(&value.value)?)),
            Expr::BinaryOp { left, op, right } => self.bind_binary(left, op, right),
            Expr::Nested(inner) => self.bind_expr(inner),
            Expr::IsNull(inner) => Ok(BoundExpr::IsNull {
                expr: Box::new(self.bind_expr(inner)?),
                negated: false,
            }),
            Expr::IsNotNull(inner) => Ok(BoundExpr::IsNull {
                expr: Box::new(self.bind_expr(inner)?),
                negated: true,
            }),
            Expr::UnaryOp { op, expr } => self.bind_unary(op, expr),
            other => Err(BindError::UnsupportedExpression(other.to_string())),
        }
    }

    fn bind_unary(&self, op: &UnaryOperator, expr: &Expr) -> Result<BoundExpr, BindError> {
        match (op, expr) {
            (UnaryOperator::Plus, Expr::Value(value)) => {
                Ok(BoundExpr::Literal(literal_from_value(&value.value)?))
            }
            (UnaryOperator::Minus, Expr::Value(value)) => match &value.value {
                Value::Number(number, _) => {
                    Ok(BoundExpr::Literal(number_datum(&format!("-{number}"))?))
                }
                other => Err(BindError::UnsupportedExpression(format!("-{other}"))),
            },
            _ => Err(BindError::UnsupportedExpression(format!("{op} {expr}"))),
        }
    }

    fn bind_binary(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<BoundExpr, BindError> {
        let op = BoundOp::from_sql(op)?;
        let mut left = self.bind_expr(left)?;
        let mut right = self.bind_expr(right)?;

        if matches!(op, BoundOp::And | BoundOp::Or) {
            ensure_boolean(&left)?;
            ensure_boolean(&right)?;
        } else {
            match (&left, &right) {
                (BoundExpr::Column { data_type, .. }, BoundExpr::Literal(datum)) => {
                    right = BoundExpr::Literal(coerce_literal(datum.clone(), data_type)?);
                }
                (BoundExpr::Literal(datum), BoundExpr::Column { data_type, .. }) => {
                    left = BoundExpr::Literal(coerce_literal(datum.clone(), data_type)?);
                }
                (
                    BoundExpr::Column {
                        data_type: left_type,
                        ..
                    },
                    BoundExpr::Column {
                        data_type: right_type,
                        ..
                    },
                ) => {
                    if left_type != right_type {
                        return Err(BindError::TypeMismatch {
                            expected: format!("{left_type:?}"),
                            found: format!("{right_type:?}"),
                        });
                    }
                }
                (BoundExpr::Literal(left_datum), BoundExpr::Literal(right_datum)) => {
                    if left_datum.data_type() != right_datum.data_type() {
                        return Err(BindError::TypeMismatch {
                            expected: format!("{:?}", left_datum.data_type()),
                            found: format!("{:?}", right_datum.data_type()),
                        });
                    }
                }
                _ => {
                    return Err(BindError::UnsupportedExpression(
                        "comparison operands must be a column and a literal, or two columns"
                            .to_string(),
                    ))
                }
            }
        }

        Ok(BoundExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }
}

fn validate_select(select: &Select) -> Result<(), BindError> {
    if select.distinct.is_some() {
        return unsupported("SELECT DISTINCT");
    }
    if select.top.is_some() || select.into.is_some() || select.prewhere.is_some() {
        return unsupported("SELECT option");
    }
    if !select.lateral_views.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return unsupported("SELECT modifier");
    }
    if select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
    {
        return unsupported("SELECT clause");
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) if exprs.is_empty() => {}
        GroupByExpr::All(modifiers) if modifiers.is_empty() => {}
        _ => return unsupported("GROUP BY"),
    }
    if select.from.len() != 1 {
        return unsupported("exactly one FROM table");
    }
    if !select.from[0].joins.is_empty() {
        return unsupported("JOIN");
    }
    Ok(())
}

fn unsupported<T>(what: impl std::fmt::Display) -> Result<T, BindError> {
    Err(BindError::UnsupportedExpression(format!(
        "{what} is not supported yet"
    )))
}

fn is_plain_wildcard(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
}

fn push_column(
    output: &mut Vec<OutputColumn>,
    scan_ids: &mut Vec<i32>,
    seen: &mut HashSet<i32>,
    field_id: i32,
    name: String,
) {
    output.push(OutputColumn { field_id, name });
    if seen.insert(field_id) {
        scan_ids.push(field_id);
    }
}

fn ensure_boolean(expr: &BoundExpr) -> Result<(), BindError> {
    let data_type = expr.data_type();
    if data_type == PrimitiveType::Boolean {
        Ok(())
    } else {
        Err(BindError::TypeMismatch {
            expected: "Boolean".to_string(),
            found: format!("{data_type:?}"),
        })
    }
}

fn literal_from_value(value: &Value) -> Result<Datum, BindError> {
    match value {
        Value::Number(number, _) => number_datum(number),
        Value::Boolean(value) => Ok(Datum::bool(*value)),
        Value::Null => Err(BindError::UnsupportedExpression(
            "NULL literal; use IS NULL instead".to_string(),
        )),
        other => match other.clone().into_string() {
            Some(string) => Ok(Datum::string(string)),
            None => Err(BindError::UnsupportedExpression(format!(
                "literal `{other}`"
            ))),
        },
    }
}

fn number_datum(number: &str) -> Result<Datum, BindError> {
    if let Ok(value) = number.parse::<i32>() {
        Ok(Datum::int(value))
    } else if let Ok(value) = number.parse::<i64>() {
        Ok(Datum::long(value))
    } else if let Ok(value) = number.parse::<f64>() {
        Ok(Datum::double(value))
    } else {
        Err(BindError::InvalidLiteral {
            value: number.to_string(),
            target: "number".to_string(),
        })
    }
}

/// Coerce a literal to the type of the column it is compared with.
fn coerce_literal(datum: Datum, target: &PrimitiveType) -> Result<Datum, BindError> {
    use PrimitiveLiteral as Lit;

    let source = format!("{:?}", datum.data_type());
    let invalid = || BindError::InvalidLiteral {
        value: format!("{datum:?}"),
        target: format!("{target:?}"),
    };

    match (datum.literal(), target) {
        (Lit::Boolean(value), PrimitiveType::Boolean) => Ok(Datum::bool(*value)),
        (Lit::Int(value), PrimitiveType::Int) => Ok(Datum::int(*value)),
        (Lit::Int(value), PrimitiveType::Long) => Ok(Datum::long(*value as i64)),
        (Lit::Int(value), PrimitiveType::Float) => Ok(Datum::float(*value as f32)),
        (Lit::Int(value), PrimitiveType::Double) => Ok(Datum::double(*value as f64)),
        (Lit::Int(value), PrimitiveType::Date) => Ok(Datum::date(*value)),
        (Lit::Long(value), PrimitiveType::Long) => Ok(Datum::long(*value)),
        (Lit::Long(value), PrimitiveType::Int) => {
            i32::try_from(*value).map(Datum::int).map_err(|_| invalid())
        }
        (Lit::Long(value), PrimitiveType::Float) => Ok(Datum::float(*value as f32)),
        (Lit::Long(value), PrimitiveType::Double) => Ok(Datum::double(*value as f64)),
        (Lit::Float(value), PrimitiveType::Float) => Ok(Datum::float(value.0)),
        (Lit::Float(value), PrimitiveType::Double) => Ok(Datum::double(value.0 as f64)),
        (Lit::Double(value), PrimitiveType::Double) => Ok(Datum::double(value.0)),
        (Lit::Double(value), PrimitiveType::Float) => Ok(Datum::float(value.0 as f32)),
        (Lit::String(value), PrimitiveType::String) => Ok(Datum::string(value.clone())),
        (Lit::String(value), PrimitiveType::Int) => {
            value.parse::<i32>().map(Datum::int).map_err(|_| invalid())
        }
        (Lit::String(value), PrimitiveType::Long) => {
            value.parse::<i64>().map(Datum::long).map_err(|_| invalid())
        }
        (Lit::String(value), PrimitiveType::Float) => value
            .parse::<f32>()
            .map(Datum::float)
            .map_err(|_| invalid()),
        (Lit::String(value), PrimitiveType::Double) => value
            .parse::<f64>()
            .map(Datum::double)
            .map_err(|_| invalid()),
        (Lit::String(value), PrimitiveType::Date) => {
            Datum::date_from_str(value).map_err(|_| invalid())
        }
        (Lit::String(value), PrimitiveType::Boolean) => {
            Datum::bool_from_str(value).map_err(|_| invalid())
        }
        _ => Err(BindError::TypeMismatch {
            expected: format!("{target:?}"),
            found: source,
        }),
    }
}

fn comparison_predicate(op: BoundOp, name: &str, datum: &Datum) -> Option<Predicate> {
    let op = op.to_predicate_operator()?;
    Some(Predicate::Binary(BinaryExpression::new(
        op,
        Reference::new(name),
        datum.clone(),
    )))
}

/// Translates a bound expression into an Iceberg predicate when possible.
fn to_predicate(expr: &BoundExpr) -> Option<Predicate> {
    match expr {
        BoundExpr::Binary {
            op: BoundOp::And,
            left,
            right,
        } => Some(to_predicate(left)?.and(to_predicate(right)?)),
        BoundExpr::Binary {
            op: BoundOp::Or,
            left,
            right,
        } => Some(to_predicate(left)?.or(to_predicate(right)?)),
        BoundExpr::Binary { op, left, right } if op.is_comparison() => match (&**left, &**right) {
            (BoundExpr::Column { name, .. }, BoundExpr::Literal(datum)) => {
                comparison_predicate(*op, name, datum)
            }
            (BoundExpr::Literal(datum), BoundExpr::Column { name, .. }) => {
                comparison_predicate(op.flip(), name, datum)
            }
            _ => None,
        },
        BoundExpr::IsNull { expr, negated } => match &**expr {
            BoundExpr::Column { name, .. } => Some(if *negated {
                Reference::new(name).is_not_null()
            } else {
                Reference::new(name).is_null()
            }),
            _ => None,
        },
        BoundExpr::Literal(datum) => match datum.literal() {
            PrimitiveLiteral::Boolean(true) => Some(Predicate::AlwaysTrue),
            PrimitiveLiteral::Boolean(false) => Some(Predicate::AlwaysFalse),
            _ => None,
        },
        _ => None,
    }
}

/// Splits a filter into the part Iceberg can evaluate and the residual.
fn split(expr: &BoundExpr) -> (Option<Predicate>, Option<BoundExpr>) {
    match expr {
        BoundExpr::Binary {
            op: BoundOp::And,
            left,
            right,
        } => {
            let (left_predicate, left_residual) = split(left);
            let (right_predicate, right_residual) = split(right);
            (
                and_predicates(left_predicate, right_predicate),
                and_expressions(left_residual, right_residual),
            )
        }
        BoundExpr::Binary {
            op: BoundOp::Or,
            left,
            right,
        } => match (to_predicate(left), to_predicate(right)) {
            (Some(left), Some(right)) => (Some(left.or(right)), None),
            _ => (None, Some(expr.clone())),
        },
        _ => match to_predicate(expr) {
            Some(predicate) => (Some(predicate), None),
            None => (None, Some(expr.clone())),
        },
    }
}

fn and_predicates(left: Option<Predicate>, right: Option<Predicate>) -> Option<Predicate> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.and(right)),
        (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
        (None, None) => None,
    }
}

fn and_expressions(left: Option<BoundExpr>, right: Option<BoundExpr>) -> Option<BoundExpr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(BoundExpr::Binary {
            op: BoundOp::And,
            left: Box::new(left),
            right: Box::new(right),
        }),
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (None, None) => None,
    }
}

fn collect_columns(expr: &BoundExpr, out: &mut Vec<i32>) {
    match expr {
        BoundExpr::Column { field_id, .. } => out.push(*field_id),
        BoundExpr::Literal(_) => {}
        BoundExpr::Binary { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        BoundExpr::IsNull { expr, .. } => collect_columns(expr, out),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::NestedField;
    use sqlparser::ast::Statement;

    use super::*;

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::required(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    3,
                    "big",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap()
    }

    fn bind(sql: &str) -> Result<BoundPlan, BindError> {
        let statements = crate::parse(sql).unwrap();
        let query = match &statements[0] {
            Statement::Query(query) => query,
            other => panic!("expected a query, got {other:?}"),
        };
        let schema = schema();
        Binder::new(
            TableIdent::new(
                iceberg::NamespaceIdent::new("db".to_string()),
                "t".to_string(),
            ),
            &schema,
            Some(7),
        )
        .bind(query)
    }

    #[test]
    fn binds_projection_in_select_order() {
        let plan = bind("select name, id from t").unwrap();
        assert_eq!(
            plan.output,
            vec![
                OutputColumn {
                    field_id: 2,
                    name: "name".to_string()
                },
                OutputColumn {
                    field_id: 1,
                    name: "id".to_string()
                },
            ]
        );
        assert_eq!(plan.scan.projection, vec![2, 1]);
        assert_eq!(plan.scan.schema_id, 0);
        assert_eq!(plan.scan.snapshot_id, Some(7));
        assert!(plan.scan.predicate.is_none());
        assert!(plan.scan.residual.is_none());
    }

    #[test]
    fn wildcard_expands_in_schema_order() {
        let plan = bind("select * from t").unwrap();
        assert_eq!(
            plan.output.iter().map(|c| c.field_id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn pushdown_does_not_project_filter_columns() {
        let plan = bind("select name from t where id = 2").unwrap();
        assert_eq!(plan.scan.projection, vec![2]);
        assert_eq!(plan.scan.predicate.as_ref().unwrap().to_string(), "id = 2");
        assert!(plan.scan.residual.is_none());
    }

    #[test]
    fn literal_is_coerced_to_column_type() {
        let plan = bind("select big from t where big = 2").unwrap();
        match plan.scan.predicate.as_ref().unwrap() {
            Predicate::Binary(expr) => {
                assert_eq!(expr.literal().data_type(), &PrimitiveType::Long);
            }
            other => panic!("expected a binary predicate, got {other}"),
        }
    }

    #[test]
    fn column_comparison_becomes_residual_and_is_projected() {
        let plan = bind("select name from t where id = id").unwrap();
        assert!(plan.scan.predicate.is_none());
        assert!(plan.scan.residual.is_some());
        assert_eq!(plan.scan.projection, vec![2, 1]);
        assert_eq!(plan.output.len(), 1);
    }

    #[test]
    fn and_splits_pushdown_from_residual() {
        let plan = bind("select name from t where id = id and name = 'a'").unwrap();
        assert_eq!(
            plan.scan.predicate.as_ref().unwrap().to_string(),
            "name = \"a\""
        );
        assert!(plan.scan.residual.is_some());
        assert_eq!(plan.scan.projection, vec![2, 1]);
    }

    #[test]
    fn rejects_unknown_columns() {
        let error = bind("select nope from t").unwrap_err();
        assert!(matches!(error, BindError::UnknownColumn { .. }));
    }

    #[test]
    fn rejects_invalid_literal_for_column_type() {
        let error = bind("select name from t where id = 'nope'").unwrap_err();
        assert!(matches!(error, BindError::InvalidLiteral { .. }));
    }

    #[test]
    fn rejects_unsupported_clauses_instead_of_panicking() {
        assert!(bind("select name from t order by id").is_err());
        assert!(bind("select count(*) from t").is_err());
        assert!(bind("select name from t group by name").is_err());
    }
}
