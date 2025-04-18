use crate::backend::Context;
use crate::Error;
use iceberg::expr::Predicate;
use iceberg::spec::PrimitiveType;
use iceberg::{NamespaceIdent, TableIdent};
use sqlparser::ast::{DataType, Expr, SelectItem, TableFactor};

pub fn type_for(sql_type: &DataType) -> Result<PrimitiveType, Error> {
    match sql_type {
        DataType::Boolean | DataType::Bool => Ok(PrimitiveType::Boolean),
        DataType::Int32 => Ok(PrimitiveType::Int),
        DataType::Int64 => Ok(PrimitiveType::Long),
        DataType::Date => Ok(PrimitiveType::Date),
        DataType::Float32 => Ok(PrimitiveType::Float),
        DataType::Float64 => Ok(PrimitiveType::Double),
        DataType::String(_) => Ok(PrimitiveType::String),
        _ => Err(Error::NotSupportedSql(
            "column type is not supported".to_string(),
        )),
    }
}

pub fn object_name_to_namespace(
    ctx: Context,
    db_name: sqlparser::ast::ObjectName,
) -> Result<NamespaceIdent, Error> {
    // we will create a namespace my_cool_schema with a subnamespace called `my_cool_db`.
    // FIXME: should look at context to see if the user already used something like `use my_cool_schema;`
    // FIXME: on previous statements
    Ok(NamespaceIdent::from_vec(
        db_name
            .0
            .into_iter()
            // it's safe to unwrap as_indent because as now there will always be one there
            .map(|i| i.as_ident().unwrap().value.clone())
            .collect(),
    )?)
}

pub fn object_name_to_table(
    ctx: Context,
    table_name: sqlparser::ast::ObjectName,
) -> Result<TableIdent, Error> {
    if table_name.0.len() > 1 {
        return Err(Error::NotSupportedSql(
            "Syntax schema.table not supported yet".to_string(),
        ));
    }

    let namespace = match ctx.namespace {
        None => return Err(Error::NotSupportedSql("Must first select a database by using `use database` or a schema by using `use schema`".to_string())),
        Some(namespace) => namespace,
    };

    Ok(TableIdent::new(
        namespace.name().clone(),
        table_name
            .0
            .into_iter()
            // it's safe to unwrap as_indent because as now there will always be one there
            .map(|i| i.as_ident().unwrap().value.clone())
            .collect(),
    ))
}

//FIXME
pub fn select_item(item: SelectItem) -> String {
    match item {
        SelectItem::UnnamedExpr(ex) => expr(ex),
        _ => unimplemented!(),
    }
}
//FIXME
pub fn expr(item: Expr) -> String {
    match item {
        Expr::Identifier(identifier) => identifier.value,
        _ => unimplemented!(),
    }
}

//FIXME
pub fn expr_predicates(expr: Option<Expr>) -> Predicate {
    if let Some(expr) = expr {
        unimplemented!()
    } else {
        Predicate::AlwaysTrue
    }
}

pub fn table_factor(ctx: Context, t: TableFactor) -> Result<TableIdent, Error> {
    match t {
        TableFactor::Table { name, .. } => object_name_to_table(ctx, name),
        _ => unimplemented!(),
    }
}
