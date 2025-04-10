use crate::error::Error;
use sqlparser::ast::Statement;
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;

const DIALECT: AnsiDialect = AnsiDialect {};

pub enum LogicalPlan {
    /// for DDLs we just use the sqlparser::ast::Statement
    DDL(Statement),
}
pub struct Query {
    // TODO: we probably should not just expose Statement from sqlparser
    pub stmts: Vec<Statement>,
}

impl Query {
    pub fn logical_plan(self) -> impl IntoIterator<Item = LogicalPlan> {
        self.stmts.into_iter().map(|s| match s {
            // TODO
            any => LogicalPlan::DDL(any),
        })
    }
}

pub fn parse(sql: &str) -> Result<Query, Error> {
    Ok(Query {
        stmts: Parser::parse_sql(&DIALECT, sql)?,
    })
}
