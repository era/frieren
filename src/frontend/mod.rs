use crate::error::Error;
use sqlparser::ast::Statement;
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;

const DIALECT: AnsiDialect = AnsiDialect {};
pub struct Query {
    // TODO: we probably should not just expose Statement from sqlparser
    pub stmts: Vec<Statement>,
}

pub fn parse(sql: &str) -> Result<Query, Error> {
    Ok(Query {
        stmts: Parser::parse_sql(&DIALECT, sql)?,
    })
}
