mod backend;
mod error;
mod server;

use sqlparser::ast::Statement;
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;

pub use error::Error;

/// Parse SQL text into `sqlparser` statements.
///
/// Parsing is syntax-only: names, types, and expressions are resolved by the
/// planner against the Iceberg schema, never here.
pub fn parse(sql: &str) -> Result<Vec<Statement>, Error> {
    Ok(Parser::parse_sql(&AnsiDialect {}, sql)?)
}
