use crate::common::{FusionError, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| FusionError::Parser(e.to_string()))
}
