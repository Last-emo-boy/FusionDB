use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;
use crate::common::{Result, FusionError};

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| FusionError::Parser(e.to_string()))
}
