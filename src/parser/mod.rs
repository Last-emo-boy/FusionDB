use crate::common::{FusionError, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| FusionError::Parser(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_table() {
        let stmts = parse_sql("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::CreateTable(_)));
    }

    #[test]
    fn test_parse_insert() {
        let stmts = parse_sql("INSERT INTO t VALUES (1, 'hello')").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::Insert(_)));
    }

    #[test]
    fn test_parse_select() {
        let stmts = parse_sql("SELECT * FROM t WHERE id = 1").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::Query(_)));
    }

    #[test]
    fn test_parse_multiple_statements() {
        let stmts = parse_sql("SELECT 1; SELECT 2").unwrap();
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_parse_invalid_sql() {
        let result = parse_sql("NOT VALID SQL !!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_begin_commit() {
        let stmts = parse_sql("BEGIN").unwrap();
        assert!(matches!(stmts[0], Statement::StartTransaction { .. }));

        let stmts = parse_sql("COMMIT").unwrap();
        assert!(matches!(stmts[0], Statement::Commit { .. }));
    }
}
