use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
fn main() {
    let sql = "INSERT INTO t VALUES (1) RETURNING *";
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).unwrap();
    if let sqlparser::ast::Statement::Insert(ins) = &stmts[0] {
        println!("returning: {:?}", ins.returning);
    }
}
