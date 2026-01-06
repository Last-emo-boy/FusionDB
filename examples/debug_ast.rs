use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
    let sql = "SELECT * FROM t ORDER BY a";
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!("{:#?}", ast);
}
