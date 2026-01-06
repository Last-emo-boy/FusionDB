use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
    let dialect = GenericDialect {};

    let sql_index = "CREATE INDEX idx_content ON articles (content) USING FTS";
    let ast_index = Parser::parse_sql(&dialect, sql_index);
    println!("Index AST: {:?}", ast_index);

    let sql_match = "SELECT * FROM articles WHERE MATCH(content) AGAINST('rust database')";
    let ast_match = Parser::parse_sql(&dialect, sql_match);
    println!("Match AST: {:?}", ast_match);
}
