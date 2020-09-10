use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use crate::parser::Query;
use sqlparser::ast::{Statement, SetExpr, Select, TableFactor};

pub fn parse_sql(text: &str) -> Query {
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, text).unwrap();
    let query = ast[0].clone();

    if let Statement::Query(q) = query {
        dbg!(q);
        unimplemented!();
        translate_query(q);
    } else {
        unimplemented!()
    }
    unimplemented!()
}

pub fn translate_query(query: Box<sqlparser::ast::Query>) -> Query {
    if let SetExpr::Select(select) = query.body {
        translate_select(select);
    } else {
        unimplemented!()
    }
    unimplemented!()
}

pub fn translate_select(select: Box<Select>) -> Query {
    translate_table(Box::new(select.from[0].relation.clone()))
}

pub fn translate_table(table: Box<TableFactor>) -> Query {
    match &*table {
        TableFactor::Table { name, alias, args, with_hints } => {
            dbg!(*table);
        },
        TableFactor::Derived { lateral, subquery, alias } => {
            dbg!(*table);
        },
        _ => unimplemented!(),
    }
    unimplemented!()
}

#[test]
fn test() {
    let sql = "SELECT c2.name as name, c2.livesleft \
    FROM (SELECT * FROM cats c) as c2 \
    WHERE c2.age = c2.livesleft";

    parse_sql(sql);
}
