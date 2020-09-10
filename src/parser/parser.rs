use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use crate::parser::{Query, Source, Identifier, Expression};
use sqlparser::ast::{Statement, SetExpr, Select, TableFactor, Ident, Expr, SelectItem};

pub fn parse_sql(text: &str) -> Query {
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, text).unwrap();

    if let Statement::Query(q) = &ast[0] {
        let parsed = parse_query(q.as_ref());
        dbg!(parsed);
    } else {
        unimplemented!()
    }
    unimplemented!()
}

pub fn parse_query(sql_query: &sqlparser::ast::Query) -> Box<Query> {
    if let SetExpr::Select(select) = &sql_query.body {
        let query = parse_select(select.as_ref());
        query
    } else {
        unimplemented!()
    }
}

pub fn parse_select(select: &Select) -> Box<Query> {
    let from = parse_table(&select.from[0].relation);

    let expressions = select.projection.iter()
        .map(parse_select_item)
        .collect();

    Box::new(Query::Select {
        expressions,
        filter: None,
        from,
        order_by: vec![]
    })
}

pub fn parse_select_item(item: &SelectItem) -> (Box<Expression>, Option<Identifier>) {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            (parse_expr(expr), None)
        },
        SelectItem::ExprWithAlias { expr, alias } => {
            (parse_expr(expr), Some(parse_ident(alias)))
        },
        _ => unimplemented!(),
    }
}

pub fn parse_table(table: &TableFactor) -> Box<Source> {
    match table {
        TableFactor::Table { name, alias, args, with_hints } => {
            return Box::new(Source::Table(parse_ident(&name.0[0]), alias.clone().map(|alias| parse_ident(&alias.name))))
        },
        TableFactor::Derived { lateral, subquery, alias } => {
            return Box::new(Source::Subquery(parse_query(subquery), alias.clone().map(|alias| parse_ident(&alias.name))))
        },
        _ => unimplemented!(),
    }
}

pub fn parse_expr(expr: &Expr) -> Box<Expression> {
    match expr {
        Expr::Identifier(ident) => {
            Box::new(Expression::Variable(parse_ident(&ident)))
        },
        Expr::CompoundIdentifier(parts) => {
            Box::new(Expression::Variable(parse_compound_ident(parts)))
        }
        _ => unimplemented!(),
    }
}

pub fn parse_compound_ident(parts: &Vec<Ident>) -> Identifier {
    if parts.len() != 2 {
        unimplemented!()
    }
    Identifier::NamespacedIdentifier(parts[0].value.clone(), parts[1].value.clone())
}

pub fn parse_ident(ident: &Ident) -> Identifier {
    Identifier::SimpleIdentifier(ident.value.clone())
}

#[test]
fn test() {
    let sql = "SELECT c2.name as name, c2.livesleft \
    FROM (SELECT c.name, c.livesleft, c.age FROM cats c) as c2 \
    WHERE c2.age = c2.livesleft";

    parse_sql(sql);
}
