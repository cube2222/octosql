// Copyright 2020 The OctoSQL Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::parser::{Expression, Identifier, Operator, Query, SelectExpression, Source, Trigger, Value, TableValuedFunctionArgument};

use super::sqlparser;
use super::sqlparser::ast;
use super::sqlparser::ast::{BinaryOperator, Expr, Function, FunctionArg, Ident, Select, SelectItem, SetExpr, Statement, TableFactor};
use super::sqlparser::dialect::GenericDialect;
use super::sqlparser::parser::Parser;
use crate::parser::sqlparser::ast::DateTimeField;
use crate::parser::sqlparser::dialect::keywords::Keyword::OPEN;

pub fn parse_sql(text: &str) -> Box<Query> {
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, text).unwrap();

    if let Statement::Query(q) = &ast[0] {
        let parsed = parse_query(q.as_ref());
        parsed
    } else {
        unimplemented!()
    }
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

    let filter_expression = select.selection.as_ref().map(parse_expr);

    let group_by_expression = select.group_by.iter()
        .map(parse_expr)
        .collect();

    let trigger = select.trigger.iter()
        .map(parse_trigger)
        .collect();

    Box::new(Query::Select {
        expressions,
        filter: filter_expression,
        from,
        order_by: vec![],
        group_by: group_by_expression,
        trigger
    })
}

pub fn parse_trigger(trigger: &sqlparser::ast::Trigger) -> Trigger {
    match trigger {
        sqlparser::ast::Trigger::Counting(n) => Trigger::Counting(n.clone()),
        sqlparser::ast::Trigger::Watermark => Trigger::Watermark,
        _ => unimplemented!(),
    }
}

pub fn parse_select_item(item: &SelectItem) -> SelectExpression {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            SelectExpression::Expression(parse_expr(expr), None)
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            SelectExpression::Expression(parse_expr(expr), Some(parse_ident(alias)))
        }
        SelectItem::Wildcard => {
            SelectExpression::Wildcard(None)
        }
        SelectItem::QualifiedWildcard(name) => {
            SelectExpression::Wildcard(Some(name.0[0].value.clone()))
        }
    }
}

pub fn parse_table(table: &TableFactor) -> Box<Source> {
    match table {
        TableFactor::Table { name, alias, args, with_hints: _ } => {
            if !args.is_empty() {
                return Box::new(Source::TableValuedFunction(
                    parse_compound_ident(&name.0),
                    args.iter().map(parse_table_valued_function_argument).collect(),
                ))
            } else {
                return Box::new(Source::Table(parse_compound_ident(&name.0), alias.clone().map(|alias| parse_ident(&alias.name))));
            }
        }
        TableFactor::Derived { lateral: _, subquery, alias } => {
            return Box::new(Source::Subquery(parse_query(subquery), alias.clone().map(|alias| parse_ident(&alias.name))));
        }
        _ => unimplemented!(),
    }
}

pub fn parse_table_valued_function_argument(arg: &FunctionArg) -> TableValuedFunctionArgument {
    match arg {
        FunctionArg::Named { name, arg } => TableValuedFunctionArgument::Named(parse_ident(name), parse_expr(arg)),
        FunctionArg::Unnamed(arg) => TableValuedFunctionArgument::Unnamed(parse_expr(arg)),
    }
}

pub fn parse_expr(expr: &Expr) -> Box<Expression> {
    match expr {
        Expr::Identifier(ident) => {
            Box::new(Expression::Variable(parse_ident(&ident)))
        }
        Expr::CompoundIdentifier(parts) => {
            Box::new(Expression::Variable(parse_compound_ident(parts)))
        }
        Expr::Value(value) => {
            Box::new(Expression::Constant(parse_value(value)))
        }
        Expr::BinaryOp { left, op, right } => {
            Box::new(Expression::Operator(
                parse_expr(left.as_ref()),
                parse_binary_operator(op),
                parse_expr(right.as_ref()),
            ))
        }
        Expr::Function(Function { name, args, over: _, distinct: _ }) => {
            Box::new(Expression::Function(parse_ident(&name.0[0]), args.iter().map(parse_function_arg).collect()))
        }
        Expr::Wildcard => {
            Box::new(Expression::Wildcard(None))
        }
        Expr::QualifiedWildcard(idents) => {
            Box::new(Expression::Wildcard(Some(idents[0].value.clone())))
        }
        Expr::Subquery(subquery) => {
            Box::new(Expression::Subquery(parse_query(subquery)))
        }
        _ => {
            dbg!(expr);
            unimplemented!()
        }
    }
}

pub fn parse_function_arg(arg: &FunctionArg) -> Box<Expression> {
    match arg {
        FunctionArg::Unnamed(expr) => parse_expr(expr),
        _ => unimplemented!(),
    }
}

pub fn parse_value(value: &ast::Value) -> Value {
    match value {
        ast::Value::Interval { value, leading_field, leading_precision, last_field, fractional_seconds_precision } => {
            let nanoseconds = value * match leading_field {
                None => {
                    dbg!(value);
                    unimplemented!()
                }
                Some(unit) => {
                    match unit {
                        DateTimeField::Day => 1000000000 * 60 * 60 * 24,
                        DateTimeField::Hour => 1000000000 * 60 * 60,
                        DateTimeField::Minute => 1000000000 * 60,
                        DateTimeField::Second => 1000000000,
                        _ => {
                            unimplemented!("{:?}", unit)
                        }
                    }
                }
            };
            Value::Duration(nanoseconds)
        },
        ast::Value::Number(val) => {
            let val_str: &str = val.as_str();
            Value::Integer(val_str.parse::<i64>().unwrap())
        },
        ast::Value::SingleQuotedString(val) => {
            Value::String(val.clone())
        },
        _ => {
            dbg!(value);
            unimplemented!()
        }
    }
}

pub fn parse_binary_operator(op: &BinaryOperator) -> Operator {
    match op {
        BinaryOperator::Lt => Operator::Lt,
        BinaryOperator::LtEq => Operator::LtEq,
        BinaryOperator::Eq => Operator::Eq,
        BinaryOperator::GtEq => Operator::GtEq,
        BinaryOperator::Gt => Operator::Gt,
        BinaryOperator::And => Operator::AND,
        _ => unimplemented!(),
    }
}

pub fn parse_compound_ident(parts: &Vec<Ident>) -> Identifier {
    if parts.len() == 1 {
        Identifier::SimpleIdentifier(parts[0].value.clone())
    } else if parts.len() == 2 {
        Identifier::NamespacedIdentifier(parts[0].value.clone(), parts[1].value.clone())
    } else {
        unimplemented!()
    }
}

pub fn parse_ident(ident: &Ident) -> Identifier {
    Identifier::SimpleIdentifier(ident.value.clone())
}

#[test]
fn test() {
    let sql = "SELECT c2.name as name, c2.livesleft, 3 as myconst \
    FROM (SELECT c.name, c.livesleft, c.age FROM cats c) as c2 \
    WHERE c2.age = c2.livesleft AND c2.age = c2.livesleft";

    parse_sql(sql);
}
