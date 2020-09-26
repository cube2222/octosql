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

use std::collections::BTreeMap;

use crate::logical::logical::{Aggregate, Expression, Node, Trigger, TableValuedFunction, TableValuedFunctionArgument};
use crate::parser;
use crate::parser::{Operator, SelectExpression, Value, Source};
use crate::physical::physical::{Identifier, ScalarValue};
use crate::logical::logical::TableValuedFunctionArgument::Table;

pub fn query_to_logical_plan(query: &parser::Query) -> Box<Node> {
    match query {
        parser::Query::Select { expressions, filter, from, order_by: _, group_by, trigger } => {
            if group_by.is_empty() {
                let mut plan = source_to_logical_plan(from.as_ref());

                let mut variables: BTreeMap<Identifier, Box<Expression>> = BTreeMap::new();

                let mut topmost_map_fields = Vec::with_capacity(expressions.len());
                let mut topmost_map_wildcards = vec![];

                for (i, select_expr) in expressions.iter().enumerate() {
                    match select_expr {
                        SelectExpression::Expression(expr, alias) => {
                            let name = alias.clone()
                                .unwrap_or_else(|| if let parser::Expression::Variable(ident) = expr.as_ref() {
                                    ident.clone()
                                } else {
                                    parser::Identifier::SimpleIdentifier(format!("column_{}", i))
                                });
                            let ident = identifier_to_logical_plan(&name);
                            variables.insert(ident.clone(), expression_to_logical_plan(expr.as_ref()));

                            topmost_map_fields.push(ident);
                        }
                        SelectExpression::Wildcard(qualifier) => {
                            topmost_map_wildcards.push(qualifier.clone());
                        }
                    }
                }

                let bottom_map_expressions = variables.into_iter()
                    .map(|(ident, expr)| {
                        (expr, ident)
                    }).collect();

                plan = Box::new(Node::Map {
                    source: plan,
                    expressions: bottom_map_expressions,
                    wildcards: vec![],
                    keep_source_fields: true,
                });

                if let Some(expr) = filter {
                    plan = Box::new(Node::Filter { source: plan, filter_expr: expression_to_logical_plan(expr.as_ref()) });
                }

                let topmost_map_expressions = topmost_map_fields.into_iter()
                    .map(|ident| (Box::new(Expression::Variable(ident.clone())), ident))
                    .collect();

                plan = Box::new(Node::Map {
                    source: plan,
                    expressions: topmost_map_expressions,
                    wildcards: topmost_map_wildcards,
                    keep_source_fields: false,
                });

                plan
            } else {
                let mut plan = source_to_logical_plan(from.as_ref());

                if let Some(expr) = filter {
                    plan = Box::new(Node::Filter { source: plan, filter_expr: expression_to_logical_plan(expr.as_ref()) });
                }

                let key_exprs = group_by.iter()
                    .map(Box::as_ref)
                    .map(expression_to_logical_plan)
                    .collect();

                let (aggregate_exprs, output_fields): (Vec<_>, Vec<_>) = expressions.iter()
                    .map(|select_expr| {
                        if let SelectExpression::Expression(expr, ident) = select_expr {
                            (expr, ident)
                        } else {
                            dbg!(select_expr);
                            unimplemented!()
                        }
                    })
                    .map(|(expr, ident)| (aggregate_expression_to_logical_plan(expr), ident.as_ref().map(identifier_to_logical_plan)))
                    .enumerate()
                    .map(|(i, (expr, ident))| (expr, ident.unwrap_or(Identifier::SimpleIdentifier(format!("column_{}", i)))))
                    .unzip();

                let (aggregates, exprs): (Vec<_>, Vec<_>) = aggregate_exprs.into_iter().unzip();

                let trigger_logical = trigger.iter()
                    .map(trigger_to_logical_plan)
                    .collect();

                plan = Box::new(Node::GroupBy {
                    source: plan,
                    key_exprs,
                    aggregates,
                    aggregated_exprs: exprs,
                    output_fields,
                    trigger: trigger_logical,
                });

                plan
            }
        }
    }
}

pub fn source_to_logical_plan(expr: &parser::Source) -> Box<Node> {
    match expr {
        parser::Source::Table(ident, alias) => {
            let mut plan = Box::new(Node::Source { name: identifier_to_logical_plan(&ident), alias: alias.clone().map(|ident| identifier_to_logical_plan(&ident)) });
            if let Some(parser::Identifier::SimpleIdentifier(ident)) = alias {
                plan = Box::new(Node::Requalifier { source: plan, qualifier: ident.clone() })
            }
            plan
        }
        parser::Source::Subquery(subquery, alias) => {
            let mut plan = query_to_logical_plan(&subquery);
            if let Some(parser::Identifier::SimpleIdentifier(ident)) = alias {
                plan = Box::new(Node::Requalifier { source: plan, qualifier: ident.clone() })
            }
            plan
        }
        Source::TableValuedFunction(ident, args) => match ident {
            parser::Identifier::SimpleIdentifier(name) => match name.as_str() {
                "range" => {
                    if args.len() != 2 {
                        unimplemented!()
                    }
                    Box::new(Node::Function(TableValuedFunction::Range(
                        table_valued_function_argument_to_logical_plan(&args[0]),
                        table_valued_function_argument_to_logical_plan(&args[1]),
                    )))
                }
                "max_diff_watermark" => {
                    if args.len() != 3 {
                        unimplemented!()
                    }
                    Box::new(Node::Function(TableValuedFunction::MaxDiffWatermarkGenerator(
                        table_valued_function_argument_to_logical_plan(&args[0]),
                        table_valued_function_argument_to_logical_plan(&args[1]),
                        table_valued_function_argument_to_logical_plan(&args[2]),
                    )))
                }
                _ => unimplemented!(),
            }
            _ => unimplemented!(),
        },
    }
}

pub fn table_valued_function_argument_to_logical_plan(arg: &parser::TableValuedFunctionArgument) -> TableValuedFunctionArgument {
    if let parser::TableValuedFunctionArgument::Unnamed(expr) = arg {
        match expr.as_ref() {
            parser::Expression::Function(name, args) => {
                if args.len() == 1 {
                    if name == &parser::Identifier::SimpleIdentifier("TABLE".to_string()) {
                        if let parser::Expression::Subquery(query) = args[0].as_ref() {
                            return TableValuedFunctionArgument::Table(query_to_logical_plan(query))
                        }
                        // TODO: When we have CTE's, also accept a variable here.
                    } else if name == &parser::Identifier::SimpleIdentifier("DESCRIPTOR".to_string()) {
                        if let parser::Expression::Variable(ident) = args[0].as_ref() {
                            return TableValuedFunctionArgument::Descriptior(identifier_to_logical_plan(ident))
                        }
                    }
                }
            }
            _ => {}
        };
        return TableValuedFunctionArgument::Expresion(expression_to_logical_plan(expr.as_ref()))
    } else {
        unimplemented!()
    }
}

pub fn expression_to_logical_plan(expr: &parser::Expression) -> Box<Expression> {
    match expr {
        parser::Expression::Variable(ident) => {
            Box::new(Expression::Variable(identifier_to_logical_plan(&ident)))
        }
        parser::Expression::Constant(value) => {
            Box::new(Expression::Constant(value_to_logical_plan(&value)))
        }
        parser::Expression::Function(name, args) => {
            Box::new(Expression::Function(identifier_to_logical_plan(name), args.iter().map(Box::as_ref).map(expression_to_logical_plan).collect()))
        }
        parser::Expression::Operator(left, op, right) => {
            Box::new(Expression::Function(operator_to_logical_plan(op), vec![expression_to_logical_plan(left.as_ref()), expression_to_logical_plan(right.as_ref())]))
        }
        parser::Expression::Wildcard(qualifier) => {
            Box::new(Expression::Wildcard(qualifier.clone()))
        }
        parser::Expression::Subquery(query) => {
            Box::new(Expression::Subquery(query_to_logical_plan(query.as_ref())))
        }
    }
}

// TODO: Maybe it should be Aggregate(Expr), this way the aggregate receives the record batch and calculates everything itself.
// Would be easier for stars and stuff I suppose.
// Think about it.
// The Cons is that each aggregate will have to define evaluating the underlying expression, which might be meh.
// Especially since star and star distinct can operate on some kind of tuple... maybe?
pub fn aggregate_expression_to_logical_plan(expr: &parser::Expression) -> (Aggregate, Box<Expression>) {
    match expr {
        parser::Expression::Variable(ident) => {
            (Aggregate::KeyPart, Box::new(Expression::Variable(identifier_to_logical_plan(&ident))))
        }
        parser::Expression::Function(name, args) => {
            match name {
                parser::Identifier::SimpleIdentifier(name) => {
                    match name.to_lowercase().as_str() {
                        "count" => {
                            (Aggregate::Count, expression_to_logical_plan(args[0].as_ref()))
                        }
                        "sum" => {
                            (Aggregate::Sum, expression_to_logical_plan(args[0].as_ref()))
                        }
                        _ => unimplemented!(),
                    }
                }
                _ => unimplemented!(),
            }
        }
        _ => {
            dbg!(expr);
            panic!("invalid aggregate expression")
        }
    }
}

pub fn trigger_to_logical_plan(trigger: &parser::Trigger) -> Trigger {
    match trigger {
        parser::Trigger::Counting(n) => Trigger::Counting(n.clone()),
    }
}

pub fn identifier_to_logical_plan(ident: &parser::Identifier) -> Identifier {
    match ident {
        parser::Identifier::SimpleIdentifier(id) => {
            Identifier::SimpleIdentifier(id.clone())
        }
        parser::Identifier::NamespacedIdentifier(namespace, id) => {
            Identifier::NamespacedIdentifier(namespace.clone(), id.clone())
        }
    }
}

pub fn value_to_logical_plan(val: &parser::Value) -> ScalarValue {
    match val {
        parser::Value::Integer(v) => {
            ScalarValue::Int64(v.clone())
        }
        Value::String(v) => { ScalarValue::Utf8(v.clone()) }
    }
}

pub fn operator_to_logical_plan(op: &parser::Operator) -> Identifier {
    Identifier::SimpleIdentifier(match op {
        Operator::Lt => "<".to_string(),
        Operator::LtEq => "<=".to_string(),
        Operator::Eq => "=".to_string(),
        Operator::GtEq => ">=".to_string(),
        Operator::Gt => ">".to_string(),
        Operator::Plus => "+".to_string(),
        Operator::Minus => "-".to_string(),
        Operator::AND => "AND".to_string(),
        Operator::OR => "OR".to_string(),
    })
}
