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

use std::sync::Arc;

use crate::physical::csv::CSVSource;
use crate::physical::filter::Filter;
use crate::physical::{group_by, aggregate};
use crate::physical::group_by::GroupBy;
use crate::physical::expression;
use crate::physical::map;
use crate::physical::physical;
use crate::physical::physical::Identifier;
use crate::physical::stream_join::StreamJoin;
// use crate::physical::functions::Equal;
use crate::physical::requalifier::Requalifier;
use crate::physical::json::JSONSource;
use crate::physical::functions;
use crate::physical::functions::FunctionExpression;
use crate::physical::functions::BUILTIN_FUNCTIONS;
use crate::physical::expression::WildcardExpression;

#[derive(Debug)]
pub enum Error {
    Unexpected(String),
}

#[derive(Debug)]
pub enum Node {
    Source {
        name: Identifier,
        alias: Option<Identifier>,
    },
    Filter {
        source: Box<Node>,
        filter_expr: Box<Expression>,
    },
    Map {
        source: Box<Node>,
        expressions: Vec<(Box<Expression>, Identifier)>,
        wildcards: Vec<Option<String>>,
        keep_source_fields: bool,
    },
    GroupBy {
        source: Box<Node>,
        key_exprs: Vec<Box<Expression>>,
        aggregates: Vec<Aggregate>,
        aggregated_exprs: Vec<Box<Expression>>,
        output_fields: Vec<Identifier>,
    },
    Join {
        source: Box<Node>,
        source_key: Vec<Box<Expression>>,
        joined: Box<Node>,
        joined_key: Vec<Box<Expression>>,
    },
    Requalifier {
        source: Box<Node>,
        alias: String,
    },
}

#[derive(Debug)]
pub enum Expression {
    Variable(Identifier),
    Constant(physical::ScalarValue),
    Function(Identifier, Vec<Box<Expression>>),
    Wildcard(Option<String>),
    Subquery(Box<Node>),
}

#[derive(Debug)]
pub enum Aggregate {
    KeyPart,
    Count,
    Sum,
}

#[derive(Debug)]
pub enum Trigger {
    Counting(u64),
}

pub struct MaterializationContext {}

impl Node {
    pub fn physical(
        &self,
        mat_ctx: &MaterializationContext,
    ) -> Result<Arc<dyn physical::Node>, Error> {
        match self {
            Node::Source { name, alias } => {
                let mut path = name.to_string();
                if path.contains(".json") {
                    Ok(Arc::new(JSONSource::new(path)))
                } else if path.contains(".csv") {
                    Ok(Arc::new(CSVSource::new(path)))
                } else {
                    unimplemented!()
                }
            },
            Node::Filter {
                source,
                filter_expr,
            } => {
                Ok(Arc::new(Filter::new(
                    filter_expr.physical(mat_ctx)?,
                    source.physical(mat_ctx)?,
                )))
            }
            Node::Map {
                source,
                expressions,
                wildcards,
                keep_source_fields,
            } => {
                let expr_vec_res = expressions
                    .iter()
                    .map(|(expr, ident)|
                        expr.physical(mat_ctx).map(|expr| (expr, ident.clone())))
                    .collect::<Vec<_>>();
                let mut expr_vec = Vec::with_capacity(expr_vec_res.len());
                let mut name_vec = Vec::with_capacity(expr_vec_res.len());
                for expr_res in expr_vec_res {
                    let (expr, name) = expr_res?;
                    expr_vec.push(expr);
                    name_vec.push(name);
                }
                Ok(Arc::new(map::Map::new(source.physical(mat_ctx)?, expr_vec, name_vec, wildcards.clone(), *keep_source_fields)))
            }
            Node::GroupBy {
                source,
                key_exprs,
                aggregates,
                aggregated_exprs,
                output_fields,
            } => {
                let key_exprs_physical = key_exprs
                    .into_iter()
                    .map(|expr| expr.physical(mat_ctx))
                    .collect::<Result<_, _>>()?;

                let (aggregates_no_key_part, aggregated_exprs_no_key_part): (Vec<_>, Vec<_>) = aggregates.iter()
                    .zip(aggregated_exprs.iter())
                    .filter(|(aggregate, aggregated_expr)| if let Aggregate::KeyPart = **aggregate {false} else {true})
                    .unzip();

                let aggregate_vec = aggregates_no_key_part
                    .iter()
                    .map(|expr| expr.physical(mat_ctx))
                    .collect::<Result<_, _>>()?;
                let aggregated_exprs_physical = aggregated_exprs_no_key_part
                    .into_iter()
                    .map(|expr| expr.physical(mat_ctx))
                    .collect::<Result<_, _>>()?;

                let aggregated_exprs_key_part = aggregates.iter()
                    .zip(aggregated_exprs.iter())
                    .filter(|(aggregate, aggregated_expr)| if let Aggregate::KeyPart = **aggregate {true} else {false})
                    .map(|(aggregate, aggregated_expr)| aggregated_expr)
                    .collect::<Vec<_>>();

                let aggregate_output_names = aggregates.iter()
                    .enumerate()
                    .filter(|(i, aggregate)| if let Aggregate::KeyPart = **aggregate {false} else {true})
                    .map(|(i, _)| output_fields[i].clone())
                    .collect();

                let mut output_key_indices = Vec::with_capacity(aggregated_exprs_key_part.len());

                for expr in aggregated_exprs_key_part {
                    if let Expression::Variable(var_name) = expr.as_ref() {
                        let mut found = false;
                        for i in 0..key_exprs.len() {
                            if let Expression::Variable(key_var_name) = key_exprs[i].as_ref() {
                                if var_name == key_var_name {
                                    output_key_indices.push(i);
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if !found {
                            return Err(Error::Unexpected(format!("key part variable {} not found in key", var_name.to_string())))
                        }
                    } else {
                        return Err(Error::Unexpected("key part can only contain variables".to_string()))
                    }
                }

                Ok(Arc::new(GroupBy::new(
                    key_exprs_physical,
                    output_key_indices,
                    aggregated_exprs_physical,
                    aggregate_vec,
                    aggregate_output_names,
                    source.physical(mat_ctx)?,
                )))
            }
            Node::Join {
                source,
                source_key,
                joined,
                joined_key,
            } => {
                let source_key_exprs = source_key
                    .into_iter()
                    .map(|expr| expr.physical(mat_ctx))
                    .collect::<Result<_, _>>()?;

                let joined_key_exprs = joined_key
                    .into_iter()
                    .map(|expr| expr.physical(mat_ctx))
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(StreamJoin::new(
                    source.physical(mat_ctx)?,
                    source_key_exprs,
                    joined.physical(mat_ctx)?,
                    joined_key_exprs,
                )))
            }
            Node::Requalifier { source, alias } => {
                Ok(Arc::new(Requalifier::new(alias.clone(), source.physical(mat_ctx)?)))
            },
        }
    }
}

impl Expression {
    pub fn physical(
        &self,
        mat_ctx: &MaterializationContext,
    ) -> Result<Arc<dyn expression::Expression>, Error> {
        match self {
            Expression::Variable(name) => Ok(Arc::new(expression::FieldExpression::new(name.clone()))),
            Expression::Constant(value) => Ok(Arc::new(expression::Constant::new(value.clone()))),
            Expression::Function(name, args) => {
                let args_physical = args
                    .into_iter()
                    .map(|expr| expr.physical(mat_ctx))
                    .collect::<Result<_, _>>()?;

                match name {
                    Identifier::SimpleIdentifier(ident) => {
                        match BUILTIN_FUNCTIONS.get(ident.to_lowercase().as_str()) {
                            None => {Err(Error::Unexpected(format!("unknown function: {}", ident.as_str())))},
                            Some(fn_constructor) => Ok(fn_constructor(args_physical)),
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            Expression::Wildcard(qualifier) => Ok(Arc::new(WildcardExpression::new(qualifier.as_ref().map(|s| s.as_str())))),
            Expression::Subquery(query) => Ok(Arc::new(expression::Subquery::new(query.physical(mat_ctx)?))),
        }
    }
}

impl Aggregate {
    pub fn physical(
        &self,
        mat_ctx: &MaterializationContext,
    ) -> Result<Arc<dyn aggregate::Aggregate>, Error> {
        match self {
            Aggregate::Count => Ok(Arc::new(aggregate::Count {})),
            Aggregate::Sum => Ok(Arc::new(aggregate::Sum {})),
            _ => unimplemented!(),
        }
    }
}
