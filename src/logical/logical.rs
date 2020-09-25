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

use anyhow::Result;

use crate::physical::aggregate;
use crate::physical::trigger;
use crate::physical::csv::CSVSource;
use crate::physical::expression;
use crate::physical::expression::WildcardExpression;
use crate::physical::filter::Filter;
use crate::physical::functions::BUILTIN_FUNCTIONS;
use crate::physical::group_by::GroupBy;
use crate::physical::json::JSONSource;
use crate::physical::map;
use crate::physical::physical;
use crate::physical::physical::Identifier;
use crate::physical::requalifier::Requalifier;
use crate::physical::stream_join::StreamJoin;
use crate::physical::tbv::range::Range;
use crate::physical::tbv::max_diff_watermark_generator::MaxDiffWatermarkGenerator;

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
        trigger: Vec<Trigger>,
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
    Function(TableValuedFunction)
}

#[derive(Debug)]
pub enum TableValuedFunction {
    Range(TableValuedFunctionArgument, TableValuedFunctionArgument),
    MaxDiffWatermarkGenerator(TableValuedFunctionArgument, TableValuedFunctionArgument, TableValuedFunctionArgument),
}

#[derive(Debug)]
pub enum TableValuedFunctionArgument {
    Expresion(Box<Expression>),
    Table(Box<Node>),
    Descriptior(Identifier),
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
    ) -> Result<Arc<dyn physical::Node>> {
        match self {
            Node::Source { name, alias: _ } => {
                let path = name.to_string();
                if path.contains(".json") {
                    Ok(Arc::new(JSONSource::new(path)))
                } else if path.contains(".csv") {
                    Ok(Arc::new(CSVSource::new(path)))
                } else {
                    dbg!(name);
                    unimplemented!()
                }
            }
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
                trigger,
            } => {
                let key_exprs_physical = key_exprs
                    .into_iter()
                    .map(|expr| expr.physical(mat_ctx))
                    .collect::<Result<_, _>>()?;

                let (aggregates_no_key_part, aggregated_exprs_no_key_part): (Vec<_>, Vec<_>) = aggregates.iter()
                    .zip(aggregated_exprs.iter())
                    .filter(|(aggregate, _aggregated_expr)| if let Aggregate::KeyPart = **aggregate { false } else { true })
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
                    .filter(|(aggregate, _aggregated_expr)| if let Aggregate::KeyPart = **aggregate { true } else { false })
                    .map(|(_aggregate, aggregated_expr)| aggregated_expr)
                    .collect::<Vec<_>>();

                let aggregate_output_names = aggregates.iter()
                    .enumerate()
                    .filter(|(_i, aggregate)| if let Aggregate::KeyPart = **aggregate { false } else { true })
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
                            return Err(anyhow!("key part variable {} not found in key", var_name.to_string()));
                        }
                    } else {
                        return Err(anyhow!("key part can only contain variables"));
                    }
                }

                let trigger_prototypes = trigger.iter()
                    .map(|t| t.physical(mat_ctx))
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(GroupBy::new(
                    key_exprs_physical,
                    output_key_indices,
                    aggregated_exprs_physical,
                    aggregate_vec,
                    aggregate_output_names,
                    trigger_prototypes,
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
            }
            Node::Function(function) => {
                match function {
                    TableValuedFunction::Range(start, end) => {
                        let start_expr = if let TableValuedFunctionArgument::Expresion(expr) = start {
                            expr.physical(mat_ctx)?
                        } else {
                            return Err(anyhow!("range start must be expression"))
                        };
                        let end_expr = if let TableValuedFunctionArgument::Expresion(expr) = end {
                            expr.physical(mat_ctx)?
                        } else {
                            return Err(anyhow!("range end must be expression"))
                        };
                        Ok(Arc::new(Range::new(start_expr, end_expr)))
                    },
                    TableValuedFunction::MaxDiffWatermarkGenerator(time_field_name, max_diff, source) => {
                        let time_field_name = if let TableValuedFunctionArgument::Descriptior(ident) = time_field_name {
                            ident.clone()
                        } else {
                            return Err(anyhow!("max diff watermark generator time_field_name must be identifier"))
                        };
                        let max_diff_expr = if let TableValuedFunctionArgument::Expresion(expr) = max_diff {
                            expr.physical(mat_ctx)?
                        } else {
                            return Err(anyhow!("max diff watermark generator max_diff must be expression"))
                        };
                        let source_node = if let TableValuedFunctionArgument::Table(query) = source {
                            query.physical(mat_ctx)?
                        } else {
                            return Err(anyhow!("max diff watermark generator source must be query"))
                        };
                        Ok(Arc::new(MaxDiffWatermarkGenerator::new(time_field_name, max_diff_expr, source_node)))
                    },
                }
            }
        }
    }
}

impl Expression {
    pub fn physical(
        &self,
        mat_ctx: &MaterializationContext,
    ) -> Result<Arc<dyn expression::Expression>> {
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
                            None => Err(anyhow!("unknown function: {}", ident.as_str())),
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
        _mat_ctx: &MaterializationContext,
    ) -> Result<Arc<dyn aggregate::Aggregate>> {
        match self {
            Aggregate::Count => Ok(Arc::new(aggregate::Count {})),
            Aggregate::Sum => Ok(Arc::new(aggregate::Sum {})),
            _ => unimplemented!(),
        }
    }
}

impl Trigger {
    pub fn physical(
        &self,
        _mat_ctx: &MaterializationContext,
    ) -> Result<Arc<dyn trigger::TriggerPrototype>> {
        match self {
            Trigger::Counting(n) => Ok(Arc::new(trigger::CountingTriggerPrototype::new(n.clone()))),
        }
    }
}