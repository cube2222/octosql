use std::sync::Arc;

use crate::physical::csv::CSVSource;
use crate::physical::filter::Filter;
use crate::physical::group_by;
use crate::physical::group_by::GroupBy;
use crate::physical::map;
use crate::physical::physical;
use crate::physical::physical::Identifier;
use crate::physical::stream_join::StreamJoin;
use crate::physical::functions::Equal;

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
        alias: Identifier,
    },
}

#[derive(Debug)]
pub enum Expression {
    Variable(Identifier),
    Constant(physical::ScalarValue),
    Function(Identifier, Vec<Box<Expression>>),
    Wildcard,
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
                path.push_str(".csv");
                Ok(Arc::new(CSVSource::new(path)))
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
                Ok(Arc::new(map::Map::new(source.physical(mat_ctx)?, expr_vec, name_vec, *keep_source_fields)))
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
            Node::Requalifier { source, alias } => unimplemented!(),
        }
    }
}

impl Expression {
    pub fn physical(
        &self,
        mat_ctx: &MaterializationContext,
    ) -> Result<Arc<dyn map::Expression>, Error> {
        match self {
            Expression::Variable(name) => Ok(Arc::new(map::FieldExpression::new(name.clone()))),
            Expression::Constant(value) => Ok(Arc::new(map::Constant::new(value.clone()))),
            Expression::Function(name, args) => {
                match name {
                    Identifier::SimpleIdentifier(ident) => {
                        match ident.as_str() {
                            "=" => Ok(Arc::new(Equal::new(args[0].physical(mat_ctx)?, args[1].physical(mat_ctx)?))),
                            _ => unimplemented!(),
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            Expression::Wildcard => { unimplemented!() }
            Expression::Subquery(query) => { Ok(Arc::new(map::Subquery::new(query.physical(mat_ctx)?))) }
        }
    }
}

impl Aggregate {
    pub fn physical(
        &self,
        mat_ctx: &MaterializationContext,
    ) -> Result<Arc<dyn group_by::Aggregate>, Error> {
        match self {
            Aggregate::Count => Ok(Arc::new(group_by::Count {})),
            Aggregate::Sum => Ok(Arc::new(group_by::Sum {})),
            _ => unimplemented!(),
        }
    }
}
