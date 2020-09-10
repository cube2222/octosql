use crate::physical::csv::CSVSource;
use crate::physical::physical;
use crate::physical::filter::Filter;
use crate::physical::group_by;
use crate::physical::group_by::GroupBy;
use crate::physical::map;
use crate::physical::stream_join::StreamJoin;
use std::sync::Arc;
use crate::physical::physical::Identifier;

#[derive(Debug)]
pub enum Error {
    Unexpected(String),
}

pub enum Node {
    Source {
        name: Identifier,
    },
    Filter {
        source: Box<Node>,
        filter_column: Identifier,
    },
    Map {
        source: Box<Node>,
        expressions: Vec<Expression>,
        keep_source_fields: bool,
    },
    GroupBy {
        source: Box<Node>,
        key_fields: Vec<Identifier>,
        aggregates: Vec<Aggregate>,
        aggregated_fields: Vec<Identifier>,
        output_fields: Vec<Identifier>,
    },
    Join {
        source: Box<Node>,
        source_key: Vec<Identifier>,
        joined: Box<Node>,
        joined_key: Vec<Identifier>,
    },
}

pub enum Expression {
    Variable(Identifier),
    Constant(physical::ScalarValue),
}

pub enum Aggregate {
    Count(),
    Sum(),
}

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
            Node::Source { name } => Ok(Arc::new(CSVSource::new(name.to_string()))),
            Node::Filter {
                source,
                filter_column,
            } => Ok(Arc::new(Filter::new(
                filter_column.clone(),
                source.physical(mat_ctx)?,
            ))),
            Node::Map {
                source,
                expressions,
                keep_source_fields,
            } => {
                let expr_vec_res = expressions
                    .iter()
                    .map(|expr| expr.physical(mat_ctx))
                    .collect::<Vec<_>>();
                let mut expr_vec = Vec::with_capacity(expr_vec_res.len());
                for expr_res in expr_vec_res {
                    expr_vec.push(expr_res?);
                }
                Ok(Arc::new(map::Map::new(source.physical(mat_ctx)?, expr_vec)))
            }
            Node::GroupBy {
                source,
                key_fields,
                aggregates,
                aggregated_fields,
                output_fields,
            } => {
                let aggregate_vec_res = aggregates
                    .iter()
                    .map(|expr| expr.physical(mat_ctx))
                    .collect::<Vec<_>>();
                let mut aggregate_vec = Vec::with_capacity(aggregate_vec_res.len());
                for expr_res in aggregate_vec_res {
                    aggregate_vec.push(expr_res?);
                }
                Ok(Arc::new(GroupBy::new(
                    key_fields.clone(),
                    aggregated_fields.clone(),
                    aggregate_vec,
                    output_fields.clone(),
                    source.physical(mat_ctx)?,
                )))
            }
            Node::Join {
                source,
                source_key,
                joined,
                joined_key,
            } => Ok(Arc::new(StreamJoin::new(
                source.physical(mat_ctx)?,
                source_key.clone(),
                joined.physical(mat_ctx)?,
                joined_key.clone(),
            ))),
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
            Expression::Constant(_) => unimplemented!(),
        }
    }
}

impl Aggregate {
    pub fn physical(
        &self,
        mat_ctx: &MaterializationContext,
    ) -> Result<Arc<dyn group_by::Aggregate>, Error> {
        match self {
            Aggregate::Count() => Ok(Arc::new(group_by::Count {})),
            Aggregate::Sum() => Ok(Arc::new(group_by::Sum {})),
        }
    }
}
