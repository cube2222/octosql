use crate::execution::csv::CSVSource;
use crate::execution::execution;
use crate::execution::filter::Filter;
use crate::execution::map;
use crate::execution::group_by;
use std::sync::Arc;
use crate::execution::group_by::GroupBy;
use crate::execution::stream_join::StreamJoin;

enum Error {
    Unexpected(String),
}

enum Node {
    Source {
        name: String
    },
    Filter {
        source: Box<Node>,
        filter_column: String,
    },
    Map {
        source: Box<Node>,
        expressions: Vec<Expression>,
    },
    GroupBy {
        source: Box<Node>,
        key_fields: Vec<String>,
        aggregates: Vec<Aggregate>,
        aggregated_fields: Vec<String>,
        output_fields: Vec<String>,
    },
    Join {
        source: Box<Node>,
        source_key: Vec<String>,
        joined: Box<Node>,
        joined_key: Vec<String>,
    },
}

enum Expression {
    Variable(String),
    Constant(execution::ScalarValue),
}

enum Aggregate {
    Count(),
    Sum(),
}

enum Trigger {
    Counting(u64),
}

struct MaterializationContext {}

impl Node {
    pub fn physical(&self, mat_ctx: &MaterializationContext) -> Result<Arc<dyn execution::Node>, Error> {
        Ok(Arc::new(match self {
            Node::Source { name } => {
                CSVSource::new(
                    name.clone(),
                )
            }
            Node::Filter { source, filter_column } => {
                Filter::new(
                    filter_column.clone(),
                    source.physical(mat_ctx)?,
                )
            }
            Node::Map { source, expressions } => {
                let expr_vec = expressions
                    .iter()
                    .map(Expression::physical)
                    .fold_results(vec![], |mut exprs, cur| {
                        exprs.push(cur);
                        exprs
                    })?;
                map::Map::new(
                    source.physical(mat_ctx)?,
                    expr_vec,
                )
            }
            Node::GroupBy { source, key_fields, aggregates, aggregated_fields, output_fields } => {
                let aggregate_vec = aggregates
                    .iter()
                    .map(Aggregate::physical)
                    .fold_results(vec![], |mut aggrs, cur| {
                        aggrs.push(cur);
                        aggrs
                    })?;
                GroupBy::new(
                    key_fields.clone(),
                    aggregated_fields.clone(),
                    aggregate_vec,
                    output_fields.clone(),
                    source.physical(mat_ctx)?,
                )
            }
            Node::Join { source, source_key, joined, joined_key } => {
                StreamJoin::new(
                    source.physical(mat_ctx)?,
                    source_key.clone(),
                    joined.physical(mat_ctx)?,
                    joined_key.clone(),
                )
            }
        }))
    }
}

impl Expression {
    pub fn physical(&self, mat_ctx: &MaterializationContext) -> Result<Arc<dyn map::Expression>, Error> {
        Ok(Arc::new(match self {
            Expression::Variable(name) => {
                map::FieldExpression::new(name.clone())
            }
            Expression::Constant(_) => { unimplemented!() }
        }))
    }
}

impl Aggregate {
    pub fn physical(&self, mat_ctx: &MaterializationContext) -> Result<Arc<dyn group_by::Aggregate>, Error> {
        Ok(Arc::new(match self {
            Aggregate::Count() => { group_by::Count {} }
            Aggregate::Sum() => { group_by::Sum {} }
        }))
    }
}
