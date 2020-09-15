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
            Node::Source { name, alias } => Ok(Arc::new(CSVSource::new(name.to_string()))),
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
                Ok(Arc::new(map::Map::new(source.physical(mat_ctx)?, expr_vec, name_vec)))
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
                let aggregated_exprs_physical = aggregated_exprs
                    .into_iter()
                    .map(|expr| expr.physical(mat_ctx))
                    .collect::<Result<_, _>>()?;
                let aggregate_vec = aggregates
                    .iter()
                    .map(|expr| expr.physical(mat_ctx))
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(GroupBy::new(
                    key_exprs_physical,
                    aggregated_exprs_physical,
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
            Expression::Constant(_) => unimplemented!(),
            Expression::Function(_, _) => { unimplemented!() }
            Expression::Wildcard => { unimplemented!() }
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
