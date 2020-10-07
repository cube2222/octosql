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

use std::error::Error;
use std::fs::File;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{csv, json};
use arrow::datatypes::{DataType, Field, Schema};
use itertools::Itertools;

use crate::physical::aggregate;
use crate::physical::csv::CSVSource;
use crate::physical::expression;
use crate::physical::expression::WildcardExpression;
use crate::physical::filter::Filter;
use crate::physical::functions::BUILTIN_FUNCTIONS;
use crate::physical::group_by::GroupBy;
use crate::physical::json::JSONSource;
use crate::physical::map;
use crate::physical::physical;
use crate::physical::physical::{BATCH_SIZE, Identifier, RETRACTIONS_FIELD, ScalarValue, SchemaContext, SchemaContextWithSchema};
use crate::physical::requalifier::Requalifier;
use crate::physical::stream_join::StreamJoin;
use crate::physical::tbv::max_diff_watermark_generator::MaxDiffWatermarkGenerator;
use crate::physical::tbv::range::Range;
use crate::physical::trigger;

pub struct TransformationContext {
    pub schema_context: Arc<dyn SchemaContext>,
}

type TransformNodeFn<T> = Box<dyn Fn(&TransformationContext, T, &Node) -> Result<(Box<Node>, T)>>;
type TransformExpressionFn<T> = Box<dyn Fn(&TransformationContext, &Arc<Schema>, T, &Expression) -> Result<(Box<Expression>, T)>>;

// TODO: Add schema_context and cur_record_schema for expression transformer.
pub struct Transformers<T: Clone> {
    pub node_fn: Option<TransformNodeFn<T>>,
    pub expr_fn: Option<TransformExpressionFn<T>>,
    pub base_state: T,
    pub state_reduce: Box<dyn Fn(T, T) -> T>,
}

#[derive(Clone, Debug)]
pub enum Node {
    Source {
        name: Identifier,
        alias: Option<Identifier>,
    },
    Filter {
        source: Box<Node>,
        filter_expr: Box<Expression>,
    },
    MapWithWildcards {
        source: Box<Node>,
        expressions: Vec<(Box<Expression>, Identifier)>,
        wildcards: Vec<Option<String>>,
        keep_source_fields: bool,
    },
    Map {
        source: Box<Node>,
        expressions: Vec<(Box<Expression>, Identifier)>,
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
        qualifier: String,
    },
    Function(TableValuedFunction),
}

#[derive(Clone, Debug)]
pub struct NodeMetadata {
    pub partition_count: usize,
    pub schema: Arc<Schema>,
    pub time_column: Option<usize>,
}

#[derive(Clone, Debug)]
pub enum TableValuedFunction {
    Range(TableValuedFunctionArgument, TableValuedFunctionArgument),
    MaxDiffWatermarkGenerator(TableValuedFunctionArgument, TableValuedFunctionArgument, TableValuedFunctionArgument),
}

#[derive(Clone, Debug)]
pub enum TableValuedFunctionArgument {
    Expresion(Box<Expression>),
    Table(Box<Node>),
    Descriptior(Identifier),
}

#[derive(Clone, Debug)]
pub enum Expression {
    Variable(Identifier),
    Constant(physical::ScalarValue),
    Function(Identifier, Vec<Box<Expression>>),
    Wildcard(Option<String>),
    Subquery(Box<Node>),
}

#[derive(Clone, Debug)]
pub enum Aggregate {
    KeyPart,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    First,
}

#[derive(Clone, Debug)]
pub enum Trigger {
    Counting(u64),
    Watermark,
}

pub struct MaterializationContext {
    pub schema_context: Arc<dyn SchemaContext>,
}

impl Node {
    pub fn metadata(&self, schema_context: Arc<dyn SchemaContext>) -> Result<NodeMetadata> {
        match self {
            Node::Source { name, alias } => {
                let path = name.to_string();
                if path.contains(".json") {
                    let file = File::open(path.as_str()).unwrap();
                    let r = json::ReaderBuilder::new()
                        .infer_schema(Some(10))
                        .with_batch_size(BATCH_SIZE)
                        .build(file)
                        .unwrap();
                    let mut fields = r.schema().fields().clone();
                    fields.push(Field::new(RETRACTIONS_FIELD, DataType::Boolean, false));

                    Ok(NodeMetadata { partition_count: 1, schema: Arc::new(Schema::new(fields)), time_column: None })
                } else if path.contains(".csv") {
                    let file = File::open(path.as_str()).unwrap();
                    let r = csv::ReaderBuilder::new()
                        .has_header(true)
                        .infer_schema(Some(10))
                        .with_batch_size(BATCH_SIZE)
                        .build(file)
                        .unwrap();
                    let mut fields = r.schema().fields().clone();
                    fields.push(Field::new(RETRACTIONS_FIELD, DataType::Boolean, false));

                    Ok(NodeMetadata { partition_count: 1, schema: Arc::new(Schema::new(fields)), time_column: None })
                } else {
                    dbg!(name);
                    unimplemented!()
                }
            }
            Node::Filter { source, filter_expr } => {
                source.metadata(schema_context.clone())
            }
            Node::Map { source, expressions: expressions_with_names } => {
                let source_metadata = source.metadata(schema_context.clone())?;
                let source_schema = &source_metadata.schema;

                let (expressions, names): (Vec<_>, Vec<_>) = expressions_with_names.iter()
                    // For some reason .cloned() doesn't work here.
                    .map(|(expr, ident)| (expr.clone(), ident.clone()))
                    .unzip();

                let mut new_schema_fields: Vec<Field> = expressions
                    .iter()
                    .map(|expr| {
                        expr.metadata(schema_context.clone(), &source_schema)
                            .unwrap_or_else(|err| {
                                dbg!(err);
                                unimplemented!()
                            })
                    })
                    .enumerate()
                    .map(|(i, field)| Field::new(names[i].to_string().as_str(), field.data_type().clone(), field.is_nullable()))
                    .collect();
                new_schema_fields.push(Field::new(RETRACTIONS_FIELD, DataType::Boolean, false));

                let mut new_time_column = None;
                let source_time_column = source_metadata.time_column;
                if let Some(source_time_column) = source_time_column {
                    let source_time_field = source_schema.field(source_time_column).name();
                    for i in 0..expressions.len() {
                        if let Expression::Variable(ident) = expressions[i].as_ref() {
                            if &ident.to_string() == source_time_field {
                                new_time_column = Some(i);
                                break;
                            }
                        }
                    }
                }

                Ok(NodeMetadata {
                    partition_count: source_metadata.partition_count,
                    schema: Arc::new(Schema::new(new_schema_fields)),
                    time_column: new_time_column,
                })
            }
            // TODO: Just don't allow to use retractions field as field name.
            Node::MapWithWildcards { source, expressions: expressions_with_names, wildcards, keep_source_fields } => {
                let source_metadata = source.metadata(schema_context.clone())?;
                let source_schema = &source_metadata.schema;

                let (expressions, names): (Vec<_>, Vec<_>) = expressions_with_names.iter()
                    // For some reason .cloned() doesn't work here.
                    .map(|(expr, ident)| (expr.clone(), ident.clone()))
                    .unzip();

                let mut new_schema_fields: Vec<Field> = wildcards.iter()
                    .flat_map(|qualifier| {
                        match qualifier {
                            Some(qualifier) => {
                                source_schema.fields().clone().into_iter()
                                    .filter(|f| {
                                        f.name().starts_with(qualifier)
                                    })
                                    .collect::<Vec<_>>()
                            }
                            None => {
                                source_schema.fields().clone().into_iter().collect::<Vec<_>>()
                            }
                        }
                    })
                    .filter(|f| f.name() != RETRACTIONS_FIELD)
                    .collect();

                new_schema_fields.extend(expressions
                    .iter()
                    .map(|expr| {
                        expr.metadata(schema_context.clone(), &source_schema)
                            .unwrap_or_else(|err| {
                                dbg!(err);
                                unimplemented!()
                            })
                    })
                    .enumerate()
                    .map(|(i, field)| Field::new(names[i].to_string().as_str(), field.data_type().clone(), field.is_nullable())));
                if keep_source_fields.clone() {
                    let mut to_append = new_schema_fields;
                    new_schema_fields = source_schema.fields().clone();
                    new_schema_fields.truncate(new_schema_fields.len() - 1); // Remove retraction field.
                    new_schema_fields.append(&mut to_append);
                }
                new_schema_fields.push(Field::new(RETRACTIONS_FIELD, DataType::Boolean, false));

                Ok(NodeMetadata {
                    partition_count: source_metadata.partition_count,
                    schema: Arc::new(Schema::new(new_schema_fields)),
                    time_column: if keep_source_fields.clone() { source_metadata.time_column.clone() } else { None },
                })
            }
            Node::GroupBy { source, key_exprs, aggregates, aggregated_exprs, output_fields, trigger } => {
                let source_metadata = source.metadata(schema_context.clone())?;
                let source_schema = &source_metadata.schema;
                let mut key_fields: Vec<Field> = aggregates
                    .iter()
                    .enumerate()
                    .filter(|(i, aggregate)| if let Aggregate::KeyPart = **aggregate { true } else { false })
                    .map(|(i, aggregate)| aggregated_exprs[i].metadata(schema_context.clone(), &source_schema))
                    .collect::<Result<Vec<Field>>>()?;

                let aggregated_field_types: Vec<DataType> = aggregated_exprs
                    .iter()
                    .enumerate()
                    .filter(|(i, column_expr)| if let Aggregate::KeyPart = &aggregates[i.clone()] { false } else { true })
                    .map(|(i, column_expr)| {
                        aggregates[i].metadata(column_expr.metadata(schema_context.clone(), source_schema)?.data_type())
                    })
                    .collect::<Result<Vec<DataType>>>()?;

                let aggregated_field_output_names: Vec<Identifier> = aggregated_exprs
                    .iter()
                    .enumerate()
                    .filter(|(i, column_expr)| if let Aggregate::KeyPart = &aggregates[i.clone()] { false } else { true })
                    .map(|(i, column_expr)| {
                        output_fields[i].clone()
                    })
                    .collect();

                let mut new_fields: Vec<Field> = aggregated_field_types
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(i, t)| Field::new(aggregated_field_output_names[i].to_string().as_str(), t, false))
                    .collect();

                key_fields.append(&mut new_fields);
                key_fields.push(Field::new(RETRACTIONS_FIELD, DataType::Boolean, false));
                Ok(NodeMetadata {
                    partition_count: 1,
                    schema: Arc::new(Schema::new(key_fields)),
                    time_column: None,
                })
            }
            Node::Join { source, source_key, joined, joined_key } => {
                let mut source_schema_fields = source.metadata(schema_context.clone())?.schema.fields().clone();
                source_schema_fields.truncate(source_schema_fields.len() - 1);
                let joined_schema_fields = joined.metadata(schema_context.clone())?.schema.fields().clone();
                let new_fields: Vec<Field> = source_schema_fields
                    .iter()
                    .map(|f| Field::new(f.name(), f.data_type().clone(), true))
                    .chain(
                        joined_schema_fields
                            .iter()
                            .map(|f| Field::new(f.name(), f.data_type().clone(), true)),
                    )
                    .collect();

                // TODO: Check if source and joined key types match.
                // TODO: set time_field

                Ok(NodeMetadata { partition_count: 1, schema: Arc::new(Schema::new(new_fields)), time_column: None })
            }
            Node::Requalifier { source, qualifier } => {
                let source_metadata = source.metadata(schema_context.clone())?;
                let source_schema = source_metadata.schema.as_ref();
                let source_fields = source_schema.fields();
                let mut new_fields: Vec<Field> = source_fields[0..source_fields.len()-1].iter()
                    .map(|f| Field::new(requalify(qualifier.as_str(), f.name()).as_str(), f.data_type().clone(), f.is_nullable()))
                    .collect();
                new_fields.push(source_fields[source_fields.len()-1].clone());

                Ok(NodeMetadata {
                    partition_count: source_metadata.partition_count,
                    schema: Arc::new(Schema::new(new_fields)),
                    time_column: source_metadata.time_column.clone(),
                })
            }
            Node::Function(tbv) => {
                match tbv {
                    TableValuedFunction::Range(_, _) => {
                        Ok(NodeMetadata {
                            partition_count: 1,
                            schema: Arc::new(Schema::new(vec![
                                Field::new("i", DataType::Int64, false),
                                Field::new(RETRACTIONS_FIELD, DataType::Boolean, false),
                            ])),
                            time_column: None,
                        })
                    }
                    TableValuedFunction::MaxDiffWatermarkGenerator(time_field_name, max_diff, source) => {
                        let time_field_name = if let TableValuedFunctionArgument::Descriptior(ident) = time_field_name {
                            ident
                        } else {
                            return Err(anyhow!("max diff watermark generator time_field_name must be identifier"));
                        };
                        let source_node = if let TableValuedFunctionArgument::Table(query) = source {
                            query
                        } else {
                            return Err(anyhow!("max diff watermark generator source must be query"));
                        };

                        let source_metadata = source_node.metadata(schema_context.clone())?;
                        let mut time_col = None;
                        for (i, field) in source_metadata.schema.fields().iter().enumerate() {
                            if field.name() == &time_field_name.to_string() {
                                time_col = Some(i);
                            }
                        }

                        Ok(NodeMetadata {
                            partition_count: source_metadata.partition_count,
                            schema: source_metadata.schema.clone(),
                            time_column: time_col,
                        })
                    }
                }
            }
        }
    }

    pub fn physical(
        &self,
        mat_ctx: &MaterializationContext,
    ) -> Result<Arc<dyn physical::Node>> {
        let logical_metadata = self.metadata(mat_ctx.schema_context.clone())?;

        match self {
            Node::Source { name, alias: _ } => {
                let path = name.to_string();
                if path.contains(".json") {
                    Ok(Arc::new(JSONSource::new(logical_metadata, path)))
                } else if path.contains(".csv") {
                    Ok(Arc::new(CSVSource::new(logical_metadata, path)))
                } else {
                    dbg!(name);
                    unimplemented!()
                }
            }
            Node::Filter {
                source,
                filter_expr,
            } => {
                let source_metadata = source.metadata(mat_ctx.schema_context.clone())?;

                Ok(Arc::new(Filter::new(
                    logical_metadata,
                    filter_expr.physical(mat_ctx, &source_metadata.schema)?,
                    source.physical(mat_ctx)?,
                )))
            }
            Node::Map {
                source, expressions
            } => {
                let source_metadata = source.metadata(mat_ctx.schema_context.clone())?;

                let expr_vec_res = expressions
                    .iter()
                    .map(|(expr, ident)|
                        expr.physical(mat_ctx, &source_metadata.schema).map(|expr| (expr, ident.clone())))
                    .collect::<Vec<_>>();
                let mut expr_vec = Vec::with_capacity(expr_vec_res.len());
                let mut name_vec = Vec::with_capacity(expr_vec_res.len());
                for expr_res in expr_vec_res {
                    let (expr, name) = expr_res?;
                    expr_vec.push(expr);
                    name_vec.push(name);
                }
                Ok(Arc::new(map::Map::new(logical_metadata, source.physical(mat_ctx)?, expr_vec, name_vec)))
            }
            Node::MapWithWildcards {
                source,
                expressions,
                wildcards,
                keep_source_fields,
            } => {
                unimplemented!()
            }
            Node::GroupBy {
                source,
                key_exprs,
                aggregates,
                aggregated_exprs,
                output_fields,
                trigger,
            } => {
                let source_metadata = source.metadata(mat_ctx.schema_context.clone())?;

                let key_types: Vec<DataType> = key_exprs
                    .iter()
                    .map(|key_expr| key_expr.metadata(mat_ctx.schema_context.clone(), &source_metadata.schema).unwrap().data_type().clone())
                    .collect();

                let mut key_time_part = None;
                if let Some(time_col) = source_metadata.time_column {
                    for i in 0..key_exprs.len() {
                        if let Expression::Variable(ident) = key_exprs[i].as_ref() {
                            if &ident.to_string() == source_metadata.schema.field(time_col).name() {
                                key_time_part = Some(i);
                                break;
                            }
                        }
                    }
                }

                let key_exprs_physical = key_exprs
                    .into_iter()
                    .map(|expr| expr.physical(mat_ctx, &source_metadata.schema))
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
                    .map(|expr| expr.physical(mat_ctx, &source_metadata.schema))
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
                    logical_metadata,
                    key_types,
                    key_exprs_physical,
                    key_time_part,
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
                let source_metadata = source.metadata(mat_ctx.schema_context.clone())?;

                let source_key_exprs = source_key
                    .into_iter()
                    .map(|expr| expr.physical(mat_ctx, &source_metadata.schema))
                    .collect::<Result<_, _>>()?;

                let joined_metadata = joined.metadata(mat_ctx.schema_context.clone())?;

                let joined_key_exprs = joined_key
                    .into_iter()
                    .map(|expr| expr.physical(mat_ctx, &joined_metadata.schema))
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(StreamJoin::new(
                    logical_metadata,
                    source.physical(mat_ctx)?,
                    source_key_exprs,
                    joined.physical(mat_ctx)?,
                    joined_key_exprs,
                )))
            }
            Node::Requalifier { source, qualifier: alias } => {
                Ok(Arc::new(Requalifier::new(logical_metadata, alias.clone(), source.physical(mat_ctx)?)))
            }
            Node::Function(function) => {
                match function {
                    TableValuedFunction::Range(start, end) => {
                        let start_expr = if let TableValuedFunctionArgument::Expresion(expr) = start {
                            expr.physical(mat_ctx, &Arc::new(Schema::new(vec![])))?
                        } else {
                            return Err(anyhow!("range start must be expression"));
                        };
                        let end_expr = if let TableValuedFunctionArgument::Expresion(expr) = end {
                            expr.physical(mat_ctx, &Arc::new(Schema::new(vec![])))?
                        } else {
                            return Err(anyhow!("range end must be expression"));
                        };
                        Ok(Arc::new(Range::new(logical_metadata, start_expr, end_expr)))
                    }
                    TableValuedFunction::MaxDiffWatermarkGenerator(time_field_name, max_diff, source) => {
                        let time_field_name = if let TableValuedFunctionArgument::Descriptior(ident) = time_field_name {
                            ident.clone()
                        } else {
                            return Err(anyhow!("max diff watermark generator time_field_name must be identifier"));
                        };
                        let source_node = if let TableValuedFunctionArgument::Table(query) = source {
                            query.physical(mat_ctx)?
                        } else {
                            return Err(anyhow!("max diff watermark generator source must be query"));
                        };
                        let source_schema = source_node.logical_metadata();
                        let max_diff_expr = if let TableValuedFunctionArgument::Expresion(expr) = max_diff {
                            expr.physical(mat_ctx, &source_schema.schema)?
                        } else {
                            return Err(anyhow!("max diff watermark generator max_diff must be expression"));
                        };
                        Ok(Arc::new(MaxDiffWatermarkGenerator::new(logical_metadata, time_field_name, max_diff_expr, source_node)))
                    }
                }
            }
        }
    }

    // TODO: Switch to Arc's, less expensive when cloning.
    pub fn transform<T: Clone>(&self, tf_ctx: &TransformationContext, transformers: &Transformers<T>) -> Result<(Box<Self>, T)> {
        let default_tf: TransformNodeFn<T> = Box::new(|tf_ctx: &TransformationContext, state: T, node: &Node| Ok((Box::new(node.clone()), state)));
        let tf = if let Some(tf) = &transformers.node_fn {
            tf
        } else {
            &default_tf
        };
        match self {
            Node::Source { name, alias } => tf(tf_ctx, transformers.base_state.clone(), self),
            Node::Filter { source, filter_expr } => {
                let source_schema = source.metadata(tf_ctx.schema_context.clone())?.schema;
                let (source_tf, source_state) = source.transform(tf_ctx, transformers)?;
                let (filter_expr_tf, filter_state) = filter_expr.transform(tf_ctx, &source_schema, transformers)?;
                tf(
                    tf_ctx,
                    (transformers.state_reduce)(source_state, filter_state),
                    &Node::Filter { source: source_tf, filter_expr: filter_expr_tf },
                )
            }
            Node::Map { source, expressions } => {
                let source_schema = source.metadata(tf_ctx.schema_context.clone())?.schema;
                let (source_tf, source_state) = source.transform(tf_ctx, transformers)?;
                let (expressions_tf, expressions_state): (_, Vec<T>) = expressions.iter()
                    .map(|(expr, ident)| {
                        let (expr_phys, state) = expr.transform(tf_ctx, &source_schema, transformers)?;
                        Ok(((expr_phys, ident.clone()), state))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .unzip();
                tf(
                    tf_ctx,
                    vec![source_state].into_iter().chain(expressions_state)
                        .fold1(&transformers.state_reduce).unwrap(),
                    &Node::Map {
                        source: source_tf,
                        expressions: expressions_tf,
                    },
                )
            }
            Node::MapWithWildcards { source, expressions, wildcards, keep_source_fields } => {
                let source_schema = source.metadata(tf_ctx.schema_context.clone())?.schema;
                let (source_tf, source_state) = source.transform(tf_ctx, transformers)?;
                let (expressions_tf, expressions_state): (_, Vec<T>) = expressions.iter()
                    .map(|(expr, ident)| {
                        let (expr_phys, state) = expr.transform(tf_ctx, &source_schema, transformers)?;
                        Ok(((expr_phys, ident.clone()), state))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .unzip();
                tf(
                    tf_ctx,
                    vec![source_state].into_iter().chain(expressions_state)
                        .fold1(&transformers.state_reduce).unwrap(),
                    &Node::MapWithWildcards {
                        source: source_tf,
                        expressions: expressions_tf,
                        wildcards: wildcards.clone(),
                        keep_source_fields: keep_source_fields.clone(),
                    },
                )
            }
            Node::GroupBy { source, key_exprs, aggregates, aggregated_exprs, output_fields, trigger } => {
                let source_schema = source.metadata(tf_ctx.schema_context.clone())?.schema;
                let (source_tf, source_state) = source.transform(tf_ctx, transformers)?;
                let (key_exprs_tf, key_exprs_state): (_, Vec<T>) = key_exprs.iter()
                    .map(|expr| expr.transform(tf_ctx, &source_schema, transformers))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .unzip();
                let (aggregated_exprs_tf, aggregated_exprs_state): (_, Vec<T>) = aggregated_exprs.iter()
                    .map(|expr| expr.transform(tf_ctx, &source_schema, transformers))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .unzip();
                tf(
                    tf_ctx,
                    vec![source_state].into_iter().chain(key_exprs_state).chain(aggregated_exprs_state)
                        .fold1(&transformers.state_reduce).unwrap(),
                    &Node::GroupBy {
                        source: source_tf,
                        key_exprs: key_exprs_tf,
                        aggregates: aggregates.clone(),
                        aggregated_exprs: aggregated_exprs_tf,
                        output_fields: output_fields.clone(),
                        trigger: trigger.clone(),
                    },
                )
            }
            Node::Join { source, source_key, joined, joined_key } => {
                let source_schema = source.metadata(tf_ctx.schema_context.clone())?.schema;
                let (source_tf, source_state) = source.transform(tf_ctx, transformers)?;
                let (source_key_tf, source_key_state): (_, Vec<T>) = source_key.iter()
                    .map(|expr| expr.transform(tf_ctx, &source_schema, transformers))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .unzip();

                let joined_schema = joined.metadata(tf_ctx.schema_context.clone())?.schema;
                let (joined_tf, joined_state) = joined.transform(tf_ctx, transformers)?;
                let (joined_key_tf, joined_key_state): (_, Vec<T>) = joined_key.iter()
                    .map(|expr| expr.transform(tf_ctx, &joined_schema, transformers))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .unzip();

                tf(
                    tf_ctx,
                    vec![source_state].into_iter().chain(source_key_state)
                        .chain(vec![joined_state]).chain(joined_key_state)
                        .fold1(&transformers.state_reduce).unwrap(),
                    &Node::Join {
                        source: source_tf,
                        source_key: source_key_tf,
                        joined: joined_tf,
                        joined_key: joined_key_tf,
                    },
                )
            }
            Node::Requalifier { source, qualifier } => {
                let (source_tf, source_state) = source.transform(tf_ctx, transformers)?;

                tf(
                    tf_ctx,
                    source_state,
                    &Node::Requalifier {
                        source: source_tf,
                        qualifier: qualifier.clone(),
                    },
                )
            }
            Node::Function(tbv) => {
                let (tbv_tf, states) = match tbv {
                    TableValuedFunction::Range(start, end) => {
                        let (start_expr_tf, start_expr_state) = if let TableValuedFunctionArgument::Expresion(expr) = start {
                            expr.transform(tf_ctx, &Arc::new(Schema::new(vec![])), transformers)?
                        } else {
                            return Err(anyhow!("range start must be expression"));
                        };
                        let (end_expr_tf, end_expr_state) = if let TableValuedFunctionArgument::Expresion(expr) = end {
                            expr.transform(tf_ctx, &Arc::new(Schema::new(vec![])), transformers)?
                        } else {
                            return Err(anyhow!("range end must be expression"));
                        };
                        (TableValuedFunction::Range(
                            TableValuedFunctionArgument::Expresion(start_expr_tf),
                            TableValuedFunctionArgument::Expresion(end_expr_tf),
                        ), vec![start_expr_state, end_expr_state])
                    }
                    TableValuedFunction::MaxDiffWatermarkGenerator(time_field_name, max_diff, source) => {
                        let (record_schema, (query_tf, query_state)) = if let TableValuedFunctionArgument::Table(query) = source {
                            (
                                query.metadata(tf_ctx.schema_context.clone())?.schema,
                                query.transform(tf_ctx, transformers)?,
                            )
                        } else {
                            return Err(anyhow!("max diff watermark generator source must be query"));
                        };
                        let time_field_name = if let TableValuedFunctionArgument::Descriptior(ident) = time_field_name {
                            ident.clone()
                        } else {
                            return Err(anyhow!("max diff watermark generator time_field_name must be identifier"));
                        };
                        let (max_diff_tf, max_diff_state) = if let TableValuedFunctionArgument::Expresion(expr) = max_diff {
                            expr.transform(tf_ctx, &record_schema, transformers)?
                        } else {
                            return Err(anyhow!("max diff watermark generator max_diff must be expression"));
                        };
                        (TableValuedFunction::MaxDiffWatermarkGenerator(
                            TableValuedFunctionArgument::Descriptior(time_field_name),
                            TableValuedFunctionArgument::Expresion(max_diff_tf),
                            TableValuedFunctionArgument::Table(query_tf),
                        ), vec![query_state, max_diff_state])
                    }
                };
                tf(
                    tf_ctx,
                    states.into_iter().fold1(&transformers.state_reduce).unwrap(),
                    &Node::Function(tbv_tf),
                )
            }
        }
    }
}

fn requalify(qualifier: &str, str: &String) -> String {
    if let Some(i) = str.find(".") {
        format!("{}.{}", qualifier, &str[i + 1..]).to_string()
    } else {
        format!("{}.{}", qualifier, str.as_str()).to_string()
    }
}

impl Expression {
    pub fn metadata(
        &self,
        schema_context: Arc<dyn SchemaContext>,
        record_schema: &Arc<Schema>,
    ) -> Result<Field> {
        match self {
            Expression::Variable(field) => {
                let field_name_string = field.to_string();
                let field_name = field_name_string.as_str();
                match record_schema.field_with_name(field_name) {
                    Ok(field) => Ok(field.clone()),
                    Err(arrow_err) => {
                        match schema_context.field_with_name(field_name).map(|field| field.clone()) {
                            Ok(field) => Ok(field),
                            Err(err) => Err(arrow_err).context(err),
                        }
                    }
                }
            }
            Expression::Constant(value) => {
                Ok(Field::new("", value.data_type(), value == &ScalarValue::Null))
            }
            Expression::Function(name, args) => {
                let meta_fn = match name {
                    Identifier::SimpleIdentifier(ident) => {
                        match BUILTIN_FUNCTIONS.get(ident.to_lowercase().as_str()) {
                            None => Err(anyhow!("unknown function: {}", ident.as_str()))?,
                            Some((meta_fn, _)) => meta_fn.clone(),
                        }
                    }
                    _ => unimplemented!(),
                };

                meta_fn(&schema_context, record_schema)
            }
            Expression::Wildcard(qualifier_opt) => {
                let fields = record_schema.fields().iter()
                    .enumerate()
                    .map(|(i, f)| Field::new(format!("{}", i).as_str(), f.data_type().clone(), f.is_nullable()))
                    .filter(|f| {
                        if let Some(qualifier) = qualifier_opt {
                            f.name().starts_with(qualifier)
                        } else {
                            true
                        }
                    })
                    .collect();
                Ok(Field::new("", DataType::Struct(fields), false))
            }
            Expression::Subquery(query) => {
                let source_schema = query.metadata(
                    Arc::new(SchemaContextWithSchema {
                        previous: schema_context.clone(),
                        schema: record_schema.clone(),
                    }),
                )?.schema;
                // TODO: Implement for tuples.
                let field_base = source_schema.field(0);
                Ok(Field::new(field_base.name().as_str(), field_base.data_type().clone(), true))
            }
        }
    }

    pub fn physical(
        &self,
        mat_ctx: &MaterializationContext,
        record_schema: &Arc<Schema>,
    ) -> Result<Arc<dyn expression::Expression>> {
        match self {
            Expression::Variable(name) => Ok(Arc::new(expression::FieldExpression::new(name.clone()))),
            Expression::Constant(value) => Ok(Arc::new(expression::Constant::new(value.clone()))),
            Expression::Function(name, args) => {
                let args_physical = args
                    .into_iter()
                    .map(|expr| expr.physical(mat_ctx, record_schema))
                    .collect::<Result<_, _>>()?;

                match name {
                    Identifier::SimpleIdentifier(ident) => {
                        match BUILTIN_FUNCTIONS.get(ident.to_lowercase().as_str()) {
                            None => Err(anyhow!("unknown function: {}", ident.as_str())),
                            Some((_, fn_constructor)) => Ok(fn_constructor(args_physical)),
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            Expression::Wildcard(qualifier) => Ok(Arc::new(WildcardExpression::new(qualifier.as_ref().map(|s| s.as_str())))),
            Expression::Subquery(query) => {
                Ok(Arc::new(expression::Subquery::new(query.physical(&MaterializationContext {
                    schema_context: Arc::new(SchemaContextWithSchema {
                        previous: mat_ctx.schema_context.clone(),
                        schema: record_schema.clone(),
                    })
                })?)))
            }
        }
    }

    pub fn transform<T: Clone>(&self, tf_ctx: &TransformationContext, record_schema: &Arc<Schema>, transformers: &Transformers<T>) -> Result<(Box<Self>, T)> {
        let default_tf: TransformExpressionFn<T> = Box::new(|tf_ctx: &TransformationContext, record_schema: &Arc<Schema>, state: T, expr: &Expression| Ok((Box::new(expr.clone()), state)));
        let tf = if let Some(tf) = &transformers.expr_fn {
            tf
        } else {
            &default_tf
        };
        match self {
            Expression::Variable(ident) => tf(tf_ctx, record_schema, transformers.base_state.clone(), &Expression::Variable(ident.clone())),
            Expression::Constant(value) => tf(tf_ctx, record_schema, transformers.base_state.clone(), &Expression::Constant(value.clone())),
            Expression::Function(name, args) => {
                let (args_tf, args_state): (_, Vec<T>) = args.iter()
                    .map(|expr| expr.transform(tf_ctx, record_schema, transformers))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .unzip();
                tf(
                    tf_ctx,
                    record_schema,
                    args_state.into_iter().fold1(&transformers.state_reduce).unwrap(),
                    &Expression::Function(
                        name.clone(),
                        args_tf,
                    ),
                )
            }
            Expression::Wildcard(qualifier) => tf(tf_ctx, record_schema, transformers.base_state.clone(), &Expression::Wildcard(qualifier.clone())),
            Expression::Subquery(node) => {
                let (node_tf, node_state) = node.transform(
                    &TransformationContext {
                        schema_context: Arc::new(SchemaContextWithSchema {
                            previous: tf_ctx.schema_context.clone(),
                            schema: record_schema.clone(),
                        })
                    },
                    transformers,
                )?;
                tf(
                    tf_ctx,
                    record_schema,
                    node_state,
                    &Expression::Subquery(
                        node_tf
                    ),
                )
            }
        }
    }
}

impl Aggregate {
    pub fn metadata(&self, input_type: &DataType) -> Result<DataType> {
        match self {
            Aggregate::KeyPart => Ok(input_type.clone()),
            Aggregate::Count => Ok(DataType::Int64),
            Aggregate::Avg => Ok(DataType::Float64),
            Aggregate::Sum => {
                match input_type {
                    DataType::Int8 => Ok(DataType::Int8),
                    DataType::Int16 => Ok(DataType::Int16),
                    DataType::Int32 => Ok(DataType::Int32),
                    DataType::Int64 => Ok(DataType::Int64),
                    DataType::UInt8 => Ok(DataType::UInt8),
                    DataType::UInt16 => Ok(DataType::UInt16),
                    DataType::UInt32 => Ok(DataType::UInt32),
                    DataType::UInt64 => Ok(DataType::UInt64),
                    DataType::Float32 => Ok(DataType::Float32),
                    DataType::Float64 => Ok(DataType::Float64),
                    _ => {
                        dbg!(input_type);
                        unimplemented!()
                    }
                }
            }
            Aggregate::Min | Aggregate::Max => {
                match input_type {
                    DataType::Int8 => Ok(DataType::Int8),
                    DataType::Int16 => Ok(DataType::Int16),
                    DataType::Int32 => Ok(DataType::Int32),
                    DataType::Int64 => Ok(DataType::Int64),
                    DataType::UInt8 => Ok(DataType::UInt8),
                    DataType::UInt16 => Ok(DataType::UInt16),
                    DataType::UInt32 => Ok(DataType::UInt32),
                    DataType::UInt64 => Ok(DataType::UInt64),
                    _ => { // TODO: implement for Floats (they cannot be used as key type in a map)
                        dbg!(input_type);
                        unimplemented!()
                    }
                }
            }
            Aggregate::First => {
                match input_type {
                    DataType::Int8 => Ok(DataType::Int8),
                    DataType::Int16 => Ok(DataType::Int16),
                    DataType::Int32 => Ok(DataType::Int32),
                    DataType::Int64 => Ok(DataType::Int64),
                    DataType::UInt8 => Ok(DataType::UInt8),
                    DataType::UInt16 => Ok(DataType::UInt16),
                    DataType::UInt32 => Ok(DataType::UInt32),
                    DataType::UInt64 => Ok(DataType::UInt64),
                    DataType::Utf8 => Ok(DataType::Utf8),
                    //DataType::Timestamp(Nanosecond, _) => Ok(DataType::Timestamp(Nanosecond, _)), // TODO - sth not working
                    DataType::Boolean => Ok(DataType::Boolean),
                    DataType::Null => Ok(DataType::Null),
                    _ => { // TODO: implement for Floats AND Structs (they cannot be used as key type in a map)
                        dbg!(input_type);
                        unimplemented!()
                    }
                }
            }
        }
    }

    pub fn physical(
        &self,
        _mat_ctx: &MaterializationContext,
    ) -> Result<Arc<dyn aggregate::Aggregate>> {
        match self {
            Aggregate::Count => Ok(Arc::new(aggregate::Count {})),
            Aggregate::Sum => Ok(Arc::new(aggregate::Sum {})),
            Aggregate::Avg => Ok(Arc::new(aggregate::Avg {})),
            Aggregate::Min => Ok(Arc::new(aggregate::Min {})),
            Aggregate::Max => Ok(Arc::new(aggregate::Max {})),
            Aggregate::First => Ok(Arc::new(aggregate::First {})),
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
            Trigger::Watermark => Ok(Arc::new(trigger::WatermarkTriggerPrototype::new())),
        }
    }
}
