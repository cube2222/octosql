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
use std::future::Future;
use std::sync::Arc;

use arrow::array::{ArrayBuilder, ArrayRef, BooleanBuilder, Int64Array, Int64Builder, StringBuilder, BooleanArray, Float64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::physical::arrow::{create_key, GroupByScalar, get_scalar_value};
use crate::physical::map::Expression;
use crate::physical::physical::*;
use crate::physical::trigger::*;
use crate::physical::aggregate::{Aggregate, Accumulator};

pub struct GroupBy {
    key: Vec<Arc<dyn Expression>>,
    output_key_indices: Vec<usize>,
    aggregated_exprs: Vec<Arc<dyn Expression>>,
    aggregates: Vec<Arc<dyn Aggregate>>,
    output_names: Vec<Identifier>,
    source: Arc<dyn Node>,
}

impl GroupBy {
    pub fn new(
        key: Vec<Arc<dyn Expression>>,
        output_key_indices: Vec<usize>,
        aggregated_exprs: Vec<Arc<dyn Expression>>,
        aggregates: Vec<Arc<dyn Aggregate>>,
        output_names: Vec<Identifier>,
        source: Arc<dyn Node>,
    ) -> GroupBy {
        return GroupBy {
            key,
            output_key_indices,
            aggregated_exprs,
            aggregates,
            output_names,
            source,
        };
    }
}


// TODO: Try to rewrite as a generic function. Parameterized by the builder and its native type. Could be passed a closure to turn the GroupByScalar into the raw value.
macro_rules! push_retraction_keys {
    ($data_type:ident, $builder:ident, $key_columns:expr, $key_vec:expr, $last_triggered_values:expr, $key_index:expr, $retraction_key_columns:expr) => {
        {
            let mut array = $builder::new($key_columns[0].len());
            for row in 0..$key_columns[0].len() {
                create_key($key_columns.as_slice(), row, &mut $key_vec);

                if !$last_triggered_values.contains_key(&$key_vec) {
                    continue;
                }

                match $key_vec[$key_index] {
                    GroupByScalar::$data_type(v) => {
                        array.append_value(v).unwrap()
                    }
                    _ => panic!("bug: key doesn't match schema"),
                    // TODO: Maybe use as_any -> downcast?
                }
            }
            $retraction_key_columns.push(Arc::new(array.finish()) as ArrayRef);
        }
    }
}

macro_rules! push_retraction_keys_utf8 {
    ($data_type:ident, $builder:ident, $key_columns:expr, $key_vec:expr, $last_triggered_values:expr, $key_index:expr, $retraction_key_columns:expr) => {
        {
            let mut array = $builder::new($key_columns[0].len());
            for row in 0..$key_columns[0].len() {
                create_key($key_columns.as_slice(), row, &mut $key_vec);

                if !$last_triggered_values.contains_key(&$key_vec) {
                    continue;
                }

                match &$key_vec[$key_index] {
                    GroupByScalar::$data_type(v) => {
                        array.append_value(v.as_str()).unwrap()
                    }
                    _ => panic!("bug: key doesn't match schema"),
                    // TODO: Maybe use as_any -> downcast?
                }
            }
            $retraction_key_columns.push(Arc::new(array.finish()) as ArrayRef);
        }
    }
}

macro_rules! push_retraction_values {
    ($data_type:ident, $builder:ident, $retraction_key_columns:expr, $key_vec:expr, $last_triggered_values:expr, $aggregate_index:expr, $retraction_columns:expr) => {
        {
            let mut array = $builder::new($retraction_key_columns[0].len());
            for row in 0..$retraction_key_columns[0].len() {
                create_key($retraction_key_columns.as_slice(), row, &mut $key_vec);

                let last_triggered = $last_triggered_values.get(&$key_vec);
                let last_triggered_row = match last_triggered {
                    None => continue,
                    Some(v) => v,
                };

                match last_triggered_row[$aggregate_index] {
                    ScalarValue::$data_type(n) => array.append_value(n).unwrap(),
                    _ => panic!("bug: key doesn't match schema"),
                    // TODO: Maybe use as_any -> downcast?
                }
            }
            $retraction_columns.push(Arc::new(array.finish()) as ArrayRef);
        }
    }
}

macro_rules! push_values {
    ($data_type:ident, $builder:ident, $key_columns:expr, $key_vec:expr, $accumulators_map:expr, $last_triggered_values:expr, $aggregate_index:expr, $output_columns:expr) => {
        {
            let mut array = $builder::new($key_columns[0].len());

            for row in 0..$key_columns[0].len() {
                create_key($key_columns.as_slice(), row, &mut $key_vec);
                // TODO: this key may not exist because of retractions.
                let row_accumulators = $accumulators_map.get(&$key_vec).unwrap();

                match row_accumulators[$aggregate_index].trigger() {
                    ScalarValue::$data_type(n) => array.append_value(n).unwrap(),
                    _ => panic!("bug: key doesn't match schema"),
                    // TODO: Maybe use as_any -> downcast?
                }

                let mut last_values_vec = $last_triggered_values.entry($key_vec.clone()).or_default();
                last_values_vec.push(row_accumulators[$aggregate_index].trigger());
            }
            $output_columns.push(Arc::new(array.finish()) as ArrayRef);
        }
    }
}

macro_rules! combine_columns {
    ($builder:ident, $retraction_columns:expr, $output_columns:expr, $column_index:expr) => {
        {
            let mut array = $builder::new(
                $retraction_columns[0].len() + $output_columns[0].len(),
            );
            array.append_data(&[
                $retraction_columns[$column_index].data(),
                $output_columns[$column_index].data(),
            ]);
            $output_columns[$column_index] = Arc::new(array.finish()) as ArrayRef;
        }
    }
}

impl Node for GroupBy {
    fn schema(&self, schema_context: Arc<dyn SchemaContext>) -> Result<Arc<Schema>, Error> {
        let source_schema = self.source.schema(schema_context.clone())?;
        let mut key_fields: Vec<Field> = self
            .output_key_indices
            .iter()
            .map(|i| &self.key[*i])
            .map(|key_field| key_field.field_meta(schema_context.clone(), &source_schema))
            .collect::<Result<Vec<Field>, Error>>()?;

        let aggregated_field_types: Vec<DataType> = self
            .aggregated_exprs
            .iter()
            .enumerate()
            .map(|(i, column_expr)| {
                self.aggregates[i].output_type(column_expr.field_meta(schema_context.clone(), &source_schema)?.data_type())
            })
            .collect::<Result<Vec<DataType>, Error>>()?;

        let mut new_fields: Vec<Field> = aggregated_field_types
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, t)| Field::new(self.output_names[i].to_string().as_str(), t, false))
            .collect();

        key_fields.append(&mut new_fields);
        key_fields.push(Field::new(retractions_field, DataType::Boolean, false));
        Ok(Arc::new(Schema::new(key_fields)))
    }

    fn run(
        &self,
        exec_ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<(), Error> {
        let source_schema = self.source.schema(exec_ctx.variable_context.clone())?;

        let mut accumulators_map: BTreeMap<Vec<GroupByScalar>, Vec<Box<dyn Accumulator>>> =
            BTreeMap::new();
        let mut last_triggered_values: BTreeMap<Vec<GroupByScalar>, Vec<ScalarValue>> =
            BTreeMap::new();

        let key_types: Vec<DataType> = self
            .key
            .iter()
            .map(|key_expr| key_expr.field_meta(exec_ctx.variable_context.clone(), &source_schema).unwrap().data_type().clone())
            .collect();

        let mut trigger: Box<dyn Trigger> = Box::new(CountingTrigger::new(key_types, 1));

        self.source.run(
            exec_ctx,
            &mut |ctx, batch| {
                let key_columns: Vec<ArrayRef> = self.key
                    .iter()
                    .map(|expr| expr.evaluate(exec_ctx, &batch))
                    .collect::<Result<_, _>>()?;
                let aggregated_columns: Vec<ArrayRef> = self.aggregated_exprs
                    .iter()
                    .map(|expr| expr.evaluate(exec_ctx, &batch))
                    .collect::<Result<_, _>>()?;
                let source_retractions = batch.column(batch.num_columns() - 1)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();

                let mut key_vec: Vec<GroupByScalar> = Vec::with_capacity(key_columns.len());
                for i in 0..key_columns.len() {
                    key_vec.push(GroupByScalar::Int64(0))
                }

                for row in 0..aggregated_columns[0].len() {
                    create_key(key_columns.as_slice(), row, &mut key_vec);

                    let accumulators = accumulators_map.entry(key_vec.clone()).or_insert(
                        self.aggregates
                            .iter()
                            .enumerate()
                            .map(|(i, aggr)| aggr.create_accumulator(aggregated_columns[i].data_type()))
                            .collect(),
                    );
                    accumulators.iter_mut().enumerate().for_each(|(i, acc)| {
                        // TODO: remove if false
                        acc.add(
                            get_scalar_value(&aggregated_columns[i], row).unwrap(),
                            ScalarValue::Boolean(source_retractions.value(row)),
                        );
                    })
                }

                trigger.keys_received(key_columns);

                // Check if we can trigger something
                let mut key_columns = trigger.poll();
                if key_columns[0].len() == 0 {
                    return Ok(());
                }
                let mut output_columns = self.output_key_indices
                    .iter()
                    .map(|i| key_columns[*i].clone())
                    .collect::<Vec<_>>();
                let output_schema = self.schema(exec_ctx.variable_context.clone())?;

                let mut retraction_key_columns = Vec::with_capacity(self.output_names.len());

                // Push retraction keys
                for key_index in 0..self.key.len() {
                    match output_schema.fields()[key_index].data_type() {
                        DataType::Utf8 => push_retraction_keys_utf8!(Utf8, StringBuilder, key_columns, key_vec, last_triggered_values, key_index, retraction_key_columns),
                        DataType::Int64 => push_retraction_keys!(Int64, Int64Builder, key_columns, key_vec, last_triggered_values, key_index, retraction_key_columns),
                        _ => unimplemented!(),
                    }
                }

                let mut retraction_columns = self.output_key_indices
                    .iter()
                    .map(|i| retraction_key_columns[*i].clone())
                    .collect::<Vec<_>>();

                // Push retractions
                for aggregate_index in 0..self.aggregates.len() {
                    match output_schema.fields()[self.output_key_indices.len() + aggregate_index].data_type() {
                        DataType::Int64 => push_retraction_values!(Int64, Int64Builder, retraction_key_columns, key_vec, last_triggered_values, aggregate_index, retraction_columns),
                        DataType::Float64 => push_retraction_values!(Float64, Float64Builder, retraction_key_columns, key_vec, last_triggered_values, aggregate_index, retraction_columns),
                        _ => {
                            dbg!(output_schema.fields()[self.key.len() + aggregate_index].data_type());
                            unimplemented!()
                        }
                    }
                }
                // Remove those values
                for row in 0..retraction_key_columns[0].len() {
                    create_key(retraction_key_columns.as_slice(), row, &mut key_vec);
                    last_triggered_values.remove(&key_vec);
                }
                // Build retraction array
                // TODO: BooleanBuilder => PrimitiveBuilder<BooleanType>. Maybe this can be refactored into a function after all.
                let mut retraction_array_builder =
                    BooleanBuilder::new(retraction_key_columns[0].len() + key_columns[0].len());
                for i in 0..retraction_key_columns[0].len() {
                    retraction_array_builder.append_value(true);
                }
                for i in 0..key_columns[0].len() {
                    retraction_array_builder.append_value(false);
                }
                let retraction_array = Arc::new(retraction_array_builder.finish());

                // Push new values
                for aggregate_index in 0..self.aggregates.len() {
                    match output_schema.fields()[self.output_key_indices.len() + aggregate_index].data_type() {
                        DataType::Int64 => push_values!(Int64, Int64Builder, key_columns, key_vec, accumulators_map, last_triggered_values, aggregate_index, output_columns),
                        DataType::Float64 => push_values!(Float64, Float64Builder, key_columns, key_vec, accumulators_map, last_triggered_values, aggregate_index, output_columns),
                        _ => unimplemented!(),
                    }
                }

                // Combine retraction and non-retraction columns
                for column_index in 0..output_columns.len() {
                    match output_schema.fields()[column_index].data_type() {
                        DataType::Int64 => combine_columns!(Int64Builder, retraction_columns, output_columns, column_index),
                        DataType::Float64 => combine_columns!(Float64Builder, retraction_columns, output_columns, column_index),
                        DataType::Utf8 => combine_columns!(StringBuilder, retraction_columns, output_columns, column_index),
                        _ => unimplemented!(),
                    }
                }

                // Add retraction array
                output_columns.push(retraction_array as ArrayRef);

                let new_batch = RecordBatch::try_new(output_schema, output_columns).unwrap();

                produce(&ProduceContext {}, new_batch)?;

                Ok(())
            },
            &mut noop_meta_send,
        )?;

        Ok(())
    }
}
