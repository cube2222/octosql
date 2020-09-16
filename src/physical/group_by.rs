use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Int64Array, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::physical::datafusion::{create_key, create_row, GroupByScalar};
use crate::physical::map::Expression;
use crate::physical::physical::*;
use crate::physical::trigger::*;

pub trait Aggregate: Send + Sync {
    fn output_type(&self, input_schema: &DataType) -> Result<DataType, Error>;
    fn create_accumulator(&self) -> Box<dyn Accumulator>;
}

pub trait Accumulator: std::fmt::Debug {
    fn add(&mut self, value: ScalarValue, retract: ScalarValue) -> bool;
    fn trigger(&self) -> ScalarValue;
}

pub struct Sum {}

impl Aggregate for Sum {
    fn output_type(&self, input_type: &DataType) -> Result<DataType, Error> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumAccumulator { sum: 0, count: 0 })
    }
}

#[derive(Debug)]
struct SumAccumulator {
    sum: i64,
    count: i64,
}

impl Accumulator for SumAccumulator {
    fn add(&mut self, value: ScalarValue, retract: ScalarValue) -> bool {
        let is_retraction = match retract {
            ScalarValue::Boolean(x) => x,
            _ => panic!("retraction shall be boolean"),
        };
        let multiplier = if !is_retraction { 1 } else { -1 };
        if is_retraction {
            self.count -= 1;
        } else {
            self.count += 1;
        }
        match value {
            ScalarValue::Int64(x) => {
                self.sum += x * multiplier;
            }
            _ => panic!("bad aggregate argument"),
        }
        self.count != 0
    }

    fn trigger(&self) -> ScalarValue {
        return ScalarValue::Int64(self.sum);
    }
}

pub struct Count {}

impl Aggregate for Count {
    fn output_type(&self, input_type: &DataType) -> Result<DataType, Error> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountAccumulator { count: 0 })
    }
}

#[derive(Debug)]
struct CountAccumulator {
    count: i64,
}

impl Accumulator for CountAccumulator {
    fn add(&mut self, value: ScalarValue, retract: ScalarValue) -> bool {
        let is_retraction = match retract {
            ScalarValue::Boolean(x) => x,
            _ => panic!("retraction shall be boolean"),
        };
        let multiplier = if !is_retraction { 1 } else { -1 };
        if is_retraction {
            self.count -= 1;
        } else {
            self.count += 1;
        }
        self.count != 0
    }

    fn trigger(&self) -> ScalarValue {
        return ScalarValue::Int64(self.count);
    }
}

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

impl Node for GroupBy {
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        let source_schema = self.source.schema()?;
        let mut key_fields: Vec<Field> = self
            .output_key_indices
            .iter()
            .map(|i| &self.key[*i])
            .map(|key_field| key_field.field_meta(&vec![], &source_schema))
            .collect::<Result<Vec<Field>, Error>>()?;

        let aggregated_field_types: Vec<DataType> = self
            .aggregated_exprs
            .iter()
            .enumerate()
            .map(|(i, column_expr)| {
                self.aggregates[i].output_type(column_expr.field_meta(&vec![], &source_schema)?.data_type())
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
        let source_schema = self.source.schema()?;

        let mut accumulators_map: BTreeMap<Vec<GroupByScalar>, Vec<Box<dyn Accumulator>>> =
            BTreeMap::new();
        let mut last_triggered_values: BTreeMap<Vec<GroupByScalar>, Vec<ScalarValue>> =
            BTreeMap::new();

        let key_types: Vec<DataType> = self
            .key
            .iter()
            .map(|key_expr| key_expr.field_meta(&vec![], &source_schema).unwrap().data_type().clone())
            .collect();

        let mut trigger: Box<dyn Trigger> = Box::new(CountingTrigger::new(key_types, 100));

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

                let mut key_vec: Vec<GroupByScalar> = Vec::with_capacity(key_columns.len());
                for i in 0..key_columns.len() {
                    key_vec.push(GroupByScalar::Int64(0))
                }

                for row in 0..aggregated_columns[0].len() {
                    create_key(key_columns.as_slice(), row, &mut key_vec);

                    let accumulators = accumulators_map.entry(key_vec.clone()).or_insert(
                        self.aggregates
                            .iter()
                            .map(|aggr| aggr.create_accumulator())
                            .collect(),
                    );
                    accumulators.iter_mut().enumerate().for_each(|(i, acc)| {
                        // TODO: remove if false
                        acc.add(
                            ScalarValue::Int64(
                                aggregated_columns[i]
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .value(row),
                            ),
                            ScalarValue::Boolean(false),
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
                let output_schema = self.schema()?;

                let mut retraction_key_columns = Vec::with_capacity(self.output_names.len());

                // Push retraction keys
                for key_index in 0..self.key.len() {
                    match output_schema.fields()[key_index].data_type() {
                        DataType::Utf8 => {
                            let mut array = StringBuilder::new(key_columns[0].len());
                            for row in 0..key_columns[0].len() {
                                create_key(key_columns.as_slice(), row, &mut key_vec);

                                if !last_triggered_values.contains_key(&key_vec) {
                                    continue;
                                }

                                match &key_vec[key_index] {
                                    GroupByScalar::Utf8(text) => {
                                        array.append_value(text.as_str()).unwrap()
                                    }
                                    _ => panic!("bug: key doesn't match schema"),
                                    // TODO: Maybe use as_any -> downcast?
                                }
                            }
                            retraction_key_columns.push(Arc::new(array.finish()) as ArrayRef);
                        }
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(key_columns[0].len());
                            for row in 0..key_columns[0].len() {
                                create_key(key_columns.as_slice(), row, &mut key_vec);

                                if !last_triggered_values.contains_key(&key_vec) {
                                    continue;
                                }

                                match key_vec[key_index] {
                                    GroupByScalar::Int64(n) => array.append_value(n).unwrap(),
                                    _ => panic!("bug: key doesn't match schema"),
                                    // TODO: Maybe use as_any -> downcast?
                                }
                            }
                            retraction_key_columns.push(Arc::new(array.finish()) as ArrayRef);
                        }
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
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(retraction_key_columns[0].len());
                            for row in 0..retraction_key_columns[0].len() {
                                create_key(retraction_key_columns.as_slice(), row, &mut key_vec);

                                let last_triggered = last_triggered_values.get(&key_vec);
                                let last_triggered_row = match last_triggered {
                                    None => continue,
                                    Some(v) => v,
                                };

                                match last_triggered_row[aggregate_index] {
                                    ScalarValue::Int64(n) => array.append_value(n).unwrap(),
                                    _ => panic!("bug: key doesn't match schema"),
                                    // TODO: Maybe use as_any -> downcast?
                                }
                            }
                            retraction_columns.push(Arc::new(array.finish()) as ArrayRef);
                        }
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
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(key_columns[0].len());

                            for row in 0..key_columns[0].len() {
                                create_key(key_columns.as_slice(), row, &mut key_vec);
                                // TODO: this key may not exist because of retractions.
                                let row_accumulators = accumulators_map.get(&key_vec).unwrap();

                                match row_accumulators[aggregate_index].trigger() {
                                    ScalarValue::Int64(n) => array.append_value(n).unwrap(),
                                    _ => panic!("bug: key doesn't match schema"),
                                    // TODO: Maybe use as_any -> downcast?
                                }

                                let mut last_values_vec =
                                    last_triggered_values.entry(key_vec.clone()).or_default();
                                last_values_vec.push(row_accumulators[aggregate_index].trigger());
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef);
                        }
                        _ => unimplemented!(),
                    }
                }

                // Combine key columns
                for col_index in 0..output_columns.len() {
                    match output_schema.fields()[col_index].data_type() {
                        DataType::Utf8 => {
                            let mut array = StringBuilder::new(
                                retraction_columns[0].len() + output_columns[0].len(),
                            );
                            array.append_data(&[
                                retraction_columns[col_index].data(),
                                output_columns[col_index].data(),
                            ]);
                            output_columns[col_index] = Arc::new(array.finish()) as ArrayRef;
                        }
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(
                                retraction_columns[0].len() + output_columns[0].len(),
                            );
                            array.append_data(&[
                                retraction_columns[col_index].data(),
                                output_columns[col_index].data(),
                            ]);
                            output_columns[col_index] = Arc::new(array.finish()) as ArrayRef;
                        }
                        _ => unimplemented!(),
                    }
                }

                // Add retraction array
                output_columns.push(retraction_array as ArrayRef);

                let new_batch = RecordBatch::try_new(output_schema, output_columns).unwrap();

                produce(&ProduceContext {}, new_batch);

                Ok(())
            },
            &mut noop_meta_send,
        )?;

        Ok(())
    }
}
