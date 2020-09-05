use crate::execution::datafusion::{create_key, create_row, GroupByScalar};
use crate::execution::execution::*;
use crate::execution::trigger::*;
use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Int64Array, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

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
    key: Vec<String>,
    aggregated_fields: Vec<String>,
    aggregates: Vec<Arc<dyn Aggregate>>,
    output_names: Vec<String>,
    source: Arc<dyn Node>,
}

impl GroupBy {
    pub fn new(
        key: Vec<String>,
        aggregated_fields: Vec<String>,
        aggregates: Vec<Arc<dyn Aggregate>>,
        output_names: Vec<String>,
        source: Arc<dyn Node>,
    ) -> GroupBy {
        return GroupBy {
            key,
            aggregated_fields,
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
            .key
            .iter()
            .map(|key_field| source_schema.index_of(key_field).unwrap())
            .map(|i| source_schema.field(i))
            .cloned()
            .collect();

        let aggregated_field_types: Vec<DataType> = self
            .aggregated_fields
            .iter()
            .map(|field| source_schema.index_of(field.as_str()).unwrap())
            .enumerate()
            .map(|(i, column_index)| {
                self.aggregates[i].output_type(source_schema.field(column_index).data_type())
            })
            .map(|t_res| match t_res {
                Ok(t) => t,
                Err(e) => panic!(e),
            })
            .collect();

        let mut new_fields: Vec<Field> = aggregated_field_types
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, t)| Field::new(self.output_names[i].as_str(), t, false))
            .collect();

        key_fields.append(&mut new_fields);
        key_fields.push(Field::new(retractions_field, DataType::Boolean, false));
        Ok(Arc::new(Schema::new(key_fields)))
    }

    fn run(
        &self,
        ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<(), Error> {
        let source_schema = self.source.schema()?;
        let key_indices: Vec<usize> = self
            .key
            .iter()
            .map(|key_field| source_schema.index_of(key_field).unwrap())
            .collect();
        let aggregated_field_indices: Vec<usize> = self
            .aggregated_fields
            .iter()
            .map(|field| source_schema.index_of(field.as_str()).unwrap())
            .collect();

        let mut accumulators_map: BTreeMap<Vec<GroupByScalar>, Vec<Box<dyn Accumulator>>> =
            BTreeMap::new();
        let mut last_triggered_values: BTreeMap<Vec<GroupByScalar>, Vec<ScalarValue>> =
            BTreeMap::new();

        let key_types: Vec<DataType> = match self.source.schema() {
            Ok(schema) => self
                .key
                .iter()
                .map(|field| schema.field_with_name(field).unwrap().data_type())
                .cloned()
                .collect(),
            _ => panic!("aaa"),
        };
        let mut trigger: Box<dyn Trigger> = Box::new(CountingTrigger::new(key_types, 100));

        self.source.run(
            ctx,
            &mut |ctx, batch| {
                let key_columns: Vec<ArrayRef> = key_indices
                    .iter()
                    .map(|&i| batch.column(i))
                    .cloned()
                    .collect();
                let aggregated_columns: Vec<ArrayRef> = aggregated_field_indices
                    .iter()
                    .map(|&i| batch.column(i))
                    .cloned()
                    .collect();

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
                let mut output_columns = key_columns.clone();
                let output_schema = self.schema()?;

                let mut retraction_columns = Vec::with_capacity(self.output_names.len());

                // Push retraction keys
                for key_index in 0..self.key.len() {
                    match output_schema.fields()[key_index].data_type() {
                        DataType::Utf8 => {
                            let mut array = StringBuilder::new(output_columns[0].len());
                            for row in 0..output_columns[0].len() {
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
                            retraction_columns.push(Arc::new(array.finish()) as ArrayRef);
                        }
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(output_columns[0].len());
                            for row in 0..output_columns[0].len() {
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
                            retraction_columns.push(Arc::new(array.finish()) as ArrayRef);
                        }
                        _ => unimplemented!(),
                    }
                }

                // Push retractions
                for aggregate_index in 0..self.aggregates.len() {
                    match output_schema.fields()[key_indices.len() + aggregate_index].data_type() {
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(output_columns[0].len());
                            for row in 0..output_columns[0].len() {
                                create_key(key_columns.as_slice(), row, &mut key_vec);

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
                        _ => unimplemented!(),
                    }
                }
                // Remove those values
                for row in 0..output_columns[0].len() {
                    create_key(key_columns.as_slice(), row, &mut key_vec);
                    last_triggered_values.remove(&key_vec);
                }
                // Build retraction array
                // TODO: BooleanBuilder => PrimitiveBuilder<BooleanType>. Maybe this can be refactored into a function after all.
                let mut retraction_array_builder =
                    BooleanBuilder::new(retraction_columns[0].len() + output_columns[0].len());
                for i in 0..retraction_columns[0].len() {
                    retraction_array_builder.append_value(true);
                }
                for i in 0..output_columns[0].len() {
                    retraction_array_builder.append_value(false);
                }
                let retraction_array = Arc::new(retraction_array_builder.finish());

                // Push new values
                for aggregate_index in 0..self.aggregates.len() {
                    match output_schema.fields()[key_indices.len() + aggregate_index].data_type() {
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(output_columns[0].len());

                            for row in 0..output_columns[0].len() {
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
