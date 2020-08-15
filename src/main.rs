use arrow::record_batch::RecordBatch;
use arrow::csv::reader;
use arrow::ipc::writer::*;
use std::fs::{File, read};
use arrow::util::pretty;
use std::path;
use std::result::*;
use std::io::Cursor;
use arrow::ipc::writer::FileWriter;
use arrow::csv;
use std::io;
use std::time;
use std::thread;
use arrow::util::pretty::pretty_format_batches;
use arrow::array::*;
use arrow::datatypes::{Field, Schema, DataType};
use arrow::compute::kernels::filter;
// use datafusion::logicalplan::ScalarValue;
use datafusion::execution::physical_plan::PhysicalExpr;
use datafusion::execution::physical_plan::hash_aggregate::HashAggregateExec;
use datafusion::execution::physical_plan::common::get_scalar_value;
use std::sync::Arc;
use arrow::error::ArrowError;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use crate::Error::Unexpected;
use arrow::ipc::{Utf8Builder, BoolBuilder};
use std::iter::repeat;
use std::sync::mpsc;
use std::hash::Hash;

const batch_size: usize = 8192;
const retractions_fields: &str = "retraction";

pub struct ProduceContext {}

pub struct ExecutionContext {}

pub enum Error {
    IOError(io::Error),
    ArrowError(arrow::error::ArrowError),
    Unexpected,
}

impl From<arrow::error::ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Error::ArrowError(err)
    }
}

pub type ProduceFn<'a> = &'a mut dyn FnMut(&ProduceContext, RecordBatch) -> Result<(), Error>;
pub type MetaSendFn<'a> = &'a mut dyn FnMut(&ProduceContext, MetadataMessage) -> Result<(), Error>;

enum MetadataMessage {
    EndOfStream,
}

pub trait Node: Send + Sync {
    fn schema(&self) -> Result<Arc<Schema>, Error>;
    fn run(&self, ctx: &ExecutionContext, produce: ProduceFn, meta_send: MetaSendFn) -> Result<(), Error>;
}

fn record_print(ctx: &ProduceContext, batch: RecordBatch) -> Result<(), Error> {
    println!("{}", batch.num_rows());
    println!("{}", pretty_format_batches(&[batch]).unwrap());
    Ok(())
}

fn noop_meta_send(ctx: &ProduceContext, msg: MetadataMessage) -> Result<(), Error> {
    Ok(())
}

pub struct CSVSource<'a> {
    path: &'a str
}

impl<'a> CSVSource<'a> {
    fn new(path: &'a str) -> CSVSource<'a> {
        CSVSource { path }
    }
}

impl<'a> Node for CSVSource<'a> {
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        let file = File::open(self.path).unwrap();
        let r = csv::ReaderBuilder::new()
            .has_header(true)
            .infer_schema(Some(10))
            .with_batch_size(batch_size * 2)
            .build(file).unwrap();
        let mut fields = r.schema().fields().clone();
        fields.push(Field::new(retractions_fields, DataType::Boolean, false));

        Ok(Arc::new(Schema::new(fields)))
    }

    fn run(&self, ctx: &ExecutionContext, produce: ProduceFn, meta_send: MetaSendFn) -> Result<(), Error> {
        let file = File::open(self.path).unwrap();
        let mut r = csv::ReaderBuilder::new()
            .has_header(true)
            .infer_schema(Some(10))
            .with_batch_size(batch_size)
            .build(file).unwrap();
        let mut retraction_array_builder = BooleanBuilder::new(batch_size);
        for i in 0..batch_size {
            retraction_array_builder.append_value(false);
        }
        let retraction_array = Arc::new(retraction_array_builder.finish());
        let schema = self.schema()?;
        loop {
            let maybe_rec = r.next().unwrap();
            match maybe_rec {
                None => break,
                Some(rec) => {
                    let mut columns: Vec<ArrayRef> = rec.columns().iter().cloned().collect();
                    if columns[0].len() == batch_size {
                        columns.push(retraction_array.clone() as ArrayRef)
                    } else {
                        let mut retraction_array_builder = BooleanBuilder::new(batch_size);
                        for i in 0..columns[0].len() {
                            retraction_array_builder.append_value(false);
                        }
                        let retraction_array = Arc::new(retraction_array_builder.finish());
                        columns.push(retraction_array as ArrayRef)
                    }
                    produce(&ProduceContext {}, RecordBatch::try_new(schema.clone(), columns).unwrap())
                }
            };
        }
        Ok(())
    }
}

pub struct Projection<'a, 'b> {
    fields: &'b [&'a str],
    source: Arc<dyn Node>,
}

impl<'a, 'b> Projection<'a, 'b> {
    fn new(fields: &'b [&'a str], source: Arc<dyn Node>) -> Projection<'a, 'b> {
        Projection { fields, source }
    }

    fn schema_from_source_schema(&self, source_schema: Arc<Schema>) -> Result<Arc<Schema>, Error> {
        let new_schema_fields: Vec<Field> = self.fields
            .into_iter()
            .map(|&field| source_schema.index_of(field).unwrap())
            .map(|i| source_schema.field(i).clone())
            .collect();
        Ok(Arc::new(Schema::new(new_schema_fields)))
    }
}

impl<'a, 'b> Node for Projection<'a, 'b> {
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        let source_schema = self.source.schema()?;
        self.schema_from_source_schema(source_schema)
    }

    fn run(&self, ctx: &ExecutionContext, produce: ProduceFn, meta_send: MetaSendFn) -> Result<(), Error> {
        let source_schema = self.source.schema()?;
        let new_schema = self.schema_from_source_schema(source_schema.clone())?;

        let indices: Vec<usize> = self.fields.into_iter()
            .map(|&field| source_schema.index_of(field).unwrap())
            .collect();

        self.source.run(ctx, &mut |ctx, batch| {
            let new_columns: Vec<ArrayRef> = (&indices).into_iter()
                .map(|&i| batch.column(i).clone())
                .collect();

            let new_batch = RecordBatch::try_new(
                new_schema.clone(),
                new_columns,
            ).unwrap();

            produce(ctx, new_batch)?;
            Ok(())
        }, &mut noop_meta_send);
        Ok(())
    }
}

pub struct Filter<'a> {
    field: &'a str,
    source: Arc<dyn Node>,
}

impl<'a> Filter<'a> {
    fn new(field: &'a str, source: Arc<dyn Node>) -> Filter<'a> {
        Filter { field, source }
    }
}

impl<'a> Node for Filter<'a> {
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        self.source.schema()
    }

    fn run(&self, ctx: &ExecutionContext, produce: ProduceFn, meta_send: MetaSendFn) -> Result<(), Error> {
        let source_schema = self.source.schema()?;
        let index_of_field = source_schema.index_of(self.field)?;

        self.source.run(ctx, &mut |ctx, batch| {
            let predicate_column = batch.column(index_of_field).as_any().downcast_ref::<BooleanArray>().unwrap();
            let new_columns = batch
                .columns()
                .into_iter()
                .map(|array_ref| { filter::filter(array_ref.as_ref(), predicate_column).unwrap() })
                .collect();
            let new_batch = RecordBatch::try_new(
                source_schema.clone(),
                new_columns,
            ).unwrap();
            produce(ctx, batch);
            Ok(())
        }, &mut noop_meta_send);
        Ok(())
    }
}

pub trait Aggregate: Send + Sync {
    fn output_type(&self, input_schema: &DataType) -> Result<DataType, Error>;
    fn create_accumulator(&self) -> Box<dyn Accumulator>;
}

pub trait Accumulator: std::fmt::Debug {
    fn add(&mut self, value: ScalarValue, retract: ScalarValue) -> bool;
    fn trigger(&self) -> ScalarValue;
}

struct Sum {}

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
            _ => panic!("bad aggregate argument")
        }
        self.count != 0
    }

    fn trigger(&self) -> ScalarValue {
        return ScalarValue::Int64(self.sum);
    }
}

struct Count {}

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
    aggregates: Vec<Box<dyn Aggregate>>,
    output_names: Vec<String>,
    source: Arc<dyn Node>,
}

impl GroupBy {
    fn new(
        key: Vec<String>,
        aggregated_fields: Vec<String>,
        aggregates: Vec<Box<dyn Aggregate>>,
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
        let mut key_fields: Vec<Field> = self.key
            .iter()
            .map(|key_field| { source_schema.index_of(key_field).unwrap() })
            .map(|i| source_schema.field(i))
            .cloned()
            .collect();

        let aggregated_field_types: Vec<DataType> = self.aggregated_fields
            .iter()
            .map(|field| { source_schema.index_of(field.as_str()).unwrap() })
            .enumerate()
            .map(|(i, column_index)| {
                self.aggregates[i].output_type(source_schema.field(column_index).data_type())
            })
            .map(|t_res| match t_res {
                Ok(t) => t,
                Err(e) => panic!(e)
            })
            .collect();

        let mut new_fields: Vec<Field> = aggregated_field_types
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, t)| {
                Field::new(self.output_names[i].as_str(), t, false)
            })
            .collect();

        key_fields.append(&mut new_fields);
        key_fields.push(Field::new(retractions_fields, DataType::Boolean, false));
        Ok(Arc::new(Schema::new(key_fields)))
    }

    fn run(&self, ctx: &ExecutionContext, produce: ProduceFn, meta_send: MetaSendFn) -> Result<(), Error> {
        let source_schema = self.source.schema()?;
        let key_indices: Vec<usize> = self.key
            .iter()
            .map(|key_field| { source_schema.index_of(key_field).unwrap() })
            .collect();
        let aggregated_field_indices: Vec<usize> = self.aggregated_fields
            .iter()
            .map(|field| { source_schema.index_of(field.as_str()).unwrap() })
            .collect();

        let mut accumulators_map: BTreeMap<Vec<GroupByScalar>, Vec<Box<dyn Accumulator>>> = BTreeMap::new();
        let mut last_triggered_values: BTreeMap<Vec<GroupByScalar>, Vec<ScalarValue>> = BTreeMap::new();

        let key_types: Vec<DataType> = match self.source.schema() {
            Ok(schema) => self.key
                .iter()
                .map(|field| schema.field_with_name(field).unwrap().data_type())
                .cloned()
                .collect(),
            _ => panic!("aaa"),
        };
        let mut trigger: Box<dyn Trigger> = Box::new(CountingTrigger::new(key_types, 100));

        self.source.run(ctx, &mut |ctx, batch| {
            let key_columns: Vec<ArrayRef> = key_indices
                .iter()
                .map(|&i| { batch.column(i) })
                .cloned()
                .collect();
            let aggregated_columns: Vec<ArrayRef> = aggregated_field_indices
                .iter()
                .map(|&i| { batch.column(i) })
                .cloned()
                .collect();

            let mut key_vec: Vec<GroupByScalar> = Vec::with_capacity(key_columns.len());
            for i in 0..key_columns.len() {
                key_vec.push(GroupByScalar::Int64(0))
            }

            for row in 0..aggregated_columns[0].len() {
                create_key(key_columns.as_slice(), row, &mut key_vec);

                let accumulators = accumulators_map
                    .entry(key_vec.clone())
                    .or_insert(self.aggregates
                        .iter()
                        .map(|aggr| { aggr.create_accumulator() })
                        .collect());
                accumulators
                    .iter_mut()
                    .enumerate()
                    .for_each(|(i, acc)| {
                        // TODO: remove if false
                        acc.add(ScalarValue::Int64(aggregated_columns[i].as_any().downcast_ref::<Int64Array>().unwrap().value(i)), ScalarValue::Boolean(false));
                    })
            }

            trigger.keys_received(key_columns);

            // Check if we can trigger something
            let mut output_columns = trigger.poll();
            if output_columns[0].len() == 0 {
                return Ok(());
            }
            let output_schema = self.schema()?;

            let mut retraction_columns = Vec::with_capacity(self.output_names.len());

            // Push retraction keys
            for key_index in 0..self.key.len() {
                match output_schema.fields()[key_index].data_type() {
                    DataType::Utf8 => {
                        let mut array = StringBuilder::new(output_columns[0].len());
                        for row in 0..output_columns[0].len() {
                            create_key(output_columns.as_slice(), row, &mut key_vec);

                            if !last_triggered_values.contains_key(&key_vec) {
                                continue;
                            }

                            match &key_vec[key_index] {
                                GroupByScalar::Utf8(text) => array.append_value(text.as_str()).unwrap(),
                                _ => panic!("bug: key doesn't match schema"),
                                // TODO: Maybe use as_any -> downcast?
                            }
                        }
                        retraction_columns.push(Arc::new(array.finish()) as ArrayRef);
                    }
                    DataType::Int64 => {
                        let mut array = Int64Builder::new(output_columns[0].len());
                        for row in 0..output_columns[0].len() {
                            create_key(output_columns.as_slice(), row, &mut key_vec);

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
                            create_key(output_columns.as_slice(), row, &mut key_vec);

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
                create_key(output_columns.as_slice(), row, &mut key_vec);
                last_triggered_values.remove(&key_vec);
            }
            // Build retraction array
            let mut retraction_array_builder = BooleanBuilder::new(retraction_columns[0].len() + output_columns[0].len());
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
                            create_key(output_columns.as_slice(), row, &mut key_vec);
                            // TODO: this key may not exist because of retractions.
                            let row_accumulators = accumulators_map.get(&key_vec).unwrap();

                            match row_accumulators[aggregate_index].trigger() {
                                ScalarValue::Int64(n) => array.append_value(n).unwrap(),
                                _ => panic!("bug: key doesn't match schema"),
                                // TODO: Maybe use as_any -> downcast?
                            }

                            let mut last_values_vec = last_triggered_values
                                .entry(key_vec.clone())
                                .or_default();
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
                        let mut array = StringBuilder::new(retraction_columns[0].len() + output_columns[0].len());
                        array.append_data(&[retraction_columns[col_index].data(), output_columns[col_index].data()]);
                        output_columns[col_index] = Arc::new(array.finish()) as ArrayRef;
                    }
                    DataType::Int64 => {
                        let mut array = Int64Builder::new(retraction_columns[0].len() + output_columns[0].len());
                        array.append_data(&[retraction_columns[col_index].data(), output_columns[col_index].data()]);
                        output_columns[col_index] = Arc::new(array.finish()) as ArrayRef;
                    }
                    _ => unimplemented!(),
                }
            }

            // Add retraction array
            output_columns.push(retraction_array as ArrayRef);

            let new_batch = RecordBatch::try_new(
                output_schema,
                output_columns,
            ).unwrap();

            produce(&ProduceContext {}, new_batch);

            Ok(())
        }, &mut noop_meta_send)?;

        Ok(())
    }
}

pub trait Trigger: std::fmt::Debug {
    fn keys_received(&mut self, keys: Vec<ArrayRef>);
    fn poll(&mut self) -> Vec<ArrayRef>;
}

#[derive(Debug)]
pub struct CountingTrigger {
    key_data_types: Vec<DataType>,
    trigger_count: i64,
    counts: BTreeMap<Vec<GroupByScalar>, i64>,
    to_trigger: BTreeSet<Vec<GroupByScalar>>,
}

impl CountingTrigger {
    fn new(key_data_types: Vec<DataType>,
           trigger_count: i64) -> CountingTrigger {
        CountingTrigger {
            key_data_types,
            trigger_count,
            counts: Default::default(),
            to_trigger: Default::default(),
        }
    }
}

impl Trigger for CountingTrigger {
    fn keys_received(&mut self, keys: Vec<ArrayRef>) {
        let mut key_vec: Vec<GroupByScalar> = Vec::with_capacity(keys.len());
        for i in 0..self.key_data_types.len() {
            key_vec.push(GroupByScalar::Int64(0))
        }

        for row in 0..keys[0].len() {
            create_key(keys.as_slice(), row, &mut key_vec);

            let count = self.counts
                .entry(key_vec.clone())
                .or_insert(0);
            *count += 1;
            if *count == self.trigger_count {
                *count = 0; // TODO: Delete
                self.to_trigger.insert(key_vec.clone());
            }
        }
    }

    fn poll(&mut self) -> Vec<ArrayRef> {
        let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(self.key_data_types.len());
        for key_index in 0..self.key_data_types.len() {
            match self.key_data_types[key_index] {
                DataType::Utf8 => {
                    let mut array = StringBuilder::new(self.to_trigger.len());
                    self.to_trigger
                        .iter()
                        .for_each(|k| {
                            match &k[key_index] {
                                GroupByScalar::Utf8(text) => array.append_value(text.as_str()).unwrap(),
                                _ => panic!("bug: key doesn't match schema"),
                                // TODO: Maybe use as_any -> downcast?
                            }
                        });
                    output_columns.push(Arc::new(array.finish()) as ArrayRef);
                }
                DataType::Int64 => {
                    let mut array = Int64Builder::new(self.to_trigger.len());
                    self.to_trigger
                        .iter()
                        .for_each(|k| {
                            match k[key_index] {
                                GroupByScalar::Int64(n) => array.append_value(n).unwrap(),
                                _ => panic!("bug: key doesn't match schema"),
                                // TODO: Maybe use as_any -> downcast?
                            }
                        });
                    output_columns.push(Arc::new(array.finish()) as ArrayRef);
                }
                _ => unimplemented!(),
            }
        }
        self.to_trigger.clear();
        output_columns
    }
}

struct StreamJoin {
    source: Arc<Node>,
    source_key_fields: Vec<String>,
    joined: Arc<Node>,
    joined_key_fields: Vec<String>,
}

impl StreamJoin {
    fn new(source: Arc<Node>,
           source_key_fields: Vec<String>,
           joined: Arc<Node>,
           joined_key_fields: Vec<String>,
    ) -> StreamJoin {
        StreamJoin {
            source,
            source_key_fields,
            joined,
            joined_key_fields,
        }
    }
}

impl Node for StreamJoin {
    fn schema(&self) -> Result<Arc<Schema>, Error> { // Both without last row, and retraction added at end.
        let mut source_schema_fields = self.source.schema()?.fields().clone();
        source_schema_fields.truncate(source_schema_fields.len() - 1);
        let joined_schema_fields = self.joined.schema()?.fields().clone();
        let new_fields: Vec<Field> = source_schema_fields.iter()
            .map(|f| Field::new(f.name(), f.data_type().clone(), true))
            .chain(
                joined_schema_fields.iter()
                    .map(|f| Field::new(f.name(), f.data_type().clone(), true))
            ).collect();

        // TODO: Check if source and joined key types match.

        Ok(Arc::new(Schema::new(new_fields)))
    }

    fn run(&self, ctx: &ExecutionContext, produce: ProduceFn, meta_send: MetaSendFn) -> Result<(), Error> {
        let source_schema = self.source.schema()?;
        let source_key_indices: Vec<usize> = self.source_key_fields
            .iter()
            .map(|key_field| { source_schema.index_of(key_field).unwrap() })
            .collect();
        let joined_schema = self.joined.schema()?;
        let joined_key_indices: Vec<usize> = self.joined_key_fields
            .iter()
            .map(|key_field| { joined_schema.index_of(key_field).unwrap() })
            .collect();
        let output_schema = self.schema()?;

        // TODO: Fixme HashMap => BTreeMap
        let mut state_map: BTreeMap<Vec<GroupByScalar>, (HashMap<Vec<ScalarValue>, i64>, HashMap<Vec<ScalarValue>, i64>)> = BTreeMap::new();

        let key_types: Vec<DataType> = match self.source.schema() {
            Ok(schema) => self.source_key_fields
                .iter()
                .map(|field| schema.field_with_name(field).unwrap().data_type())
                .cloned()
                .collect(),
            _ => panic!("aaa"),
        };

        let (sender, receiver) = mpsc::sync_channel::<(usize, RecordBatch)>(32);

        let sender1 = sender.clone();
        let source = self.source.clone();
        let handle1 = std::thread::spawn(move || {
            let res = source.run(&ExecutionContext {}, &mut |ctx, batch| {
                sender1.send((0, batch));
                Ok(())
            }, &mut noop_meta_send);
        });
        let sender2 = sender.clone();
        let joined = self.joined.clone();
        let handle2 = std::thread::spawn(move || {
            let res = joined.run(&ExecutionContext {}, &mut |ctx, batch| {
                sender2.send((1, batch));
                Ok(())
            }, &mut noop_meta_send);
        });

        std::mem::drop(sender);

        let key_indices = vec![source_key_indices, joined_key_indices];

        for (source_index, batch) in receiver {
            let key_columns: Vec<ArrayRef> = key_indices[source_index]
                .iter()
                .map(|&i| batch.column(i))
                .cloned()
                .collect();

            let mut new_rows: BTreeMap<Vec<GroupByScalar>, Vec<Vec<ScalarValue>>> = BTreeMap::new();

            let mut required_capacity: usize = 0;

            for row in 0..batch.num_rows() {
                let mut key_vec = Vec::with_capacity(self.source_key_fields.len());
                for i in 0..key_columns.len() {
                    key_vec.push(GroupByScalar::Int64(0))
                }
                create_key(&key_columns, row, &mut key_vec);

                if let Some((source_rows, joined_rows)) = state_map.get(&key_vec) {
                    // The row will later be joined with the other source rows.
                    required_capacity += if source_index == 0 { joined_rows.len() } else { source_rows.len() };
                }

                let mut row_vec = Vec::with_capacity(batch.num_columns());
                for i in 0..batch.num_columns() {
                    row_vec.push(ScalarValue::Int64(0))
                }
                create_row(batch.columns(), row, &mut row_vec);

                let mut new_rows_for_key = new_rows.entry(key_vec).or_default();
                new_rows_for_key.push(row_vec);
            }

            let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(source_schema.fields().len() + joined_schema.fields().len());

            if source_index == 0 {
                for column in 0..(source_schema.fields().len() - 1) { // Omit the retraction field, it goes last.
                    match batch.column(column).data_type() {
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((_, state_other_rows)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Int64(n) = &new_row[column] {
                                                    array.append_value(*n);
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        DataType::Utf8 => {
                            let mut array = StringBuilder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((_, state_other_rows)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Utf8(text) = &new_row[column] {
                                                    array.append_value(text.as_str());
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        _ => unimplemented!(),
                    }
                }

                for column in 0..(joined_schema.fields().len() - 1) {
                    match joined_schema.field(column).data_type() {
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((_, state_other_rows)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Int64(n) = &other_row[column] {
                                                    array.append_value(*n);
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        DataType::Utf8 => {
                            let mut array = StringBuilder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((_, state_other_rows)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Utf8(text) = &other_row[column] {
                                                    array.append_value(text.as_str());
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        _ => unimplemented!(),
                    }
                }

                let retraction_column = batch.num_columns() - 1;

                let mut array = BooleanBuilder::new(required_capacity);
                for (key, cur_new_rows) in &new_rows {
                    if let Some((_, state_other_rows)) = state_map.get(key) {
                        for (other_row, &count) in state_other_rows {
                            for new_row in cur_new_rows {
                                for repetition in 0..count {
                                    if let ScalarValue::Boolean(retraction) = new_row[retraction_column] {
                                        array.append_value(retraction);
                                    } else {
                                        panic!("invalid type");
                                    }
                                }
                            }
                        }
                    }
                }
                output_columns.push(Arc::new(array.finish()) as ArrayRef)
            } else {
                for column in 0..(source_schema.fields().len() - 1) {
                    match source_schema.field(column).data_type() {
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((state_other_rows, _)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Int64(n) = &other_row[column] {
                                                    array.append_value(*n);
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        DataType::Utf8 => {
                            let mut array = StringBuilder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((state_other_rows, _)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Utf8(text) = &other_row[column] {
                                                    array.append_value(text.as_str());
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        _ => unimplemented!(),
                    }
                }

                for column in 0..(joined_schema.fields().len() - 1) { // Omit the retraction field, it goes last.
                    match batch.column(column).data_type() {
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((state_other_rows, _)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Int64(n) = &new_row[column] {
                                                    array.append_value(*n);
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        DataType::Utf8 => {
                            let mut array = StringBuilder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((state_other_rows, _)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Utf8(text) = &new_row[column] {
                                                    array.append_value(text.as_str());
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        _ => unimplemented!(),
                    }
                }

                let retraction_column = batch.num_columns() - 1;

                let mut array = BooleanBuilder::new(required_capacity);
                for (key, cur_new_rows) in &new_rows {
                    if let Some((state_other_rows, _)) = state_map.get(key) {
                        for (other_row, &count) in state_other_rows {
                            for new_row in cur_new_rows {
                                for repetition in 0..count {
                                    if let ScalarValue::Boolean(retraction) = new_row[retraction_column] {
                                        array.append_value(retraction);
                                    } else {
                                        panic!("invalid type");
                                    }
                                }
                            }
                        }
                    }
                }
                output_columns.push(Arc::new(array.finish()) as ArrayRef)
            }

            for (key, cur_new_rows) in new_rows {
                for mut row in cur_new_rows {
                    let (source_state, joined_state) = state_map.entry(key.clone()).or_insert((HashMap::default(), HashMap::default()));
                    let retraction = if let ScalarValue::Boolean(retraction) = row[row.len() - 1] {
                        retraction
                    } else {
                        panic!("invalid retraction type")
                    };
                    row.truncate(row.len() - 1);

                    let my_rows = if source_index == 0 { source_state } else { joined_state };
                    let my_entry = my_rows.entry(row).or_default();
                    if !retraction {
                        *my_entry += 1;
                    } else {
                        *my_entry -= 1;
                    }
                }
            }

            let output_batch = RecordBatch::try_new(output_schema.clone(), output_columns)?;
            if output_batch.num_rows() > 0 {
                produce(&ProduceContext {}, output_batch);
            }
        }

        handle1.join();
        handle2.join();

        Ok(())
    }
}

fn main() {
    let start_time = std::time::Instant::now();

    // let plan: Arc<dyn Node> = Arc::new(CSVSource::new("cats.csv"));
    let goals: Arc<dyn Node> = Arc::new(CSVSource::new("goals_big.csv"));
    let teams: Arc<dyn Node> = Arc::new(CSVSource::new("teams.csv"));
    //let plan: Arc<dyn Node> = Arc::new(Projection::new(&["id", "name"], plan));
    // let plan: Arc<dyn Node> = Arc::new(GroupBy::new(
    //     vec![String::from("name"), String::from("age")],
    //     vec![String::from("livesleft")],
    //     vec![Box::new(Sum {})],
    //     vec![String::from("livesleft")],
    //     plan,
    // ));
    // let plan: Arc<dyn Node> = Arc::new(GroupBy::new(
    //     vec![String::from("name")],
    //     vec![String::from("livesleft")],
    //     vec![Box::new(Sum {})],
    //     vec![String::from("livesleft")],
    //     plan,
    // ));
    let plan: Arc<dyn Node> = Arc::new(StreamJoin::new(goals.clone(), vec![String::from("team")], teams.clone(), vec![String::from("id")]));
    let plan: Arc<dyn Node> = Arc::new(GroupBy::new(
        vec![String::from("team")],
        vec![String::from("team")],
        vec![Box::new(Count {})],
        vec![String::from("count")],
        plan,
    ));
    let res = plan.run(&ExecutionContext {}, &mut record_print, &mut noop_meta_send);
    println!("{:?}", start_time.elapsed());
}

/// ScalarValue enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Float32(f32),
    Float64(f64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Utf8(String),
    Struct(Vec<ScalarValue>),
}

impl Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            ScalarValue::Null => (),
            ScalarValue::Boolean(x) => x.hash(state),
            ScalarValue::Float32(x) => unimplemented!(),
            ScalarValue::Float64(x) => unimplemented!(),
            ScalarValue::Int8(x) => x.hash(state),
            ScalarValue::Int16(x) => x.hash(state),
            ScalarValue::Int32(x) => x.hash(state),
            ScalarValue::Int64(x) => x.hash(state),
            ScalarValue::UInt8(x) => x.hash(state),
            ScalarValue::UInt16(x) => x.hash(state),
            ScalarValue::UInt32(x) => x.hash(state),
            ScalarValue::UInt64(x) => x.hash(state),
            ScalarValue::Utf8(x) => x.hash(state),
            ScalarValue::Struct(x) => x.hash(state),
        }
    }
}

impl Eq for ScalarValue {}

// ****** Copied from datafusion

/// Enumeration of types that can be used in a GROUP BY expression (all primitives except
/// for floating point numerics)
#[derive(Debug, PartialEq, Eq, Hash, Clone, Ord, PartialOrd)]
enum GroupByScalar {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Utf8(String),
}

/// Create a Vec<GroupByScalar> that can be used as a map key
fn create_key(
    group_by_keys: &[ArrayRef],
    row: usize,
    vec: &mut Vec<GroupByScalar>,
) -> Result<(), Error> {
    for i in 0..group_by_keys.len() {
        let col = &group_by_keys[i];
        match col.data_type() {
            DataType::UInt8 => {
                let array = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                vec[i] = GroupByScalar::UInt8(array.value(row))
            }
            DataType::UInt16 => {
                let array = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                vec[i] = GroupByScalar::UInt16(array.value(row))
            }
            DataType::UInt32 => {
                let array = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                vec[i] = GroupByScalar::UInt32(array.value(row))
            }
            DataType::UInt64 => {
                let array = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                vec[i] = GroupByScalar::UInt64(array.value(row))
            }
            DataType::Int8 => {
                let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
                vec[i] = GroupByScalar::Int8(array.value(row))
            }
            DataType::Int16 => {
                let array = col.as_any().downcast_ref::<Int16Array>().unwrap();
                vec[i] = GroupByScalar::Int16(array.value(row))
            }
            DataType::Int32 => {
                let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                vec[i] = GroupByScalar::Int32(array.value(row))
            }
            DataType::Int64 => {
                let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                vec[i] = GroupByScalar::Int64(array.value(row))
            }
            DataType::Utf8 => {
                let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                vec[i] = GroupByScalar::Utf8(String::from(array.value(row)))
            }
            _ => {
                return Err(Error::Unexpected);
            }
        }
    }
    Ok(())
}

fn create_row(
    columns: &[ArrayRef],
    row: usize,
    vec: &mut Vec<ScalarValue>,
) -> Result<(), Error> {
    for i in 0..columns.len() {
        let col = &columns[i];
        match col.data_type() {
            DataType::Boolean => {
                let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                vec[i] = ScalarValue::Boolean(array.value(row))
            }
            DataType::UInt8 => {
                let array = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                vec[i] = ScalarValue::UInt8(array.value(row))
            }
            DataType::UInt16 => {
                let array = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                vec[i] = ScalarValue::UInt16(array.value(row))
            }
            DataType::UInt32 => {
                let array = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                vec[i] = ScalarValue::UInt32(array.value(row))
            }
            DataType::UInt64 => {
                let array = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                vec[i] = ScalarValue::UInt64(array.value(row))
            }
            DataType::Int8 => {
                let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
                vec[i] = ScalarValue::Int8(array.value(row))
            }
            DataType::Int16 => {
                let array = col.as_any().downcast_ref::<Int16Array>().unwrap();
                vec[i] = ScalarValue::Int16(array.value(row))
            }
            DataType::Int32 => {
                let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                vec[i] = ScalarValue::Int32(array.value(row))
            }
            DataType::Int64 => {
                let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                vec[i] = ScalarValue::Int64(array.value(row))
            }
            DataType::Utf8 => {
                let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                vec[i] = ScalarValue::Utf8(String::from(array.value(row)))
            }
            _ => {
                return Err(Error::Unexpected);
            }
        }
    }
    Ok(())
}
