mod execution;

use crate::execution::execution::*;
use crate::execution::csv::CSVSource;
use crate::execution::stream_join::StreamJoin;
use crate::execution::group_by::{GroupBy, Count};

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
use arrow::ipc::{Utf8Builder, BoolBuilder};
use std::iter::repeat;
use std::sync::mpsc;
use std::hash::Hash;

fn record_print(ctx: &ProduceContext, batch: RecordBatch) -> Result<(), Error> {
    println!("{}", batch.num_rows());
    println!("{}", pretty_format_batches(&[batch]).unwrap());
    Ok(())
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

pub trait Expression: Send + Sync {
    // DataType + Nullable
    fn data_type(&self, context_schema: Vec<Arc<Schema>>, record_schema: Arc<Schema>) -> Result<(DataType, bool), Error>;
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error>;
}

struct FieldExpression {
    field: String
}

impl Expression for FieldExpression {
    fn data_type(&self, context_schema: Vec<Arc<Schema>>, record_schema: Arc<Schema>) -> Result<(DataType, bool), Error> {
        unimplemented!()
    }
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error> {
        unimplemented!()
    }
}

// struct Map {
//     expressions: Vec<Expression>,
//     source: Arc<dyn Node>,
// }
//
// impl Node for Map {
//
// }

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
    let res = plan.run(&ExecutionContext {
        variable_context: Arc::new(VariableContext {
            previous: None,
            schema: Arc::new(Schema::new(vec![])), // Potential runtime variables.
            variables: vec![],
        })
    }, &mut record_print, &mut noop_meta_send);
    println!("{:?}", start_time.elapsed());
}
