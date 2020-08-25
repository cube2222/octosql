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

pub struct Map {
    source: Arc<dyn Node>,
    expressions: Vec<Arc<dyn Expression>>,
}

impl Map {
    fn new(source: Arc<dyn Node>, expressions: Vec<Arc<dyn Expression>>) -> Map {
        Map { source, expressions }
    }
}

impl Node for Map {
    // TODO: Just don't allow to use retractions field as field name.
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        let source_schema = self.source.schema()?;
        let mut new_schema_fields: Vec<Field> = self.expressions
            .iter()
            .map(|expr| expr.field_meta(&vec![], &source_schema).unwrap_or_else(|err| unimplemented!()))
            .collect();
        new_schema_fields.push(Field::new(retractions_field, DataType::Boolean, false));
        Ok(Arc::new(Schema::new(new_schema_fields)))
    }

    fn run(&self, ctx: &ExecutionContext, produce: ProduceFn, meta_send: MetaSendFn) -> Result<(), Error> {
        let output_schema = self.schema()?;

        self.source.run(ctx, &mut |produce_ctx, batch| {
            let mut new_columns: Vec<ArrayRef> = self.expressions
                .iter()
                .map(|expr| expr.evaluate(ctx, &batch).unwrap_or_else(|err| unimplemented!()))
                .collect();
            new_columns.push(batch.column(batch.num_columns()-1).clone());

            let new_batch = RecordBatch::try_new(
                output_schema.clone(),
                new_columns,
            ).unwrap();

            produce(produce_ctx, new_batch)?;
            Ok(())
        }, &mut noop_meta_send);
        Ok(())
    }
}

pub trait Expression: Send + Sync {
    fn field_meta(&self, context_schema: &Vec<Arc<Schema>>, record_schema: &Arc<Schema>) -> Result<Field, Error>;
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error>;
}

struct FieldExpression {
    field: String
}

// TODO: Two phases, FieldExpression and RunningFieldExpression. First gets the schema and produces the second.
impl Expression for FieldExpression {
    fn field_meta(&self, context_schema: &Vec<Arc<Schema>>, record_schema: &Arc<Schema>) -> Result<Field, Error> {
        Ok(record_schema.field_with_name(self.field.as_str()).unwrap().clone())
    }
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error> {
        let record_schema: Arc<Schema> = record.schema();
        let field_index = record_schema.index_of(self.field.as_str()).unwrap();
        Ok(record.column(field_index).clone())
    }
}

fn main() {
    let start_time = std::time::Instant::now();

    let plan: Arc<dyn Node> = Arc::new(CSVSource::new("cats.csv"));
    // let goals: Arc<dyn Node> = Arc::new(CSVSource::new("goals_big.csv"));
    // let teams: Arc<dyn Node> = Arc::new(CSVSource::new("teams.csv"));
    let plan: Arc<dyn Node> = Arc::new(Map::new(plan, vec![Arc::new(FieldExpression{field: "id".to_string()}), Arc::new(FieldExpression{field: "livesleft".to_string()})]));
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
    // let plan: Arc<dyn Node> = Arc::new(StreamJoin::new(goals.clone(), vec![String::from("team")], teams.clone(), vec![String::from("id")]));
    // let plan: Arc<dyn Node> = Arc::new(GroupBy::new(
    //     vec![String::from("team")],
    //     vec![String::from("team")],
    //     vec![Box::new(Count {})],
    //     vec![String::from("count")],
    //     plan,
    // ));
    let res = plan.run(&ExecutionContext {
        variable_context: Arc::new(VariableContext {
            previous: None,
            schema: Arc::new(Schema::new(vec![])), // Potential runtime variables.
            variables: vec![],
        })
    }, &mut record_print, &mut noop_meta_send);
    println!("{:?}", start_time.elapsed());
}
