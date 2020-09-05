mod execution;
mod logical;

use crate::execution::execution::*;
use crate::execution::csv::CSVSource;
use crate::execution::stream_join::StreamJoin;
use crate::execution::group_by::{GroupBy, Count};
use crate::execution::map::*;

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

fn main() {
    let start_time = std::time::Instant::now();

    let plan: Arc<dyn Node> = Arc::new(CSVSource::new("cats.csv"));
    // let goals: Arc<dyn Node> = Arc::new(CSVSource::new("goals_big.csv"));
    // let teams: Arc<dyn Node> = Arc::new(CSVSource::new("teams.csv"));
    let plan: Arc<dyn Node> = Arc::new(Map::new(plan, vec![Arc::new(FieldExpression::new("id".to_string())), Arc::new(FieldExpression::new("livesleft".to_string()))]));
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
