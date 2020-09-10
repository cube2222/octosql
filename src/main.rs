mod physical;
mod logical;
mod parser;

use crate::physical::physical::{noop_meta_send, ExecutionContext, ProduceContext, VariableContext, retractions_field, Identifier};
use crate::logical::logical::Expression::Variable;
use crate::logical::logical::MaterializationContext;
use crate::logical::logical::Node::{Map, Source, GroupBy, Filter};

use arrow::array::*;
use arrow::compute::kernels::filter;
use arrow::csv;
use arrow::csv::reader;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::ipc::writer::*;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use arrow::util::pretty::pretty_format_batches;
use std::fs::{read, File};
use std::io;
use std::io::Cursor;
use std::path;
use std::result::*;
use std::thread;
use std::time;
// use datafusion::logicalplan::ScalarValue;
use arrow::error::ArrowError;
use arrow::ipc::{BoolBuilder, Utf8Builder};
use datafusion::execution::physical_plan::common::get_scalar_value;
use datafusion::execution::physical_plan::hash_aggregate::HashAggregateExec;
use datafusion::execution::physical_plan::PhysicalExpr;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
use std::iter::repeat;
use std::sync::mpsc;
use std::sync::Arc;
use crate::logical::logical::Aggregate::{Count, Sum};

fn record_print(
    ctx: &ProduceContext,
    batch: RecordBatch,
) -> Result<(), physical::physical::Error> {
    println!("{}", batch.num_rows());
    println!("{}", pretty_format_batches(&[batch]).unwrap());
    Ok(())
}

fn main() {
    let start_time = std::time::Instant::now();

    // let plan: Arc<dyn Node> = Arc::new(CSVSource::new(String::from("cats.csv")));
    // let goals: Arc<dyn Node> = Arc::new(CSVSource::new("goals_big.csv"));
    // let teams: Arc<dyn Node> = Arc::new(CSVSource::new("teams.csv"));
    // let plan: Arc<dyn Node> = Arc::new(Map::new(plan, vec![Arc::new(FieldExpression::new("id".to_string())), Arc::new(FieldExpression::new("livesleft".to_string()))]));
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
    let logical_plan = Source {
        name: Identifier::SimpleIdentifier("cats.csv".to_string()),
    };
    let logical_plan = Map {
        source: Box::new(logical_plan),
        expressions: vec![Variable(Identifier::SimpleIdentifier("name".to_string())), Variable(Identifier::SimpleIdentifier("livesleft".to_string()))],
        keep_source_fields: false,
    };
    let logical_plan = GroupBy {
        source: Box::new(logical_plan),
        key_fields: vec![Identifier::SimpleIdentifier("livesleft".to_string())],
        aggregates: vec![Count(), Sum()],
        aggregated_fields: vec![Identifier::SimpleIdentifier("livesleft".to_string()), Identifier::SimpleIdentifier("livesleft".to_string())],
        output_fields: vec![Identifier::SimpleIdentifier("livesleft_count".to_string()), Identifier::SimpleIdentifier("livesleft_sum".to_string())]
    };
    // let logical_plan = Filter {
    //     source: Box::new(logical_plan),
    //     filter_column: retractions_field.to_string(),
    // };
    let plan = logical_plan.physical(&MaterializationContext {}).unwrap();

    let res = plan.run(
        &ExecutionContext {
            variable_context: Arc::new(VariableContext {
                previous: None,
                schema: Arc::new(Schema::new(vec![])), // Potential runtime variables.
                variables: vec![],
            }),
        },
        &mut record_print,
        &mut noop_meta_send,
    );
    println!("{:?}", start_time.elapsed());
}
