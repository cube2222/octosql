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

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate anyhow;

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use anyhow::Result;

use crate::logical::logical::MaterializationContext;
use crate::logical::sql::query_to_logical_plan;
use crate::parser::parser::parse_sql;
use crate::physical::physical::{EmptySchemaContext, ExecutionContext, noop_meta_send, ProduceContext, VariableContext};
use crate::pretty::pretty_format_batches;

#[macro_use]
mod physical;
mod logical;
mod parser;
mod pretty;

fn record_print(
    _ctx: &ProduceContext,
    batch: RecordBatch,
) -> Result<()> {
    println!("{}", batch.num_rows());
    println!("{}", pretty_format_batches(&[batch]).unwrap());
    Ok(())
}

// fn var(name: &str) -> Box<Expression> {
//     Box::new(Expression::Variable(Identifier::SimpleIdentifier(name.to_string())))
// }

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
    // let logical_plan = Source {
    //     name: Identifier::SimpleIdentifier("cats.csv".to_string()),
    //     alias: None,
    // };
    // let logical_plan = Map {
    //     source: Box::new(logical_plan),
    //     expressions: vec![Variable(Identifier::SimpleIdentifier("name".to_string())), Variable(Identifier::SimpleIdentifier("livesleft".to_string()))],
    //     keep_source_fields: false,
    // };
    // let logical_plan = GroupBy {
    //     source: Box::new(logical_plan),
    //     key_exprs: vec![var("livesleft")],
    //     aggregates: vec![Aggregate::KeyPart, Aggregate::KeyPart, Count, Sum],
    //     aggregated_exprs: vec![var("livesleft"), var("livesleft"), var("livesleft"), var("livesleft")],
    //     output_fields: vec![Identifier::SimpleIdentifier("livesleft_count".to_string()), Identifier::SimpleIdentifier("livesleft_sum".to_string())]
    // };
    // let logical_plan = Filter {
    //     source: Box::new(logical_plan),
    //     filter_column: RETRACTIONS_FIELD.to_string(),
    // };
    let sql = std::env::args().nth(1).unwrap();
    dbg!(&sql);

    let query = parse_sql(sql.as_str());
    dbg!(&query);
    let logical_plan = query_to_logical_plan(query.as_ref());
    dbg!(&logical_plan);

    dbg!(logical_plan.metadata(Arc::new(EmptySchemaContext{})).unwrap());

    let plan = logical_plan.physical(&MaterializationContext {}).unwrap();

    let schema = plan.metadata(Arc::new(EmptySchemaContext{})).unwrap();
    dbg!(schema);

    let _res = plan.run(
        &ExecutionContext {
            partition: 0,
            variable_context: Arc::new(VariableContext {
                previous: None,
                schema: Arc::new(Schema::new(vec![])), // Potential runtime variables.
                variables: vec![],
            }),
        },
        &mut record_print,
        &mut |ctx, msg| {
            dbg!(msg);
            Ok(())
        },
    ).unwrap();
    dbg!(start_time.elapsed());
    // println!("{:?}", start_time.elapsed());
}
