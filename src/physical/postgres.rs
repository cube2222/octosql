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

use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder};
use arrow::csv;

use arrow::record_batch::RecordBatch;
use anyhow::Result;

use crate::physical::physical::*;
use crate::logical::logical::NodeMetadata;

// pub struct CSVSource {
//     logical_metadata: NodeMetadata,
//     path: String,
// }
//
// impl CSVSource {
//     pub fn new(logical_metadata: NodeMetadata, path: String) -> CSVSource {
//         CSVSource { logical_metadata, path }
//     }
// }
//
// impl Node for CSVSource {
//     fn logical_metadata(&self) -> NodeMetadata {
//         self.logical_metadata.clone()
//     }
//
//     fn run(
//         &self,
//         _ctx: &ExecutionContext,
//         produce: ProduceFn,
//         _meta_send: MetaSendFn,
//     ) -> Result<()> {
//         let file = File::open(self.path.as_str()).unwrap();
//         let mut r = csv::ReaderBuilder::new()
//             .has_header(true)
//             .infer_schema(Some(10))
//             .with_batch_size(BATCH_SIZE)
//             .build(file)
//             .unwrap();
//         let mut retraction_array_builder = BooleanBuilder::new(BATCH_SIZE);
//         for _i in 0..BATCH_SIZE {
//             retraction_array_builder.append_value(false)?;
//         }
//         let retraction_array = Arc::new(retraction_array_builder.finish());
//         let schema = self.logical_metadata.schema.clone();
//         loop {
//             let maybe_rec = r.next();
//             match maybe_rec {
//                 None => break,
//                 Some(rec) => {
//                     let mut columns: Vec<ArrayRef> = rec?.columns().iter().cloned().collect();
//                     if columns[0].len() == BATCH_SIZE {
//                         columns.push(retraction_array.clone() as ArrayRef)
//                     } else {
//                         let mut retraction_array_builder = BooleanBuilder::new(BATCH_SIZE);
//                         for _i in 0..columns[0].len() {
//                             retraction_array_builder.append_value(false)?;
//                         }
//                         let retraction_array = Arc::new(retraction_array_builder.finish());
//                         columns.push(retraction_array as ArrayRef)
//                     }
//                     produce(
//                         &ProduceContext {},
//                         RecordBatch::try_new(schema.clone(), columns).unwrap(),
//                     )?
//                 }
//             };
//         }
//         Ok(())
//     }
// }

use postgres::{Client, NoTls};
use postgres::types::Type;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::datatypes::TimeUnit::Nanosecond;

pub fn infer_schema(name: String) -> Result<NodeMetadata> {
    let mut client = Client::connect("host=localhost user=postgres password=postgres", NoTls).unwrap();

    let mut fields = vec![];
    for row in client.query("SELECT column_name, data_type, is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_name = $1;", &[&name])? {
        let column_name: &str = row.get(0);
        let type_name: &str = row.get(1);
        let data_type = get_arrow_type(type_name);
        let is_nullable: &str = row.get(2);
        fields.push(Field::new(column_name, data_type, is_nullable == "YES"));
    }

    Ok(NodeMetadata{
        partition_count: 1,
        schema: Arc::new(Schema::new(fields)),
        time_column: None,
    })
}

fn get_arrow_type(typename: &str) -> DataType {
    match typename {
        "boolean" => DataType::Boolean,
        // "char" => i8, TODO: Handle single character.
        "smallint" | "smallserial" => DataType::Int64,
        "integer" | "serial" => DataType::Int32,
        // "OID" => u32,
        "bigint" | "bigserial" => DataType::Int64,
        "real" => DataType::Float32,
        "double precision" => DataType::Float64,
        "character varying" | "character" | "text" => DataType::Utf8,
        "bytea" => DataType::Binary,
        // "hstore" => {}
        "timestamp" => DataType::Timestamp(Nanosecond, None), // TODO: Convert from millisecond.
        // "timestamp with time zone" => {},
        // "INET" => {}
        "json" | "jsonb" => DataType::Utf8, // TODO: testme
        _ => DataType::Utf8 // Unknown, TODO: testme
    }
}

// #[test]
// fn test() {
//     let mut client = Client::connect("host=localhost user=postgres password=postgres", NoTls).unwrap();
//
//     client.batch_execute("
//     CREATE TABLE person (
//         id      SERIAL PRIMARY KEY,
//         name    TEXT NOT NULL,
//         name2   VARCHAR(64),
//         test boolean,
//         test2 CHAR,
//         test3 CHAR(32),
//         data    BYTEA
//     )
// ").unwrap();
//
//     let name = "Ferris";
//     let data = None::<&[u8]>;
//     client.execute(
//         "INSERT INTO person (name, data) VALUES ($1, $2)",
//         &[&name, &data],
//     ).unwrap();
//
//     for row in client.query("SELECT id, name, data FROM person", &[]).unwrap() {
//         let id: i32 = row.get(0);
//         let name: &str = row.get(1);
//         let data: Option<&[u8]> = row.get(2);
//
//         println!("found person: {} {} {:?}", id, name, data);
//     }
// }
