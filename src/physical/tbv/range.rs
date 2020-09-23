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

use crate::physical::expression::Expression;
use crate::physical::physical::{Node, ExecutionContext, SchemaContext, ProduceContext, ProduceFn, MetaSendFn, BATCH_SIZE, RETRACTIONS_FIELD};
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{Int64Builder, Int64Array, BooleanBuilder};
use anyhow::Result;
use std::cmp::min;

pub struct Range {
    start: Arc<dyn Expression>,
    end: Arc<dyn Expression>,
}

impl Range {
    pub fn new(start: Arc<dyn Expression>, end: Arc<dyn Expression>) -> Self {
        Range { start, end }
    }
}

impl Node for Range {
    fn schema(&self, schema_context: Arc<dyn SchemaContext>) -> Result<Arc<Schema>> {
        Ok(Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, false),
            Field::new(RETRACTIONS_FIELD, DataType::Boolean, false),
        ])))
    }

    // TODO: Put batchsize into execution context.
    fn run(&self, ctx: &ExecutionContext, produce: ProduceFn, meta_send: MetaSendFn) -> Result<()> {
        // Create retraction_array
        let mut retraction_array_builder = BooleanBuilder::new(BATCH_SIZE);
        for _i in 0..BATCH_SIZE {
            retraction_array_builder.append_value(false)?;
        }
        let retraction_array = Arc::new(retraction_array_builder.finish());

        // TODO: Maybe add some kind of "ScalarEvaluate"?
        let mut scalar_builder = Int64Builder::new(1);
        scalar_builder.append_value(1);
        let scalar_array = scalar_builder.finish();

        let scalar_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("", DataType::Int64, false)])),
            vec![Arc::new(scalar_array)],
        ).unwrap();
        let start_array = self.start.evaluate(ctx, &scalar_batch)?;
        let mut start: i64 = if let Some(array) = start_array.as_any().downcast_ref::<Int64Array>() {
            array.value(0)
        } else {
            return Err(anyhow!("range start must be integer"));
        };
        let end_array = self.end.evaluate(ctx, &scalar_batch)?;
        let end: i64 = if let Some(array) = end_array.as_any().downcast_ref::<Int64Array>() {
            array.value(0)
        } else {
            return Err(anyhow!("range end must be integer"));
        };

        let output_schema = self.schema(ctx.variable_context.clone())?;

        while start < end {
            let batch_end = min(start + (BATCH_SIZE as i64), end);

            let mut batch_builder = Int64Builder::new((batch_end - start) as usize);
            for i in start..batch_end {
                batch_builder.append_value(i)?
            }

            let batch_retraction_array = if end - start == (BATCH_SIZE as i64) {
                retraction_array.clone()
            } else {
                let mut retraction_array_builder = BooleanBuilder::new(BATCH_SIZE);
                for i in start..batch_end {
                    retraction_array_builder.append_value(false)?;
                }
                Arc::new(retraction_array_builder.finish())
            };

            let record_batch = RecordBatch::try_new(
                output_schema.clone(),
                vec![
                    Arc::new(batch_builder.finish()),
                    batch_retraction_array,
                ],
            )?;

            produce(&ProduceContext {}, record_batch)?;

            start = batch_end;
        }
        Ok(())
    }
}
