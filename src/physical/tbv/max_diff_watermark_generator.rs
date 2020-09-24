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

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Int64Array, Int64Builder, StringArray, TimestampNanosecondArray};
use arrow::compute::kernels::aggregate::max;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::physical::expression::Expression;
use crate::physical::physical::{ExecutionContext, Identifier, MetadataMessage, MetaSendFn, Node, noop_meta_send, ProduceFn, ScalarValue, SchemaContext};

pub struct MaxDiffWatermarkGenerator {
    time_field_name: Arc<dyn Expression>,
    max_diff: Arc<dyn Expression>,
    source: Arc<dyn Node>,
}

impl MaxDiffWatermarkGenerator {
    // TODO: Add some kind of "Descriptor" for field names?
    // TODO: If I'd like to use a table, and not SELECT * FROM table, then I need "Table" for such names.
    // Or descriptor as well. Whatever.
    pub fn new(time_field_name: Arc<dyn Expression>, max_diff: Arc<dyn Expression>, source: Arc<dyn Node>) -> MaxDiffWatermarkGenerator {
        MaxDiffWatermarkGenerator {
            time_field_name,
            max_diff,
            source,
        }
    }
}

impl Node for MaxDiffWatermarkGenerator {
    fn schema(&self, schema_context: Arc<dyn SchemaContext>) -> Result<Arc<Schema>> {
        self.source.schema(schema_context.clone())
    }

    fn run(
        &self,
        exec_ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<()> {
        let time_field_name = if let ScalarValue::Utf8(v) = self.time_field_name.evaluate_scalar(exec_ctx)? {
            v
        } else {
            Err(anyhow!("time field name must be string"))?
        };

        let max_diff =  if let ScalarValue::Int64(v) = self.time_field_name.evaluate_scalar(exec_ctx)? {
            v
        } else {
            Err(anyhow!("max difference must be integer"))?
        };

        let source_schema = self.source.schema(exec_ctx.variable_context.clone())?;
        let (time_field_index, _) = source_schema.column_with_name(time_field_name.as_str()).context("time field not found")?;

        let mut cur_max: i64 = 0;

        self.source.run(
            exec_ctx,
            &mut |ctx, batch| {
                let time_array = batch.column(time_field_index).clone();
                let time_array_typed = time_array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .context("time field must be timestamp array")?;

                let max_time = max(time_array_typed).context("no max time value found")?;

                produce(ctx, batch)?;

                if max_time > cur_max {
                    cur_max = max_time;
                    meta_send(ctx, MetadataMessage::Watermark(max_time - max_diff))?;
                }

                Ok(())
            },
            &mut noop_meta_send,
        )?;
        Ok(())
    }
}
