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
use arrow::array::{StringArray, TimestampNanosecondArray};
use arrow::compute::kernels::aggregate::max;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::physical::expression::Expression;
use crate::physical::physical::{ExecutionContext, Identifier, MetadataMessage, MetaSendFn, Node, noop_meta_send, ProduceFn, ScalarValue, SchemaContext};
use crate::logical::logical::NodeMetadata;

pub struct MaxDiffWatermarkGenerator {
    logical_metadata: NodeMetadata,
    time_field_name: Identifier,
    max_diff: Arc<dyn Expression>,
    source: Arc<dyn Node>,
}

impl MaxDiffWatermarkGenerator {
    pub fn new(logical_metadata: NodeMetadata, time_field_name: Identifier, max_diff: Arc<dyn Expression>, source: Arc<dyn Node>) -> MaxDiffWatermarkGenerator {
        MaxDiffWatermarkGenerator {
            logical_metadata,
            time_field_name,
            max_diff,
            source,
        }
    }
}

impl Node for MaxDiffWatermarkGenerator {
    fn logical_metadata(&self) -> NodeMetadata {
        self.logical_metadata.clone()
    }

    fn run(
        &self,
        exec_ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<()> {
        let max_diff =  if let ScalarValue::Duration(v) = self.max_diff.evaluate_scalar(exec_ctx)? {
            v
        } else {
            Err(anyhow!("max difference must be duration"))?
        };

        let source_schema = self.source.logical_metadata().schema;
        let (time_field_index, _) = source_schema.column_with_name(self.time_field_name.to_string().as_str()).context("time field not found")?;

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
