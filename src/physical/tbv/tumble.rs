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
use arrow::array::{ArrayRef, PrimitiveArrayOps, TimestampNanosecondArray, TimestampNanosecondBuilder};


use arrow::record_batch::RecordBatch;

use crate::physical::expression::Expression;
use crate::physical::physical::{ExecutionContext, Identifier, MetadataMessage, MetaSendFn, Node, noop_meta_send, ProduceFn, ScalarValue, SchemaContext};
use crate::logical::logical::NodeMetadata;
use arrow::error::ArrowError;

pub struct Tumble {
    logical_metadata: NodeMetadata,
    time_field_name: Identifier,
    window_size: Arc<dyn Expression>,
    offset: Arc<dyn Expression>,
    source: Arc<dyn Node>,
}

impl Tumble {
    pub fn new(logical_metadata: NodeMetadata, time_field_name: Identifier, window_size: Arc<dyn Expression>, offset: Arc<dyn Expression>, source: Arc<dyn Node>) -> Tumble {
        Tumble {
            logical_metadata,
            time_field_name,
            window_size,
            offset,
            source,
        }
    }
}

impl Node for Tumble {
    fn logical_metadata(&self) -> NodeMetadata {
        self.logical_metadata.clone()
    }

    fn run(
        &self,
        exec_ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<()> {
        let window_size = if let ScalarValue::Duration(v) = self.window_size.evaluate_scalar(exec_ctx)? {
            v
        } else {
            Err(anyhow!("window size must be a duration"))?
        };

        let offset = if let ScalarValue::Duration(v) = self.offset.evaluate_scalar(exec_ctx)? {
            v
        } else {
            Err(anyhow!("offset must be a duration"))?
        };

        let source_schema = self.source.logical_metadata().schema;
        let (time_field_index, _) = source_schema.column_with_name(self.time_field_name.to_string().as_str()).context("time field not found")?;

        self.source.run(
            exec_ctx,
            &mut |ctx, batch| {
                let time_array = batch.column(time_field_index).clone();
                let window_starts_res: Result<_, ArrowError> = compute_single_arg!(time_array, TimestampNanosecondArray, TimestampNanosecondBuilder, |x: i64| -> Result<i64> { Ok((x / window_size) * window_size + offset) } );
                let window_starts = window_starts_res?;
                let window_ends_res: Result<_, ArrowError> = compute_single_arg!(window_starts, TimestampNanosecondArray, TimestampNanosecondBuilder, |x: i64| -> Result<i64> { Ok(x + window_size) } );

                let mut columns = batch.columns().clone()[0..batch.num_columns() - 1].to_vec();
                columns.push(window_starts);
                columns.push(window_ends_res?);
                columns.push(batch.column(batch.num_columns() - 1).clone());

                produce(ctx, RecordBatch::try_new(self.logical_metadata.schema.clone(), columns)?)?;

                Ok(())
            },
            meta_send,
        )?;
        Ok(())
    }
}
