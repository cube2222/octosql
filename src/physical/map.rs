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

use arrow::array::ArrayRef;

use arrow::record_batch::RecordBatch;
use anyhow::Result;

use crate::physical::expression::Expression;
use crate::physical::physical::*;
use crate::logical::logical::NodeMetadata;

pub struct Map {
    logical_metadata: NodeMetadata,
    source: Arc<dyn Node>,
    expressions: Vec<Arc<dyn Expression>>,
    names: Vec<Identifier>,
}

impl Map {
    pub fn new(logical_metadata: NodeMetadata, source: Arc<dyn Node>, expressions: Vec<Arc<dyn Expression>>, names: Vec<Identifier>) -> Map {
        Map {
            logical_metadata,
            source,
            expressions,
            names,
        }
    }
}

impl Node for Map {
    fn logical_metadata(&self) -> NodeMetadata {
        self.logical_metadata.clone()
    }

    fn run(
        &self,
        ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<()> {
        let output_schema = self.logical_metadata.schema.clone();

        self.source.run(
            ctx,
            &mut |produce_ctx, batch| {
                let mut new_columns: Vec<ArrayRef> = self
                    .expressions
                    .iter()
                    .map(|expr| expr.evaluate(ctx, &batch))
                    .collect::<Result<_, _>>()?;
                new_columns.push(batch.column(batch.num_columns() - 1).clone());

                let new_batch = RecordBatch::try_new(output_schema.clone(), new_columns).unwrap();

                produce(produce_ctx, new_batch)?;
                Ok(())
            },
            meta_send,
        )?;
        Ok(())
    }
}
