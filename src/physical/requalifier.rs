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


use arrow::record_batch::RecordBatch;
use anyhow::Result;

use crate::physical::physical::{ExecutionContext, MetaSendFn, Node, noop_meta_send, ProduceFn, SchemaContext};
use crate::logical::logical::NodeMetadata;

pub struct Requalifier {
    logical_metadata: NodeMetadata,
    qualifier: String,
    source: Arc<dyn Node>,
}

impl Requalifier {
    pub fn new(logical_metadata: NodeMetadata, qualifier: String, source: Arc<dyn Node>) -> Requalifier {
        Requalifier { logical_metadata, qualifier, source }
    }
}

impl Requalifier {
    fn requalify(&self, str: &String) -> String {
        if let Some(i) = str.find(".") {
            format!("{}.{}", self.qualifier.as_str(), &str[i + 1..]).to_string()
        } else {
            format!("{}.{}", self.qualifier.as_str(), str.as_str()).to_string()
        }
    }
}

impl Node for Requalifier {
    fn logical_metadata(&self) -> NodeMetadata {
        self.logical_metadata.clone()
    }

    fn run(
        &self,
        exec_ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<()> {
        let schema = self.logical_metadata.schema.clone();

        self.source.run(
            exec_ctx,
            &mut |ctx, batch| {
                let new_batch = RecordBatch::try_new(
                    schema.clone(),
                    batch.columns().iter().cloned().collect(),
                )?;
                produce(ctx, new_batch)?;

                Ok(())
            },
            meta_send,
        )?;
        Ok(())
    }
}