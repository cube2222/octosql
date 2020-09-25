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

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use anyhow::Result;

use crate::physical::physical::{ExecutionContext, MetaSendFn, Node, noop_meta_send, ProduceFn, SchemaContext, NodeMetadata};

pub struct Requalifier {
    qualifier: String,
    source: Arc<dyn Node>,
}

impl Requalifier {
    pub fn new(qualifier: String, source: Arc<dyn Node>) -> Requalifier {
        Requalifier { qualifier, source }
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
    fn metadata(&self, schema_context: Arc<dyn SchemaContext>) -> Result<NodeMetadata> {
        let source_metadata = self.source.metadata(schema_context.clone())?;
        let source_schema = source_metadata.schema.as_ref();
        let new_fields = source_schema.fields().iter()
            .map(|f| Field::new(self.requalify(f.name()).as_str(), f.data_type().clone(), f.is_nullable()))
            .collect();

        Ok(NodeMetadata{schema: Arc::new(Schema::new(new_fields)), time_field: source_metadata.time_field.clone() })
    }

    fn run(
        &self,
        exec_ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<()> {
        let schema = self.metadata(exec_ctx.variable_context.clone())?.schema.clone();

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