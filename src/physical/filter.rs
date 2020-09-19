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

use arrow::array::BooleanArray;
use arrow::compute::kernels::filter;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::physical::map::Expression;
use crate::physical::physical::*;

pub struct Filter {
    filter_expr: Arc<dyn Expression>,
    source: Arc<dyn Node>,
}

impl Filter {
    pub fn new(filter_expr: Arc<dyn Expression>, source: Arc<dyn Node>) -> Filter {
        Filter { filter_expr, source }
    }
}

impl Node for Filter {
    fn schema(&self, schema_context: Arc<dyn SchemaContext>) -> Result<Arc<Schema>, Error> {
        self.source.schema(schema_context.clone())
    }

    fn run(
        &self,
        exec_ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<(), Error> {
        let source_schema = self.source.schema(exec_ctx.variable_context.clone())?;

        self.source.run(
            exec_ctx,
            &mut |ctx, batch| {
                let predicate_column_untyped = self.filter_expr
                    .evaluate(exec_ctx, &batch)?;
                let predicate_column = predicate_column_untyped
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                let new_columns = batch
                    .columns()
                    .into_iter()
                    .map(|array_ref| filter::filter(array_ref.as_ref(), predicate_column).unwrap())
                    .collect();
                let new_batch = RecordBatch::try_new(source_schema.clone(), new_columns).unwrap();
                if new_batch.num_rows() > 0 {
                    produce(ctx, new_batch)?;
                }
                Ok(())
            },
            &mut noop_meta_send,
        )?;
        Ok(())
    }
}
