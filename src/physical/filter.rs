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
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        self.source.schema()
    }

    fn run(
        &self,
        exec_ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<(), Error> {
        let source_schema = self.source.schema()?;

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
