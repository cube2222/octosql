use crate::execution::execution::*;
use std::sync::Arc;
use arrow::compute::kernels::filter;
use arrow::array::BooleanArray;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

pub struct Filter<'a> {
    field: &'a str,
    source: Arc<dyn Node>,
}

impl<'a> Filter<'a> {
    fn new(field: &'a str, source: Arc<dyn Node>) -> Filter<'a> {
        Filter { field, source }
    }
}

impl<'a> Node for Filter<'a> {
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        self.source.schema()
    }

    fn run(&self, ctx: &ExecutionContext, produce: ProduceFn, meta_send: MetaSendFn) -> Result<(), Error> {
        let source_schema = self.source.schema()?;
        let index_of_field = source_schema.index_of(self.field)?;

        self.source.run(ctx, &mut |ctx, batch| {
            let predicate_column = batch.column(index_of_field).as_any().downcast_ref::<BooleanArray>().unwrap();
            let new_columns = batch
                .columns()
                .into_iter()
                .map(|array_ref| { filter::filter(array_ref.as_ref(), predicate_column).unwrap() })
                .collect();
            let new_batch = RecordBatch::try_new(
                source_schema.clone(),
                new_columns,
            ).unwrap();
            produce(ctx, batch);
            Ok(())
        }, &mut noop_meta_send);
        Ok(())
    }
}