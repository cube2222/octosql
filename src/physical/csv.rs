use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder};
use arrow::csv;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::physical::physical::*;

pub struct CSVSource {
    path: String,
}

impl CSVSource {
    pub fn new(path: String) -> CSVSource {
        CSVSource { path }
    }
}

impl Node for CSVSource {
    fn schema(&self, schema_context: Arc<dyn SchemaContext>) -> Result<Arc<Schema>, Error> {
        let file = File::open(self.path.as_str()).unwrap();
        let r = csv::ReaderBuilder::new()
            .has_header(true)
            .infer_schema(Some(10))
            .with_batch_size(batch_size * 2)
            .build(file)
            .unwrap();
        let mut fields = r.schema().fields().clone();
        fields.push(Field::new(retractions_field, DataType::Boolean, false));

        Ok(Arc::new(Schema::new(fields)))
    }

    fn run(
        &self,
        ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<(), Error> {
        let file = File::open(self.path.as_str()).unwrap();
        let mut r = csv::ReaderBuilder::new()
            .has_header(true)
            .infer_schema(Some(10))
            .with_batch_size(batch_size)
            .build(file)
            .unwrap();
        let mut retraction_array_builder = BooleanBuilder::new(batch_size);
        for i in 0..batch_size {
            retraction_array_builder.append_value(false);
        }
        let retraction_array = Arc::new(retraction_array_builder.finish());
        let schema = self.schema(ctx.variable_context.clone())?;
        loop {
            let maybe_rec = r.next().unwrap();
            match maybe_rec {
                None => break,
                Some(rec) => {
                    let mut columns: Vec<ArrayRef> = rec.columns().iter().cloned().collect();
                    if columns[0].len() == batch_size {
                        columns.push(retraction_array.clone() as ArrayRef)
                    } else {
                        let mut retraction_array_builder = BooleanBuilder::new(batch_size);
                        for i in 0..columns[0].len() {
                            retraction_array_builder.append_value(false);
                        }
                        let retraction_array = Arc::new(retraction_array_builder.finish());
                        columns.push(retraction_array as ArrayRef)
                    }
                    produce(
                        &ProduceContext {},
                        RecordBatch::try_new(schema.clone(), columns).unwrap(),
                    )?
                }
            };
        }
        Ok(())
    }
}
