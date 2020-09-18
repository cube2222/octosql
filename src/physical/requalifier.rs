use std::sync::Arc;
use crate::physical::physical::{Node, Error, ExecutionContext, ProduceFn, MetaSendFn, noop_meta_send};
use arrow::datatypes::{Schema, Field};
use arrow::record_batch::RecordBatch;

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
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        let source_schema = self.source.schema()?;
        let new_fields = source_schema.fields().iter()
            .map(|f| Field::new(self.requalify(f.name()).as_str(), f.data_type().clone(), f.is_nullable()))
            .collect();

        Ok(Arc::new(Schema::new(new_fields)))
    }

    fn run(
        &self,
        exec_ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<(), Error> {
        let schema = self.schema()?;

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
            &mut noop_meta_send,
        )?;
        Ok(())
    }
}