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
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::physical::expression::Expression;
use crate::physical::physical::*;

pub struct Map {
    source: Arc<dyn Node>,
    expressions: Vec<Arc<dyn Expression>>,
    names: Vec<Identifier>,
    wildcards: Vec<Option<String>>,
    keep_source_fields: bool,
}

impl Map {
    pub fn new(source: Arc<dyn Node>, expressions: Vec<Arc<dyn Expression>>, names: Vec<Identifier>, wildcards: Vec<Option<String>>, keep_source_fields: bool) -> Map {
        let wildcards = wildcards.into_iter()
            .map(|wildcard| {
                wildcard.map(|mut qualifier| {
                    qualifier.push_str(".");
                    qualifier
                })
            })
            .collect();

        Map {
            source,
            expressions,
            names,
            wildcards,
            keep_source_fields,
        }
    }
}

impl Node for Map {
    // TODO: Just don't allow to use retractions field as field name.
    fn schema(&self, schema_context: Arc<dyn SchemaContext>) -> Result<Arc<Schema>, Error> {
        let source_schema = self.source.schema(schema_context.clone())?;
        let mut new_schema_fields: Vec<Field> = self.wildcards.iter()
            .flat_map(|qualifier| {
                match qualifier {
                    Some(qualifier) => {
                        source_schema.fields().clone().into_iter()
                            .filter(|f| {
                                f.name().starts_with(qualifier)
                            })
                            .collect::<Vec<_>>()
                    }
                    None => {
                        source_schema.fields().clone().into_iter().collect::<Vec<_>>()
                    }
                }
            })
            .filter(|f| f.name() != RETRACTIONS_FIELD)
            .collect();

        new_schema_fields.extend(self
            .expressions
            .iter()
            .map(|expr| {
                expr.field_meta(schema_context.clone(), &source_schema)
                    .unwrap_or_else(|err| {
                        dbg!(err);
                        unimplemented!()
                    })
            })
            .enumerate()
            .map(|(i, field)| Field::new(self.names[i].to_string().as_str(), field.data_type().clone(), field.is_nullable())));
        if self.keep_source_fields {
            let mut to_append = new_schema_fields;
            new_schema_fields = source_schema.fields().clone();
            new_schema_fields.truncate(new_schema_fields.len() - 1); // Remove retraction field.
            new_schema_fields.append(&mut to_append);
        }
        new_schema_fields.push(Field::new(RETRACTIONS_FIELD, DataType::Boolean, false));
        Ok(Arc::new(Schema::new(new_schema_fields)))
    }

    fn run(
        &self,
        ctx: &ExecutionContext,
        produce: ProduceFn,
        _meta_send: MetaSendFn,
    ) -> Result<(), Error> {
        let source_schema = self.source.schema(ctx.variable_context.clone())?;
        let output_schema = self.schema(ctx.variable_context.clone())?;
        let wildcard_column_indices: Vec<usize> = self.wildcards.iter()
            .flat_map(|qualifier| {
                match qualifier {
                    Some(qualifier) => {
                        source_schema.fields().clone().into_iter()
                            .enumerate()
                            .filter(|(_column_index, f)| {
                                f.name().starts_with(qualifier)
                            })
                            .collect::<Vec<_>>()
                    }
                    None => {
                        source_schema.fields().clone().into_iter().enumerate().collect::<Vec<_>>()
                    }
                }
            })
            .filter(|(_column_index, f)| f.name() != RETRACTIONS_FIELD)
            .map(|(column_index, _f)| column_index)
            .collect();

        self.source.run(
            ctx,
            &mut |produce_ctx, batch| {
                let mut new_columns: Vec<ArrayRef> = wildcard_column_indices.iter()
                    .map(|column_index| batch.column(column_index.clone()).clone())
                    .collect();

                let mut new_evaluated_columns: Vec<ArrayRef> = self
                    .expressions
                    .iter()
                    .map(|expr| expr.evaluate(ctx, &batch))
                    .collect::<Result<_, _>>()?;
                new_columns.append(&mut new_evaluated_columns);
                new_columns.push(batch.column(batch.num_columns() - 1).clone());

                if self.keep_source_fields {
                    let mut to_append = new_columns;
                    new_columns = batch.columns().iter().cloned().collect();
                    new_columns.truncate(new_columns.len() - 1); // Remove retraction field.
                    new_columns.append(&mut to_append);
                }

                let new_batch = RecordBatch::try_new(output_schema.clone(), new_columns).unwrap();

                produce(produce_ctx, new_batch)?;
                Ok(())
            },
            &mut noop_meta_send,
        )?;
        Ok(())
    }
}
