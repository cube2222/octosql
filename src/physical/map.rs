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

use std::io::Write;
use std::mem;
use std::sync::Arc;

use arrow::array::{ArrayDataBuilder, ArrayDataRef, ArrayRef, BooleanBufferBuilder, BufferBuilderTrait, Int32Builder, Int64Builder, StringBuilder};
use arrow::array::{BinaryArray, BooleanArray, Date32Array, Date64Array, DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray, IntervalYearMonthArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, NullArray, StringArray, StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array, UnionArray};
use arrow::buffer::MutableBuffer;
use arrow::compute::kernels::comparison::eq;
use arrow::datatypes::{DataType, DateUnit, Field, Int16Type, Int32Type, Int64Type, Int8Type, IntervalUnit, Schema, TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use arrow::record_batch::RecordBatch;

use crate::physical::arrow::create_row;
use crate::physical::expression::Expression;
use crate::physical::physical::*;

pub struct Map {
    source: Arc<dyn Node>,
    expressions: Vec<Arc<dyn Expression>>,
    names: Vec<Identifier>,
    keep_source_fields: bool,
}

impl Map {
    pub fn new(source: Arc<dyn Node>, expressions: Vec<Arc<dyn Expression>>, names: Vec<Identifier>, keep_source_fields: bool) -> Map {
        Map {
            source,
            expressions,
            names,
            keep_source_fields,
        }
    }
}

impl Node for Map {
    // TODO: Just don't allow to use retractions field as field name.
    fn schema(&self, schema_context: Arc<dyn SchemaContext>) -> Result<Arc<Schema>, Error> {
        let source_schema = self.source.schema(schema_context.clone())?;
        let mut new_schema_fields: Vec<Field> = self
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
            .map(|(i, field)| Field::new(self.names[i].to_string().as_str(), field.data_type().clone(), field.is_nullable()))
            .collect();
        if self.keep_source_fields {
            let mut to_append = new_schema_fields;
            new_schema_fields = source_schema.fields().clone();
            new_schema_fields.truncate(new_schema_fields.len() - 1); // Remove retraction field.
            new_schema_fields.append(&mut to_append);
        }
        new_schema_fields.push(Field::new(retractions_field, DataType::Boolean, false));
        Ok(Arc::new(Schema::new(new_schema_fields)))
    }

    fn run(
        &self,
        ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<(), Error> {
        let output_schema = self.schema(ctx.variable_context.clone())?;

        self.source.run(
            ctx,
            &mut |produce_ctx, batch| {
                let mut new_columns: Vec<ArrayRef> = self
                    .expressions
                    .iter()
                    .map(|expr| expr.evaluate(ctx, &batch))
                    .collect::<Result<_, _>>()?;
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
