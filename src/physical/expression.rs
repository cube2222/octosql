use std::sync::Arc;

use arrow::array::{ArrayDataBuilder, ArrayRef, BooleanBufferBuilder, BufferBuilderTrait, Int64Builder, StringBuilder};
use arrow::buffer::MutableBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::physical::arrow::{create_row, make_array};
use crate::physical::physical::{Error, ExecutionContext, Identifier, Node, noop_meta_send, ScalarValue, SchemaContext, SchemaContextWithSchema, VariableContext};

pub trait Expression: Send + Sync {
    fn field_meta(
        &self,
        schema_context: Arc<dyn SchemaContext>,
        record_schema: &Arc<Schema>,
    ) -> Result<Field, Error>;
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error>;
}

pub struct FieldExpression {
    field: Identifier,
}

impl FieldExpression {
    pub fn new(field: Identifier) -> FieldExpression {
        FieldExpression { field }
    }
}

// TODO: Two phases, FieldExpression and RunningFieldExpression. First gets the schema and produces the second.
impl Expression for FieldExpression {
    fn field_meta(
        &self,
        schema_context: Arc<dyn SchemaContext>,
        record_schema: &Arc<Schema>,
    ) -> Result<Field, Error> {
        let field_name_string = self.field.to_string();
        let field_name = field_name_string.as_str();
        match record_schema.field_with_name(field_name) {
            Ok(field) => Ok(field.clone()),
            Err(arrow_err) => {
                match schema_context.field_with_name(field_name).map(|field| field.clone()) {
                    Ok(field) => Ok(field),
                    Err(err) => Err(Error::Wrapped(format!("{}", arrow_err), Box::new(err.into()))),
                }
            }
        }
    }
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error> {
        let record_schema: Arc<Schema> = record.schema();
        let field_name_string = self.field.to_string();
        let field_name = field_name_string.as_str();
        let field_index = record_schema.index_of(field_name);
        if let Err(err) = field_index {
            let mut variable_context = Some(ctx.variable_context.clone());
            loop {
                if let Some(var_ctx) = variable_context {
                    if let Ok(index) = var_ctx.schema.index_of(field_name) {
                        let val = var_ctx.variables[index].clone();
                        return Constant::new(val).evaluate(ctx, record);
                    }

                    variable_context = var_ctx.previous.clone();
                } else {
                    return Err(Error::from(err));
                }
            }
        } else {
            let index = field_index?;
            Ok(record.column(index).clone())
        }
    }
}

pub struct Constant {
    value: ScalarValue,
}

impl Constant {
    pub fn new(value: ScalarValue) -> Constant {
        Constant { value }
    }
}

impl Expression for Constant {
    fn field_meta(
        &self,
        schema_context: Arc<dyn SchemaContext>,
        record_schema: &Arc<Schema>,
    ) -> Result<Field, Error> {
        Ok(Field::new("", self.value.data_type(), self.value == ScalarValue::Null))
    }
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error> {
        match &self.value {
            ScalarValue::Int64(n) => {
                let mut array = Int64Builder::new(record.num_rows());
                for i in 0..record.num_rows() {
                    array.append_value(*n).unwrap();
                }
                Ok(Arc::new(array.finish()) as ArrayRef)
            }
            ScalarValue::Utf8(v) => {
                let mut array = StringBuilder::new(record.num_rows());
                for i in 0..record.num_rows() {
                    array.append_value(v.as_str()).unwrap();
                }
                Ok(Arc::new(array.finish()) as ArrayRef)
            }
            _ => {
                dbg!(self.value.data_type());
                unimplemented!()
            }
        }
    }
}

pub struct Subquery {
    query: Arc<dyn Node>,
}

impl Subquery {
    pub fn new(query: Arc<dyn Node>) -> Subquery {
        Subquery { query }
    }
}

impl Expression for Subquery {
    fn field_meta(
        &self,
        schema_context: Arc<dyn SchemaContext>,
        record_schema: &Arc<Schema>,
    ) -> Result<Field, Error> {
        let source_schema = self.query.schema(
            Arc::new(SchemaContextWithSchema {
                previous: schema_context.clone(),
                schema: record_schema.clone(),
            }),
        )?;
        // TODO: Implement for tuples.
        let field_base = source_schema.field(0);
        Ok(Field::new(field_base.name().as_str(), field_base.data_type().clone(), true))
    }

    // TODO: Would probably be more elegant to gather vectors of record batches, and then do a type switch later, creating the final array in a typesafe way.
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error> {
        let source_schema = self.query.schema(Arc::new(SchemaContextWithSchema {
            previous: ctx.variable_context.clone(),
            schema: record.schema(),
        }))?;
        let output_type = source_schema.field(0).data_type().clone();

        let single_value_byte_size: usize = match output_type {
            DataType::Int64 => 8,
            _ => unimplemented!(),
        };

        let builder = ArrayDataBuilder::new(output_type);
        let mut null_bitmap_builder = BooleanBufferBuilder::new(record.num_rows() * single_value_byte_size);
        let mut buffer = MutableBuffer::new(record.num_rows() * single_value_byte_size);

        for i in 0..record.num_rows() {
            let mut row = Vec::with_capacity(record.num_columns());
            for i in 0..record.num_columns() {
                row.push(ScalarValue::Null);
            }

            create_row(record.columns(), i, &mut row)?;

            let ctx = ExecutionContext {
                variable_context: Arc::new(VariableContext {
                    previous: Some(ctx.variable_context.clone()),
                    schema: record.schema().clone(),
                    variables: row,
                })
            };

            let mut batches = vec![];

            self.query.run(
                &ctx,
                &mut |produce_ctx, batch| {
                    batches.push(batch);
                    Ok(())
                },
                &mut noop_meta_send,
            )?;

            if batches.len() == 0 || batches[0].num_rows() == 0 {
                null_bitmap_builder.append(false);
                buffer.resize(buffer.len() + single_value_byte_size);
                continue;
            }

            null_bitmap_builder.append(true);

            if batches.len() != 1 {
                unimplemented!()
            }

            if batches[0].num_rows() != 1 {
                unimplemented!()
            }

            let cur_data = batches[0].column(0).data();
            let cur_buffer = &cur_data.buffers()[0];

            if cur_buffer.len() != single_value_byte_size {
                unimplemented!();
            }

            buffer.reserve(buffer.len() + single_value_byte_size).unwrap();
            buffer.write_bytes(cur_buffer.data(), 0).unwrap();
        }

        let builder = builder.add_buffer(buffer.freeze());
        let builder = builder.null_bit_buffer(null_bitmap_builder.finish());
        let builder = builder.len(record.num_rows());

        let output_array = make_array(builder.build());

        Ok(output_array)
    }
}