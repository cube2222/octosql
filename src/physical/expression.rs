use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayDataBuilder, ArrayRef, BooleanBufferBuilder, BufferBuilderTrait, Int64Builder, StringBuilder, StructArray};
use arrow::buffer::MutableBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::physical::arrow::{create_row, get_scalar_value, make_array};
use crate::physical::physical::{ExecutionContext, Identifier, Node, noop_meta_send, ScalarValue, SchemaContext, SchemaContextWithSchema, VariableContext};

pub trait Expression: Send + Sync {
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef>;

    fn evaluate_scalar(&self, ctx: &ExecutionContext) -> Result<ScalarValue> {
        let mut scalar_builder = Int64Builder::new(1);
        scalar_builder.append_value(1);
        let scalar_array = scalar_builder.finish();

        let scalar_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("", DataType::Int64, false)])),
            vec![Arc::new(scalar_array)],
        ).unwrap();

        let output_array = self.evaluate(ctx, &scalar_batch)?;
        get_scalar_value(&output_array, 0).context("couldn't get value from output array")
    }
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
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef> {
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
                    return Err(err)?;
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
    fn evaluate(&self, _ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef> {
        match &self.value {
            ScalarValue::Int64(n) => {
                let mut array = Int64Builder::new(record.num_rows());
                for _i in 0..record.num_rows() {
                    array.append_value(*n).unwrap();
                }
                Ok(Arc::new(array.finish()) as ArrayRef)
            }
            ScalarValue::Utf8(v) => {
                let mut array = StringBuilder::new(record.num_rows());
                for _i in 0..record.num_rows() {
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
    // TODO: Would probably be more elegant to gather vectors of record batches, and then do a type switch later, creating the final array in a typesafe way.
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef> {
        let source_schema = self.query.logical_metadata().schema;
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
            for _i in 0..record.num_columns() {
                row.push(ScalarValue::Null);
            }

            create_row(record.columns(), i, &mut row)?;

            let ctx = ExecutionContext {
                partition: 0,
                variable_context: Arc::new(VariableContext {
                    previous: Some(ctx.variable_context.clone()),
                    schema: record.schema().clone(),
                    variables: row,
                })
            };

            let mut batches = vec![];

            self.query.run(
                &ctx,
                &mut |_produce_ctx, batch| {
                    batches.push(batch);
                    Ok(())
                },
                &mut noop_meta_send,
            )?;

            if batches.len() == 0 || batches[0].num_rows() == 0 {
                null_bitmap_builder.append(false)?;
                buffer.resize(buffer.len() + single_value_byte_size)?;
                continue;
            }

            null_bitmap_builder.append(true)?;

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

pub struct WildcardExpression {
    qualifier: Option<String>,
}

impl WildcardExpression {
    pub fn new(qualifier: Option<&str>) -> WildcardExpression {
        let qualifier_with_dot = qualifier.map(|qualifier| {
            let mut qualifier_with_dot = qualifier.to_string();
            qualifier_with_dot.push_str(".");
            qualifier_with_dot
        });

        WildcardExpression { qualifier: qualifier_with_dot }
    }
}

impl Expression for WildcardExpression {
    fn evaluate(&self, _ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef> {
        let source_schema = record.schema();

        let tuple_elements = record.columns().iter()
            .enumerate()
            .filter(|(i, _)| {
                if let Some(qualifier) = &self.qualifier {
                    source_schema.field(i.clone()).name().starts_with(qualifier)
                } else {
                    true
                }
            })
            .map(|(i, col)| {
                let source_field = source_schema.field(i);
                (
                    Field::new(format!("{}", i).as_str(), source_field.data_type().clone(), source_field.is_nullable()),
                    col.clone(),
                )
            })
            .collect::<Vec<_>>();

        Ok(Arc::new(StructArray::from(tuple_elements)))
    }
}
