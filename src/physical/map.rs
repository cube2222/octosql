use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Builder, Int32Builder, ArrayDataBuilder, ArrayDataRef};
use arrow::array::{BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array, Float32Array, Float64Array, Date32Array, Date64Array, Time32SecondArray, Time32MillisecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampSecondArray, TimestampMillisecondArray, TimestampMicrosecondArray, TimestampNanosecondArray, IntervalYearMonthArray, IntervalDayTimeArray, DurationSecondArray, DurationMillisecondArray, DurationMicrosecondArray, DurationNanosecondArray, BinaryArray, LargeBinaryArray, FixedSizeBinaryArray, StringArray, LargeStringArray, ListArray, LargeListArray, StructArray, UnionArray, FixedSizeListArray, NullArray, DictionaryArray};
use arrow::datatypes::{DataType, Field, Schema, DateUnit, TimeUnit, IntervalUnit, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type};
use arrow::compute::kernels::comparison::eq;
use arrow::record_batch::RecordBatch;

use crate::physical::physical::*;
use arrow::buffer::MutableBuffer;
use std::mem;
use std::io::Write;

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
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        let source_schema = self.source.schema()?;
        let mut new_schema_fields: Vec<Field> = self
            .expressions
            .iter()
            .map(|expr| {
                expr.field_meta(&vec![], &source_schema)
                    .unwrap_or_else(|err| unimplemented!())
            })
            .enumerate()
            .map(|(i, field)| Field::new(self.names[i].to_string().as_str(), field.data_type().clone(), field.is_nullable()))
            .collect();
        if self.keep_source_fields {
            let mut to_append = new_schema_fields;
            new_schema_fields = source_schema.fields().clone();
            new_schema_fields.truncate(new_schema_fields.len()-1); // Remove retraction field.
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
        let output_schema = self.schema()?;

        self.source.run(
            ctx,
            &mut |produce_ctx, batch| {
                let mut new_columns: Vec<ArrayRef> = self
                    .expressions
                    .iter()
                    .map(|expr| {
                        expr.evaluate(ctx, &batch)
                            .unwrap_or_else(|err| unimplemented!())
                    })
                    .collect();
                new_columns.push(batch.column(batch.num_columns() - 1).clone());

                if self.keep_source_fields {
                    let mut to_append = new_columns;
                    new_columns = batch.columns().iter().cloned().collect();
                    new_columns.truncate(new_columns.len()-1); // Remove retraction field.
                    new_columns.append(&mut to_append);
                }

                let new_batch = RecordBatch::try_new(output_schema.clone(), new_columns).unwrap();

                produce(produce_ctx, new_batch)?;
                Ok(())
            },
            &mut noop_meta_send,
        );
        Ok(())
    }
}

pub trait Expression: Send + Sync {
    fn field_meta(
        &self,
        context_schema: &Vec<Arc<Schema>>,
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
        context_schema: &Vec<Arc<Schema>>,
        record_schema: &Arc<Schema>,
    ) -> Result<Field, Error> {
        Ok(record_schema
            .field_with_name(self.field.to_string().as_str())
            .unwrap()
            .clone())
    }
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error> {
        let record_schema: Arc<Schema> = record.schema();
        let field_index = record_schema.index_of(self.field.to_string().as_str()).unwrap();
        Ok(record.column(field_index).clone())
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
        context_schema: &Vec<Arc<Schema>>,
        record_schema: &Arc<Schema>,
    ) -> Result<Field, Error> {
        Ok(Field::new("", self.value.data_type(), self.value == ScalarValue::Null))
    }
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error> {
        match self.value {
            ScalarValue::Int64(n) => {
                let mut array = Int64Builder::new(record.num_rows());
                for i in 0..record.num_rows() {
                    array.append_value(n).unwrap();
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
        context_schema: &Vec<Arc<Schema>>,
        record_schema: &Arc<Schema>,
    ) -> Result<Field, Error> {
        let source_schema = self.query.schema()?;
        // TODO: Implement for tuples.
        Ok(source_schema.field(0).clone())
    }

    // TODO: Would probably be more elegant to gather vectors of record batches, and then do a type switch later, creating the final array in a typesafe way.
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error> {
        let source_schema = self.query.schema()?;
        let output_type = source_schema.field(0).data_type().clone();
        let builder = ArrayDataBuilder::new(output_type);
        let mut buffer = MutableBuffer::new(0);

        for i in 0..record.num_rows() {
            let mut batches = vec![];

            self.query.run(
                ctx,
                &mut |produce_ctx, batch| {
                    batches.push(batch);
                    Ok(())
                },
                &mut noop_meta_send,
            )?;

            if batches.len() != 1 {
                unimplemented!()
            }

            if batches[0].num_rows() != 1 {
                unimplemented!()
            }

            let cur_data = batches[0].column(0).data();

            let cur_buffer = &cur_data.buffers()[0];
            buffer.reserve(buffer.len() + cur_buffer.len()).unwrap();
            buffer.write_bytes(cur_buffer.data(), 0).unwrap();
        }

        let builder = builder.add_buffer(buffer.freeze());
        let builder = builder.len(record.num_rows());

        let output_array = make_array(builder.build());

        Ok(output_array)
    }
}

// Coped from Arrow
pub fn make_array(data: ArrayDataRef) -> ArrayRef {
    match data.data_type() {
        DataType::Boolean => Arc::new(BooleanArray::from(data)) as ArrayRef,
        DataType::Int8 => Arc::new(Int8Array::from(data)) as ArrayRef,
        DataType::Int16 => Arc::new(Int16Array::from(data)) as ArrayRef,
        DataType::Int32 => Arc::new(Int32Array::from(data)) as ArrayRef,
        DataType::Int64 => Arc::new(Int64Array::from(data)) as ArrayRef,
        DataType::UInt8 => Arc::new(UInt8Array::from(data)) as ArrayRef,
        DataType::UInt16 => Arc::new(UInt16Array::from(data)) as ArrayRef,
        DataType::UInt32 => Arc::new(UInt32Array::from(data)) as ArrayRef,
        DataType::UInt64 => Arc::new(UInt64Array::from(data)) as ArrayRef,
        DataType::Float16 => panic!("Float16 datatype not supported"),
        DataType::Float32 => Arc::new(Float32Array::from(data)) as ArrayRef,
        DataType::Float64 => Arc::new(Float64Array::from(data)) as ArrayRef,
        DataType::Date32(DateUnit::Day) => Arc::new(Date32Array::from(data)) as ArrayRef,
        DataType::Date64(DateUnit::Millisecond) => {
            Arc::new(Date64Array::from(data)) as ArrayRef
        }
        DataType::Time32(TimeUnit::Second) => {
            Arc::new(Time32SecondArray::from(data)) as ArrayRef
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            Arc::new(Time32MillisecondArray::from(data)) as ArrayRef
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(Time64NanosecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            Arc::new(TimestampSecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Arc::new(TimestampMillisecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Arc::new(TimestampMicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Arc::new(TimestampNanosecondArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            Arc::new(IntervalYearMonthArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Arc::new(IntervalDayTimeArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Second) => {
            Arc::new(DurationSecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            Arc::new(DurationMillisecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Arc::new(DurationMicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Arc::new(DurationNanosecondArray::from(data)) as ArrayRef
        }
        DataType::Binary => Arc::new(BinaryArray::from(data)) as ArrayRef,
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(data)) as ArrayRef,
        DataType::FixedSizeBinary(_) => {
            Arc::new(FixedSizeBinaryArray::from(data)) as ArrayRef
        }
        DataType::Utf8 => Arc::new(StringArray::from(data)) as ArrayRef,
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(data)) as ArrayRef,
        DataType::List(_) => Arc::new(ListArray::from(data)) as ArrayRef,
        DataType::LargeList(_) => Arc::new(LargeListArray::from(data)) as ArrayRef,
        DataType::Struct(_) => Arc::new(StructArray::from(data)) as ArrayRef,
        DataType::Union(_) => Arc::new(UnionArray::from(data)) as ArrayRef,
        DataType::FixedSizeList(_, _) => {
            Arc::new(FixedSizeListArray::from(data)) as ArrayRef
        }
        DataType::Dictionary(ref key_type, _) => match key_type.as_ref() {
            DataType::Int8 => {
                Arc::new(DictionaryArray::<Int8Type>::from(data)) as ArrayRef
            }
            DataType::Int16 => {
                Arc::new(DictionaryArray::<Int16Type>::from(data)) as ArrayRef
            }
            DataType::Int32 => {
                Arc::new(DictionaryArray::<Int32Type>::from(data)) as ArrayRef
            }
            DataType::Int64 => {
                Arc::new(DictionaryArray::<Int64Type>::from(data)) as ArrayRef
            }
            DataType::UInt8 => {
                Arc::new(DictionaryArray::<UInt8Type>::from(data)) as ArrayRef
            }
            DataType::UInt16 => {
                Arc::new(DictionaryArray::<UInt16Type>::from(data)) as ArrayRef
            }
            DataType::UInt32 => {
                Arc::new(DictionaryArray::<UInt32Type>::from(data)) as ArrayRef
            }
            DataType::UInt64 => {
                Arc::new(DictionaryArray::<UInt64Type>::from(data)) as ArrayRef
            }
            dt => panic!("Unexpected dictionary key type {:?}", dt),
        },
        DataType::Null => Arc::new(NullArray::from(data)) as ArrayRef,
        dt => panic!("Unexpected data type {:?}", dt),
    }
}
