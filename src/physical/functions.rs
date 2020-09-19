use crate::physical::map::Expression;
use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType, TimeUnit};
use arrow::array::{BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array, Float32Array, Float64Array, Date32Array, Date64Array, Time32SecondArray, Time32MillisecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampSecondArray, TimestampMillisecondArray, TimestampMicrosecondArray, TimestampNanosecondArray, IntervalYearMonthArray, IntervalDayTimeArray, DurationSecondArray, DurationMillisecondArray, DurationMicrosecondArray, DurationNanosecondArray, BinaryArray, LargeBinaryArray, FixedSizeBinaryArray, StringArray, LargeStringArray, ListArray, LargeListArray, StructArray, UnionArray, FixedSizeListArray, NullArray, DictionaryArray};
use crate::physical::physical::{Error, ExecutionContext, SchemaContext};
use arrow::compute::kernels::comparison::{eq};
use arrow::record_batch::RecordBatch;
use arrow::array::ArrayRef;
use arrow::error::ArrowError;

// TODO: Add "custom function" expression.

// *** Copied from Datafusion
/// Invoke a compute kernel on a pair of binary data arrays
// macro_rules! compute_utf8_op {
//     ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
//         let ll = $LEFT
//             .as_any()
//             .downcast_ref::<$DT>()
//             .expect("compute_op failed to downcast array");
//         let rr = $RIGHT
//             .as_any()
//             .downcast_ref::<$DT>()
//             .expect("compute_op failed to downcast array");
//         Ok(Arc::new(paste::expr! {[<$OP _utf8>]}(&ll, &rr)?))
//     }};
// }

/// Invoke a compute kernel on a pair of arrays
macro_rules! compute_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
}

macro_rules! binary_string_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Utf8 => compute_utf8_op!($LEFT, $RIGHT, $OP, StringArray),
            other => {
                dbg!(other);
                unimplemented!()
            }
        }
    }};
}

/// Invoke a compute kernel on a pair of arrays
/// The binary_primitive_array_op macro only evaluates for primitive types
/// like integers and floats.
macro_rules! binary_primitive_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Int8 => compute_op!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            other => {
                dbg!(other);
                unimplemented!()
            }
        }
    }};
}

/// The binary_array_op macro includes types that extend beyond the primitive,
/// such as Utf8 strings.
macro_rules! binary_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Int8 => compute_op!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            // DataType::Utf8 => compute_utf8_op!($LEFT, $RIGHT, $OP, StringArray),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                compute_op!($LEFT, $RIGHT, $OP, TimestampNanosecondArray)
            }
            other => {
                dbg!(other);
                unimplemented!()
            }
        }
    }};
}

/// Invoke a boolean kernel on a pair of arrays
macro_rules! boolean_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
}

// *** End copy from Datafusion


pub struct Equal {
    left: Arc<dyn Expression>,
    right: Arc<dyn Expression>,
}

impl Equal {
    pub fn new(left: Arc<dyn Expression>, right: Arc<dyn Expression>) -> Equal {
        Equal { left, right }
    }
}

impl Expression for Equal {
    fn field_meta(
        &self,
        schema_context: Arc<dyn SchemaContext>,
        record_schema: &Arc<Schema>,
    ) -> Result<Field, Error> {
        // TODO: Nullable if either subexpression nullable?
        Ok(Field::new("", DataType::Boolean, false))
    }
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error> {
        let left = self.left.evaluate(ctx, record)?;
        let right = self.right.evaluate(ctx, record)?;

        let output: Result<_, ArrowError> = binary_array_op!(left, right, eq);
        Ok(output?)
    }
}
