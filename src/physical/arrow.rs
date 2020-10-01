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

// Copyright 2016-2019 The Apache Software Foundation
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

use arrow::array;
use arrow::array::{BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array, Float32Array, Float64Array, Date32Array, Date64Array, Time32SecondArray, Time32MillisecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampSecondArray, TimestampMillisecondArray, TimestampMicrosecondArray, TimestampNanosecondArray, IntervalYearMonthArray, IntervalDayTimeArray, DurationSecondArray, DurationMillisecondArray, DurationMicrosecondArray, DurationNanosecondArray, BinaryArray, LargeBinaryArray, FixedSizeBinaryArray, StringArray, LargeStringArray, ListArray, LargeListArray, StructArray, UnionArray, FixedSizeListArray, NullArray, DictionaryArray, ArrayRef, ArrayDataRef};
use arrow::datatypes::{DataType, TimeUnit, DateUnit, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type, IntervalUnit};
use anyhow::Result;

use crate::physical::physical::ScalarValue;

/// Enumeration of types that can be used in a GROUP BY expression (all primitives except
/// for floating point numerics)
#[derive(Debug, PartialEq, Eq, Hash, Clone, Ord, PartialOrd)]
pub enum GroupByScalar {
    Boolean(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Timestamp(i64),
    Utf8(String),
}

/// Create a Vec<GroupByScalar> that can be used as a map key
pub fn create_key(
    group_by_keys: &[ArrayRef],
    row: usize,
    vec: &mut Vec<GroupByScalar>,
) -> Result<()> {
    for i in 0..group_by_keys.len() {
        let col = &group_by_keys[i];
        match col.data_type() {
            DataType::Boolean => {
                let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                vec[i] = GroupByScalar::Boolean(array.value(row))
            }
            DataType::UInt8 => {
                let array = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                vec[i] = GroupByScalar::UInt8(array.value(row))
            }
            DataType::UInt16 => {
                let array = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                vec[i] = GroupByScalar::UInt16(array.value(row))
            }
            DataType::UInt32 => {
                let array = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                vec[i] = GroupByScalar::UInt32(array.value(row))
            }
            DataType::UInt64 => {
                let array = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                vec[i] = GroupByScalar::UInt64(array.value(row))
            }
            DataType::Int8 => {
                let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
                vec[i] = GroupByScalar::Int8(array.value(row))
            }
            DataType::Int16 => {
                let array = col.as_any().downcast_ref::<Int16Array>().unwrap();
                vec[i] = GroupByScalar::Int16(array.value(row))
            }
            DataType::Int32 => {
                let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                vec[i] = GroupByScalar::Int32(array.value(row))
            }
            DataType::Int64 => {
                let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                vec[i] = GroupByScalar::Int64(array.value(row))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let array = col.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                vec[i] = GroupByScalar::Timestamp(array.value(row))
            }
            DataType::Utf8 => {
                let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                vec[i] = GroupByScalar::Utf8(String::from(array.value(row)))
            }
            _ => {
                unimplemented!()
            }
        }
    }
    Ok(())
}

pub fn create_row(
    columns: &[ArrayRef],
    row: usize,
    vec: &mut Vec<ScalarValue>,
) -> Result<()> {
    for i in 0..columns.len() {
        let col = &columns[i];
        match col.data_type() {
            DataType::Boolean => {
                let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                vec[i] = ScalarValue::Boolean(array.value(row))
            }
            DataType::UInt8 => {
                let array = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                vec[i] = ScalarValue::UInt8(array.value(row))
            }
            DataType::UInt16 => {
                let array = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                vec[i] = ScalarValue::UInt16(array.value(row))
            }
            DataType::UInt32 => {
                let array = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                vec[i] = ScalarValue::UInt32(array.value(row))
            }
            DataType::UInt64 => {
                let array = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                vec[i] = ScalarValue::UInt64(array.value(row))
            }
            DataType::Int8 => {
                let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
                vec[i] = ScalarValue::Int8(array.value(row))
            }
            DataType::Int16 => {
                let array = col.as_any().downcast_ref::<Int16Array>().unwrap();
                vec[i] = ScalarValue::Int16(array.value(row))
            }
            DataType::Int32 => {
                let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                vec[i] = ScalarValue::Int32(array.value(row))
            }
            DataType::Int64 => {
                let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                vec[i] = ScalarValue::Int64(array.value(row))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let array = col.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                vec[i] = ScalarValue::Timestamp(array.value(row))
            }
            DataType::Float32 => {
                let array = col.as_any().downcast_ref::<Float32Array>().unwrap();
                vec[i] = ScalarValue::Float32(array.value(row))
            }
            DataType::Float64 => {
                let array = col.as_any().downcast_ref::<Float64Array>().unwrap();
                vec[i] = ScalarValue::Float64(array.value(row))
            }
            DataType::Utf8 => {
                let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                vec[i] = ScalarValue::Utf8(String::from(array.value(row)))
            }
            _ => {
                dbg!(col.data_type());
                unimplemented!()
            }
        }
    }
    Ok(())
}

/// Get a value from an array as a ScalarValue
pub fn get_scalar_value(array: &ArrayRef, row: usize) -> Result<ScalarValue> {
    if array.is_null(row) {
        return Ok(ScalarValue::Null);
    }
    let value: ScalarValue = match array.data_type() {
        DataType::Boolean => {
            let array = array
                .as_any()
                .downcast_ref::<array::BooleanArray>()
                .expect("Failed to cast array");
            ScalarValue::Boolean(array.value(row))
        }
        DataType::UInt8 => {
            let array = array
                .as_any()
                .downcast_ref::<array::UInt8Array>()
                .expect("Failed to cast array");
            ScalarValue::UInt8(array.value(row))
        }
        DataType::UInt16 => {
            let array = array
                .as_any()
                .downcast_ref::<array::UInt16Array>()
                .expect("Failed to cast array");
            ScalarValue::UInt16(array.value(row))
        }
        DataType::UInt32 => {
            let array = array
                .as_any()
                .downcast_ref::<array::UInt32Array>()
                .expect("Failed to cast array");
            ScalarValue::UInt32(array.value(row))
        }
        DataType::UInt64 => {
            let array = array
                .as_any()
                .downcast_ref::<array::UInt64Array>()
                .expect("Failed to cast array");
            ScalarValue::UInt64(array.value(row))
        }
        DataType::Int8 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Int8Array>()
                .expect("Failed to cast array");
            ScalarValue::Int8(array.value(row))
        }
        DataType::Int16 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Int16Array>()
                .expect("Failed to cast array");
            ScalarValue::Int16(array.value(row))
        }
        DataType::Int32 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Int32Array>()
                .expect("Failed to cast array");
            ScalarValue::Int32(array.value(row))
        }
        DataType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Int64Array>()
                .expect("Failed to cast array");
            ScalarValue::Int64(array.value(row))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<array::TimestampNanosecondArray>()
                .unwrap();
            ScalarValue::Timestamp(array.value(row))
        }
        DataType::Float32 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Float32Array>()
                .unwrap();
            ScalarValue::Float32(array.value(row))
        }
        DataType::Float64 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Float64Array>()
                .unwrap();
            ScalarValue::Float64(array.value(row))
        }
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<array::StringArray>()
                .unwrap();
            ScalarValue::Utf8(array.value(row).to_string())
        }
        DataType::Struct(_fields) => {
            let array = array
                .as_any()
                .downcast_ref::<array::StructArray>()
                .unwrap();
            ScalarValue::Struct(array.columns().iter()
                .cloned()
                .map(|col| get_scalar_value(col, row))
                .collect::<Result<Vec<_>, _>>()?)
        }
        other => {
            return Err(anyhow!("Unsupported data type {:?} for result of aggregate expression", other));
        }
    };
    Ok(value)
}

macro_rules! compute_single_arg {
    ($arg:expr, $input_type:ident, $output_builder:ident, $op:expr) => {{
        let arg = $arg
            .as_any()
            .downcast_ref::<$input_type>()
            .expect("compute_single_arg failed to downcast array");

        let mut result = $output_builder::new($arg.len());
        for i in 0..$arg.len() {
            result.append_value($op(arg.value(i))?)?;
        }

        Ok(Arc::new(result.finish()) as ArrayRef)
    }};
}

macro_rules! compute_single_arg_str {
    ($arg:expr, $input_type:ident, $output_builder:ident, $op:expr) => {{
        let arg = $arg
            .as_any()
            .downcast_ref::<$input_type>()
            .expect("compute_single_arg failed to downcast array");

        let mut result = $output_builder::new($arg.len());
        for i in 0..$arg.len() {
            result.append_value($op(arg.value(i)).as_str())?;
        }

        Ok(Arc::new(result.finish()) as ArrayRef)
    }};
}

macro_rules! compute_two_arg {
    ($arg1:expr, $arg2:expr, $input_type1:ident, $input_type2:ident, $output_builder:ident, $op:expr) => {{
        let arg1 = $arg1
            .as_any()
            .downcast_ref::<$input_type1>()
            .expect("compute_single_arg failed to downcast array");

        let arg2 = $arg2
            .as_any()
            .downcast_ref::<$input_type2>()
            .expect("compute_single_arg failed to downcast array");

        let mut result = $output_builder::new($arg1.len());
        for i in 0..$arg1.len() {
            result.append_value($op(arg1.value(i), arg2.value(i))?)?;
        }

        Ok(Arc::new(result.finish()) as ArrayRef)
    }};
}

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

/// Invoke a compute kernel on a pair of binary data arrays
macro_rules! compute_utf8_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new(paste::expr! {[<$OP _utf8>]}(&ll, &rr)?))
    }};
}

// macro_rules! binary_string_array_op {
//     ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
//         match $LEFT.data_type() {
//             DataType::Utf8 => compute_utf8_op!($LEFT, $RIGHT, $OP, StringArray),
//             other => {
//                 dbg!(other);
//                 unimplemented!()
//             }
//         }
//     }};
// }

// /// Invoke a compute kernel on a pair of arrays
// /// The binary_primitive_array_op macro only evaluates for primitive types
// /// like integers and floats.
// macro_rules! binary_primitive_array_op {
//     ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
//         match $LEFT.data_type() {
//             DataType::Int8 => compute_op!($LEFT, $RIGHT, $OP, Int8Array),
//             DataType::Int16 => compute_op!($LEFT, $RIGHT, $OP, Int16Array),
//             DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
//             DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
//             DataType::UInt8 => compute_op!($LEFT, $RIGHT, $OP, UInt8Array),
//             DataType::UInt16 => compute_op!($LEFT, $RIGHT, $OP, UInt16Array),
//             DataType::UInt32 => compute_op!($LEFT, $RIGHT, $OP, UInt32Array),
//             DataType::UInt64 => compute_op!($LEFT, $RIGHT, $OP, UInt64Array),
//             DataType::Float32 => compute_op!($LEFT, $RIGHT, $OP, Float32Array),
//             DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
//             other => {
//                 dbg!(other);
//                 unimplemented!()
//             }
//         }
//     }};
// }

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
            DataType::Utf8 => compute_utf8_op!($LEFT, $RIGHT, $OP, StringArray),
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

// /// Invoke a boolean kernel on a pair of arrays
// macro_rules! boolean_op {
//     ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
//         let ll = $LEFT
//             .as_any()
//             .downcast_ref::<BooleanArray>()
//             .expect("boolean_op failed to downcast array");
//         let rr = $RIGHT
//             .as_any()
//             .downcast_ref::<BooleanArray>()
//             .expect("boolean_op failed to downcast array");
//         Ok(Arc::new($OP(&ll, &rr)?))
//     }};
// }

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
