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

use arrow::array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;

use crate::physical::physical::{Error, ScalarValue};

/// Enumeration of types that can be used in a GROUP BY expression (all primitives except
/// for floating point numerics)
#[derive(Debug, PartialEq, Eq, Hash, Clone, Ord, PartialOrd)]
pub enum GroupByScalar {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Utf8(String),
}

/// Create a Vec<GroupByScalar> that can be used as a map key
pub fn create_key(
    group_by_keys: &[ArrayRef],
    row: usize,
    vec: &mut Vec<GroupByScalar>,
) -> Result<(), Error> {
    for i in 0..group_by_keys.len() {
        let col = &group_by_keys[i];
        match col.data_type() {
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
            DataType::Utf8 => {
                let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                vec[i] = GroupByScalar::Utf8(String::from(array.value(row)))
            }
            _ => {
                return Err(Error::Unexpected);
            }
        }
    }
    Ok(())
}

pub fn create_row(
    columns: &[ArrayRef],
    row: usize,
    vec: &mut Vec<ScalarValue>,
) -> Result<(), Error> {
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
            DataType::Utf8 => {
                let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                vec[i] = ScalarValue::Utf8(String::from(array.value(row)))
            }
            _ => {
                return Err(Error::Unexpected);
            }
        }
    }
    Ok(())
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
