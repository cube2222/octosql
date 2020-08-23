
// ****** Copied from datafusion

use arrow::array::{ArrayRef, UInt8Array, UInt16Array, UInt32Array, UInt64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray, BooleanArray};
use crate::execution::execution::{Error, ScalarValue};
use arrow::datatypes::DataType;

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
