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
use std::collections::HashMap;

use arrow::array::{ArrayRef, PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder, Array};
use arrow::datatypes::{Field, Schema, DataType, TimeUnit, DateUnit, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type, IntervalUnit, BooleanType, DurationNanosecondType, TimestampNanosecondType, Float32Type, Float64Type, Date32Type, Date64Type, Time32SecondType, Time32MillisecondType, Time64MicrosecondType, Time64NanosecondType, TimestampSecondType, TimestampMillisecondType, TimestampMicrosecondType, IntervalYearMonthType, IntervalDayTimeType, DurationSecondType, DurationMillisecondType, DurationMicrosecondType};
use arrow::compute::kernels::comparison::{lt, lt_eq, eq, gt_eq, gt, lt_utf8, lt_eq_utf8, eq_utf8, gt_eq_utf8, gt_utf8};
use arrow::compute::kernels::arithmetic;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use anyhow::Result;

use crate::physical::expression::Expression;
use crate::physical::physical::{ExecutionContext};

use chrono::{DateTime};
use arrow::datatypes::TimeUnit::Nanosecond;

// TODO: Add "custom function" expression.

type EvaluateFunction = Arc<dyn Fn(Vec<ArrayRef>) -> Result<ArrayRef> + Send + Sync>;
type MetaFunction = Arc<dyn Fn(Vec<Field>) -> Result<Field> + Send + Sync>;

pub struct FunctionExpression {
    evaluate_function: EvaluateFunction,
    args: Vec<Arc<dyn Expression>>,
}

impl FunctionExpression
{
    pub fn new(
        evaluate_function: EvaluateFunction,
        args: Vec<Arc<dyn Expression>>,
    ) -> FunctionExpression {
        FunctionExpression {
            evaluate_function,
            args,
        }
    }
}

impl Expression for FunctionExpression
{
    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef> {
        let args = self.args.iter()
            .map(|expr| expr.evaluate(ctx, record))
            .collect::<Result<Vec<ArrayRef>>>()?;

        (self.evaluate_function)(args)
    }
}

macro_rules! make_const_meta_body {
    ($data_type: expr) => {
        Arc::new(|_args| {
            Ok(Field::new("", $data_type, false))
        })
    }
}

// TODO: Fixme check if args[0] is actually a numeric type.
macro_rules! make_numeric_meta_body {
    () => {
        Arc::new(|args| {
            if args[0].data_type() == args[1].data_type() {
                Ok(Field::new("", args[0].data_type().clone(), true))
            } else {
                Err(anyhow!("Both arguments of a numeric operator must be of the same type."))
            }
        })
    }
}

// macro_rules! make_binary_primitive_array_evaluate_function {
//     ($function: ident) => {
//         Arc::new(|args: Vec<ArrayRef>| {
//             let output: Result<_, ArrowError> = binary_primitive_array_op!(args[0], args[1], $function);
//             Ok(output? as ArrayRef)
//         })
//     }
// }

macro_rules! make_binary_array_evaluate_function {
    ($function: ident) => {
        Arc::new(|args: Vec<ArrayRef>| {
            let output: Result<_, ArrowError> = binary_array_op!(args[0], args[1], $function);
            Ok(output? as ArrayRef)
        })
    }
}

macro_rules! make_binary_numeric_array_evaluate_function {
    ($function: ident) => {
        Arc::new(|args: Vec<ArrayRef>| {
            let output: Result<_, ArrowError> = binary_numeric_array_op!(args[0], args[1], $function);
            Ok(output? as ArrayRef)
        })
    }
}

// macro_rules! make_single_arg_evaluate_function {
//     ($function: ident) => {
//         Arc::new(|args: Vec<ArrayRef>| {
//             let output: Result<_, ArrowError> = binary_primitive_array_op!(args[0], args[1], $function);
//             Ok(output? as ArrayRef)
//         })
//     }
// }

fn less_than(args: Vec<Field>) -> Result<(Field, EvaluateFunction)> {
    let eval_fn = match (args[0].data_type(), args[1].data_type()) {
        (DataType::UInt8, DataType::UInt8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<Int8Type>),
        (DataType::UInt16, DataType::UInt16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<UInt16Type>),
        (DataType::UInt32, DataType::UInt32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<UInt32Type>),
        (DataType::UInt64, DataType::UInt64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<UInt64Type>),
        (DataType::Int8, DataType::Int8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<Int8Type>),
        (DataType::Int16, DataType::Int16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<Int16Type>),
        (DataType::Int32, DataType::Int32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<Int32Type>),
        (DataType::Int64, DataType::Int64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<Int64Type>),
        (DataType::Float32, DataType::Float32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<Float32Type>),
        (DataType::Float64, DataType::Float64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<Float64Type>),
        (DataType::Duration(TimeUnit::Nanosecond), DataType::Duration(TimeUnit::Nanosecond)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<DurationNanosecondType>),
        (DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Timestamp(TimeUnit::Nanosecond, None)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt, PrimitiveArray<TimestampNanosecondType>),
        (DataType::Utf8, DataType::Utf8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_utf8, StringArray),
        _ => return Err(anyhow!("Invalid comparison operator argument types."))
    };

    Ok((Field::new("", DataType::Boolean, false), Arc::new(eval_fn)))
}

fn less_than_equal(args: Vec<Field>) -> Result<(Field, EvaluateFunction)> {
    let eval_fn = match (args[0].data_type(), args[1].data_type()) {
        (DataType::UInt8, DataType::UInt8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<Int8Type>),
        (DataType::UInt16, DataType::UInt16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<UInt16Type>),
        (DataType::UInt32, DataType::UInt32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<UInt32Type>),
        (DataType::UInt64, DataType::UInt64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<UInt64Type>),
        (DataType::Int8, DataType::Int8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<Int8Type>),
        (DataType::Int16, DataType::Int16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<Int16Type>),
        (DataType::Int32, DataType::Int32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<Int32Type>),
        (DataType::Int64, DataType::Int64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<Int64Type>),
        (DataType::Float32, DataType::Float32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<Float32Type>),
        (DataType::Float64, DataType::Float64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<Float64Type>),
        (DataType::Duration(TimeUnit::Nanosecond), DataType::Duration(TimeUnit::Nanosecond)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<DurationNanosecondType>),
        (DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Timestamp(TimeUnit::Nanosecond, None)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq, PrimitiveArray<TimestampNanosecondType>),
        (DataType::Utf8, DataType::Utf8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], lt_eq_utf8, StringArray),
        _ => return Err(anyhow!("Invalid comparison operator argument types."))
    };

    Ok((Field::new("", DataType::Boolean, false), Arc::new(eval_fn)))
}

fn equal(args: Vec<Field>) -> Result<(Field, EvaluateFunction)> {
    let eval_fn = match (args[0].data_type(), args[1].data_type()) {
        (DataType::UInt8, DataType::UInt8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<Int8Type>),
        (DataType::UInt16, DataType::UInt16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<UInt16Type>),
        (DataType::UInt32, DataType::UInt32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<UInt32Type>),
        (DataType::UInt64, DataType::UInt64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<UInt64Type>),
        (DataType::Int8, DataType::Int8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<Int8Type>),
        (DataType::Int16, DataType::Int16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<Int16Type>),
        (DataType::Int32, DataType::Int32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<Int32Type>),
        (DataType::Int64, DataType::Int64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<Int64Type>),
        (DataType::Float32, DataType::Float32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<Float32Type>),
        (DataType::Float64, DataType::Float64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<Float64Type>),
        (DataType::Duration(TimeUnit::Nanosecond), DataType::Duration(TimeUnit::Nanosecond)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<DurationNanosecondType>),
        (DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Timestamp(TimeUnit::Nanosecond, None)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq, PrimitiveArray<TimestampNanosecondType>),
        (DataType::Utf8, DataType::Utf8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], eq_utf8, StringArray),
        _ => return Err(anyhow!("Invalid comparison operator argument types."))
    };

    Ok((Field::new("", DataType::Boolean, false), Arc::new(eval_fn)))
}

fn greater_than_equal(args: Vec<Field>) -> Result<(Field, EvaluateFunction)> {
    let eval_fn = match (args[0].data_type(), args[1].data_type()) {
        (DataType::UInt8, DataType::UInt8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<Int8Type>),
        (DataType::UInt16, DataType::UInt16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<UInt16Type>),
        (DataType::UInt32, DataType::UInt32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<UInt32Type>),
        (DataType::UInt64, DataType::UInt64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<UInt64Type>),
        (DataType::Int8, DataType::Int8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<Int8Type>),
        (DataType::Int16, DataType::Int16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<Int16Type>),
        (DataType::Int32, DataType::Int32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<Int32Type>),
        (DataType::Int64, DataType::Int64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<Int64Type>),
        (DataType::Float32, DataType::Float32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<Float32Type>),
        (DataType::Float64, DataType::Float64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<Float64Type>),
        (DataType::Duration(TimeUnit::Nanosecond), DataType::Duration(TimeUnit::Nanosecond)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<DurationNanosecondType>),
        (DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Timestamp(TimeUnit::Nanosecond, None)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq, PrimitiveArray<TimestampNanosecondType>),
        (DataType::Utf8, DataType::Utf8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_eq_utf8, StringArray),
        _ => return Err(anyhow!("Invalid comparison operator argument types."))
    };

    Ok((Field::new("", DataType::Boolean, false), Arc::new(eval_fn)))
}

fn greater_than(args: Vec<Field>) -> Result<(Field, EvaluateFunction)> {
    let eval_fn = match (args[0].data_type(), args[1].data_type()) {
        (DataType::UInt8, DataType::UInt8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<Int8Type>),
        (DataType::UInt16, DataType::UInt16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<UInt16Type>),
        (DataType::UInt32, DataType::UInt32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<UInt32Type>),
        (DataType::UInt64, DataType::UInt64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<UInt64Type>),
        (DataType::Int8, DataType::Int8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<Int8Type>),
        (DataType::Int16, DataType::Int16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<Int16Type>),
        (DataType::Int32, DataType::Int32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<Int32Type>),
        (DataType::Int64, DataType::Int64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<Int64Type>),
        (DataType::Float32, DataType::Float32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<Float32Type>),
        (DataType::Float64, DataType::Float64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<Float64Type>),
        (DataType::Duration(TimeUnit::Nanosecond), DataType::Duration(TimeUnit::Nanosecond)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<DurationNanosecondType>),
        (DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Timestamp(TimeUnit::Nanosecond, None)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt, PrimitiveArray<TimestampNanosecondType>),
        (DataType::Utf8, DataType::Utf8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_utf8, StringArray),
        _ => return Err(anyhow!("Invalid comparison operator argument types."))
    };

    Ok((Field::new("", DataType::Boolean, false), Arc::new(eval_fn)))
}

fn any_nullable(args: &[Field]) -> bool {
    return args.iter()
        .any(|arg| arg.is_nullable());
}

fn add(args: Vec<Field>) -> Result<(Field, EvaluateFunction)> {
    let eval_fn = match (args[0].data_type(), args[1].data_type()) {
        (DataType::UInt8, DataType::UInt8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<Int8Type>),
        (DataType::UInt16, DataType::UInt16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<UInt16Type>),
        (DataType::UInt32, DataType::UInt32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<UInt32Type>),
        (DataType::UInt64, DataType::UInt64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<UInt64Type>),
        (DataType::Int8, DataType::Int8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<Int8Type>),
        (DataType::Int16, DataType::Int16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<Int16Type>),
        (DataType::Int32, DataType::Int32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<Int32Type>),
        (DataType::Int64, DataType::Int64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<Int64Type>),
        (DataType::Float32, DataType::Float32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<Float32Type>),
        (DataType::Float64, DataType::Float64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<Float64Type>),
        (DataType::Duration(TimeUnit::Nanosecond), DataType::Duration(TimeUnit::Nanosecond)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<DurationNanosecondType>),
        (DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Timestamp(TimeUnit::Nanosecond, None)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::add, PrimitiveArray<TimestampNanosecondType>),
        // (DataType::Utf8, DataType::Utf8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_utf8, StringArray),
        _ => return Err(anyhow!("Invalid comparison operator argument types."))
    };

    Ok((Field::new("", args[0].data_type().clone(), any_nullable(args.as_ref())), Arc::new(eval_fn)))
}

fn subtract(args: Vec<Field>) -> Result<(Field, EvaluateFunction)> {
    let eval_fn = match (args[0].data_type(), args[1].data_type()) {
        (DataType::UInt8, DataType::UInt8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<Int8Type>),
        (DataType::UInt16, DataType::UInt16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<UInt16Type>),
        (DataType::UInt32, DataType::UInt32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<UInt32Type>),
        (DataType::UInt64, DataType::UInt64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<UInt64Type>),
        (DataType::Int8, DataType::Int8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<Int8Type>),
        (DataType::Int16, DataType::Int16) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<Int16Type>),
        (DataType::Int32, DataType::Int32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<Int32Type>),
        (DataType::Int64, DataType::Int64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<Int64Type>),
        (DataType::Float32, DataType::Float32) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<Float32Type>),
        (DataType::Float64, DataType::Float64) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<Float64Type>),
        (DataType::Duration(TimeUnit::Nanosecond), DataType::Duration(TimeUnit::Nanosecond)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<DurationNanosecondType>),
        (DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Timestamp(TimeUnit::Nanosecond, None)) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], arithmetic::subtract, PrimitiveArray<TimestampNanosecondType>),
        // (DataType::Utf8, DataType::Utf8) => |args: Vec<ArrayRef>| compute_op!(args[0], args[1], gt_utf8, StringArray),
        _ => return Err(anyhow!("Invalid comparison operator argument types."))
    };

    Ok((Field::new("", args[0].data_type().clone(), any_nullable(args.as_ref())), Arc::new(eval_fn)))
}

macro_rules! register_function {
    ($map: expr, $name: expr, $meta_fn: expr) => {
        $map.insert($name, (Arc::new(|args: Vec<Field>| {
            let (out_meta, _) = ($meta_fn)(args)?;
            Ok(out_meta)
        }), Arc::new(|arg_types: Vec<Field>, args: Vec<Arc<dyn Expression>>| {
            let (_, eval_fn) = ($meta_fn)(arg_types).unwrap();
            Arc::new(FunctionExpression::new(eval_fn, args))
        })));
    }
}

lazy_static! {
    pub static ref BUILTIN_FUNCTIONS: HashMap<&'static str, (MetaFunction, Arc<dyn Fn(Vec<Field>, Vec<Arc<dyn Expression>>)-> Arc<FunctionExpression> + Send + Sync>)> = {
        let mut m: HashMap<&'static str, (MetaFunction, Arc<dyn Fn(Vec<Field>, Vec<Arc<dyn Expression>>)-> Arc<FunctionExpression> + Send + Sync>)> = HashMap::new();
        register_function!(m, "<", less_than);
        register_function!(m, "<=", less_than_equal);
        register_function!(m, "=", equal);
        register_function!(m, ">=", greater_than_equal);
        register_function!(m, ">", greater_than);
        register_function!(m, "+", add);
        register_function!(m, "-", subtract);
        // register_function!(m, "+", make_numeric_meta_body!(), make_binary_numeric_array_evaluate_function!(add));
        // register_function!(m, "-", make_numeric_meta_body!(), make_binary_numeric_array_evaluate_function!(subtract));
        // register_function!(m, "*", Arc::new(|args| {
        //     Ok(Field::new("", match (args[0].data_type(), args[1].data_type()) {
        //         (DataType::Int64, DataType::Int64) => DataType::Int64,
        //         (DataType::UInt8, DataType::UInt8) => DataType::UInt8,
        //         (DataType::UInt16, DataType::UInt16) => DataType::UInt16,
        //         (DataType::UInt32, DataType::UInt32) => DataType::UInt32,
        //         (DataType::UInt64, DataType::UInt64) => DataType::UInt64,
        //         (DataType::Int8, DataType::Int8) => DataType::Int8,
        //         (DataType::Int16, DataType::Int16) => DataType::Int16,
        //         (DataType::Int32, DataType::Int32) => DataType::Int32,
        //         (DataType::Int64, DataType::Int64) => DataType::Int64,
        //         (DataType::Duration, DataType::Duration) => DataType::Duration(TimeUnit::Nanosecond),
        //         (DataType::Timestamp, DataType::Timestamp) => DataType::Timestamp(TimeUnit::Nanosecond),
        //         (DataType::Float32, DataType::Float32) => DataType::Float32,
        //         (DataType::Float64, DataType::Float64) => DataType::Float64,
        //         _ => return Err(anyhow!("Invalid numeric operator argument types."))
        //     }, true)) // TODO: Fixme nullability
        // }), Arc::new(|args: Vec<ArrayRef>| {
        //     let output: Result<_, ArrowError> = binary_numeric_array_op!(args[0], args[1], multiply);
        //     Ok(output? as ArrayRef)
        // }));
        // register_function!(m, "/", make_numeric_meta_body!(), make_binary_numeric_array_evaluate_function!(divide));
        // register_function!(m, "upper", make_const_meta_body!(DataType::Utf8), Arc::new(|args: Vec<ArrayRef>| {
        //     let output: Result<_, ArrowError> = compute_single_arg_str!(args[0], StringArray, StringBuilder, |text: &str| {
        //         text.to_uppercase()
        //     });
        //     Ok(output? as ArrayRef)
        // }));
        // register_function!(m, "parse_datetime_rfc3339", make_const_meta_body!(DataType::Timestamp(Nanosecond, None)), Arc::new(|args: Vec<ArrayRef>| {
        //     let output: Result<_> = compute_single_arg!(args[0], StringArray, PrimitiveBuilder<TimestampNanosecondType>, |text: &str| -> Result<i64> {
        //         match DateTime::parse_from_rfc3339(text) {
        //             Ok(dt) => Ok(dt.timestamp_nanos()),
        //             Err(err) => Err(err)?,
        //         }
        //     });
        //     Ok(output? as ArrayRef)
        // }));
        // register_function!(m, "parse_datetime_tz", make_const_meta_body!(DataType::Timestamp(Nanosecond, None)), Arc::new(|args: Vec<ArrayRef>| {
        //     let output: Result<_> = compute_two_arg!(args[0], args[1], StringArray, StringArray, PrimitiveBuilder<TimestampNanosecondType>, |fmt: &str, text: &str| -> Result<i64> {
        //         match DateTime::parse_from_str(text, fmt) {
        //             Ok(dt) => Ok(dt.timestamp_nanos()),
        //             Err(err) => Err(err)?
        //         }
        //     });
        //     Ok(output? as ArrayRef)
        // }));
        m
    };
}
