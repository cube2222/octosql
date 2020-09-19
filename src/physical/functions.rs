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

use arrow::array::{BinaryArray, BooleanArray, Date32Array, Date64Array, DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray, IntervalYearMonthArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, NullArray, StringArray, StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array, UnionArray, PrimitiveArray};
use arrow::array::ArrayRef;
use arrow::compute::kernels::comparison::eq;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, ArrowPrimitiveType};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use crate::physical::map::Expression;
use crate::physical::physical::{Error, ExecutionContext, SchemaContext};
use crate::physical::arrow::*;

// TODO: Add "custom function" expression.

type EvaluateFunction = Arc<dyn Fn(Vec<ArrayRef>) -> Result<ArrayRef, Error> + Send + Sync>;
type MetaFunction = Arc<dyn Fn(&Arc<dyn SchemaContext>, &Arc<Schema>) -> Result<Field, Error> + Send + Sync>;

pub struct FunctionExpression {
    meta_function: MetaFunction,
    evaluate_function: EvaluateFunction,
    args: Vec<Arc<dyn Expression>>,
}

impl FunctionExpression
{
    pub fn new(
        meta_function: MetaFunction,
        evaluate_function: EvaluateFunction,
        args: Vec<Arc<dyn Expression>>,
    ) -> FunctionExpression {
        FunctionExpression {
            meta_function,
            evaluate_function,
            args
        }
    }
}

impl Expression for FunctionExpression
{
    fn field_meta(&self, schema_context: Arc<dyn SchemaContext>, record_schema: &Arc<Schema>) -> Result<Field, Error> {
        (self.meta_function)(&schema_context, record_schema)
    }

    fn evaluate(&self, ctx: &ExecutionContext, record: &RecordBatch) -> Result<ArrayRef, Error> {
        let args = self.args.iter()
            .map(|expr| expr.evaluate(ctx, record))
            .collect::<Result<Vec<ArrayRef>, Error>>()?;

        (self.evaluate_function)(args)
    }
}

macro_rules! make_const_meta_body {
    ($data_type: expr) => {
        Arc::new(|schema_context, record_schema| {
            Ok(Field::new("", $data_type, false))
        })
    }
}

macro_rules! make_binary_primitive_array_evaluate_function {
    ($function: ident) => {
        Arc::new(|args: Vec<ArrayRef>| {
            let output: Result<_, ArrowError> = binary_primitive_array_op!(args[0], args[1], $function);
            Ok(output? as ArrayRef)
        })
    }
}

macro_rules! register_function {
    ($map: expr, $name: expr, $meta_fn: expr, $eval_fn: expr) => {
        $map.insert($name, Arc::new(|args: Vec<Arc<dyn Expression>>| Arc::new(FunctionExpression::new($meta_fn, $eval_fn, args))));
    }
}

lazy_static! {
    pub static ref BUILTIN_FUNCTIONS: HashMap<&'static str, Arc<dyn Fn(Vec<Arc<dyn Expression>>)-> Arc<FunctionExpression> + Send + Sync>> = {
        let mut m: HashMap<&'static str, Arc<dyn Fn(Vec<Arc<dyn Expression>>)-> Arc<FunctionExpression> + Send + Sync>> = HashMap::new();
        register_function!(m, "=", make_const_meta_body!(DataType::Boolean), make_binary_primitive_array_evaluate_function!(eq));
        m
    };
}
