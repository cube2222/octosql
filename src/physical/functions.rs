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

use arrow::array::{BinaryArray, BooleanArray, Date32Array, Date64Array, DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray, IntervalYearMonthArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, NullArray, StringArray, StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array, UnionArray};
use arrow::array::ArrayRef;
use arrow::compute::kernels::comparison::eq;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use crate::physical::map::Expression;
use crate::physical::physical::{Error, ExecutionContext, SchemaContext};
use crate::physical::arrow::*;

// TODO: Add "custom function" expression.

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
