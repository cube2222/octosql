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
