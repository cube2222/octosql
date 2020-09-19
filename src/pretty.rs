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

//! Utilities for printing record batches

use std::time::{SystemTime, UNIX_EPOCH, Duration};

use arrow::array;
use arrow::datatypes::{DataType, TimeUnit, ArrowPrimitiveType, Int64Type};
use arrow::record_batch::RecordBatch;
use arrow::error::{Result, ArrowError};

use prettytable::format;
use prettytable::{Cell, Row, Table};
use arrow::array::{Int64Array, PrimitiveArrayOps, PrimitiveArray};
use chrono::{Utc, DateTime, NaiveDateTime, Local};
use chrono::format::Fixed::RFC3339;

///! Create a visual representation of record batches
pub fn pretty_format_batches(results: &[RecordBatch]) -> Result<String> {
    Ok(create_table(results)?.to_string())
}

///! Prints a visual representation of record batches to stdout
pub fn print_batches(results: &[RecordBatch]) -> Result<()> {
    create_table(results)?.printstd();
    Ok(())
}

///! Convert a series of record batches into a table
fn create_table(results: &[RecordBatch]) -> Result<Table> {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    if results.is_empty() {
        return Ok(table);
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(&field.name()));
    }
    table.set_titles(Row::new(header));

    for batch in results {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for col in 0..batch.num_columns() {
                let column = batch.column(col);
                cells.push(Cell::new(&array_value_to_string(column.clone(), row)?));
            }
            table.add_row(Row::new(cells));
        }
    }

    Ok(table)
}

macro_rules! make_string {
    ($array_type:ty, $column: ident, $row: ident) => {{
        if $column.is_null($row) {
            Ok("<null>".to_string())
        } else {
            Ok($column
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap()
                .value($row)
                .to_string())
        }
    }};
}

/// Get the value at the given row in an array as a string
fn array_value_to_string(column: array::ArrayRef, row: usize) -> Result<String> {
    match column.data_type() {
        DataType::Utf8 => Ok(column
            .as_any()
            .downcast_ref::<array::StringArray>()
            .unwrap()
            .value(row)
            .to_string()),
        DataType::Boolean => make_string!(array::BooleanArray, column, row),
        DataType::Int16 => make_string!(array::Int16Array, column, row),
        DataType::Int32 => make_string!(array::Int32Array, column, row),
        DataType::Int64 => make_string!(array::Int64Array, column, row),
        DataType::UInt8 => make_string!(array::UInt8Array, column, row),
        DataType::UInt16 => make_string!(array::UInt16Array, column, row),
        DataType::UInt32 => make_string!(array::UInt32Array, column, row),
        DataType::UInt64 => make_string!(array::UInt64Array, column, row),
        DataType::Float16 => make_string!(array::Float32Array, column, row),
        DataType::Float32 => make_string!(array::Float32Array, column, row),
        DataType::Float64 => make_string!(array::Float64Array, column, row),
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Second => {
            make_string!(array::TimestampSecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Millisecond => {
            make_string!(array::TimestampMillisecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Microsecond => {
            make_string!(array::TimestampMicrosecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Nanosecond => {
            if column.is_null(row) {
                Ok("<null>".to_string())
            } else {
                let nano_duration = UNIX_EPOCH + Duration::from_nanos(column
                    .as_any()
                    .downcast_ref::<array::TimestampNanosecondArray>()
                    .unwrap()
                    .value(row) as u64);
                let datetime = DateTime::<Utc>::from(nano_duration);
                Ok(datetime.to_rfc3339())
            }
        }
        DataType::Date32(_) => make_string!(array::Date32Array, column, row),
        DataType::Date64(_) => make_string!(array::Date64Array, column, row),
        DataType::Time32(unit) if *unit == TimeUnit::Second => {
            make_string!(array::Time32SecondArray, column, row)
        }
        DataType::Time32(unit) if *unit == TimeUnit::Millisecond => {
            make_string!(array::Time32MillisecondArray, column, row)
        }
        DataType::Time32(unit) if *unit == TimeUnit::Microsecond => {
            make_string!(array::Time64MicrosecondArray, column, row)
        }
        DataType::Time64(unit) if *unit == TimeUnit::Nanosecond => {
            make_string!(array::Time64NanosecondArray, column, row)
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Unsupported {:?} type for repl.",
            column.data_type()
        ))),
    }
}