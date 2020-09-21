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

use arrow::datatypes::DataType;
use nom::lib::std::ops::{AddAssign, SubAssign};

use crate::physical::physical::{Error, ScalarValue};

pub trait Aggregate: Send + Sync {
    fn output_type(&self, input_schema: &DataType) -> Result<DataType, Error>;
    fn create_accumulator(&self, input_type: &DataType) -> Box<dyn Accumulator>;
}

pub trait Accumulator: std::fmt::Debug {
    fn add(&mut self, value: ScalarValue, retract: ScalarValue) -> bool;
    fn trigger(&self) -> ScalarValue;
}

pub struct Sum {}

impl Aggregate for Sum {
    fn output_type(&self, input_type: &DataType) -> Result<DataType, Error> {
        match input_type {
            DataType::Int8 => Ok(DataType::Int8),
            DataType::Int16 => Ok(DataType::Int16),
            DataType::Int32 => Ok(DataType::Int32),
            DataType::Int64 => Ok(DataType::Int64),
            DataType::UInt8 => Ok(DataType::UInt8),
            DataType::UInt16 => Ok(DataType::UInt16),
            DataType::UInt32 => Ok(DataType::UInt32),
            DataType::UInt64 => Ok(DataType::UInt64),
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            _ => {
                dbg!(input_type);
                unimplemented!()
            }
        }
    }

    fn create_accumulator(&self, input_type: &DataType) -> Box<dyn Accumulator> {
        match input_type {
            DataType::Int8 => Box::new(SumAccumulator::<i8> { sum: 0, count: 0 }),
            DataType::Int16 => Box::new(SumAccumulator::<i16> { sum: 0, count: 0 }),
            DataType::Int32 => Box::new(SumAccumulator::<i32> { sum: 0, count: 0 }),
            DataType::Int64 => Box::new(SumAccumulator::<i64> { sum: 0, count: 0 }),
            DataType::UInt8 => Box::new(SumAccumulator::<u8> { sum: 0, count: 0 }),
            DataType::UInt16 => Box::new(SumAccumulator::<u16> { sum: 0, count: 0 }),
            DataType::UInt32 => Box::new(SumAccumulator::<u32> { sum: 0, count: 0 }),
            DataType::UInt64 => Box::new(SumAccumulator::<u64> { sum: 0, count: 0 }),
            DataType::Float32 => Box::new(SumAccumulator::<f32> { sum: 0.0, count: 0 }),
            DataType::Float64 => Box::new(SumAccumulator::<f64> { sum: 0.0, count: 0 }),
            _ => {
                dbg!(input_type);
                unimplemented!()
            }
        }
    }
}

#[derive(Debug)]
struct SumAccumulator<T: AddAssign<T> + SubAssign<T>> {
    sum: T,
    count: i64,
}

macro_rules! impl_sum_accumulator {
    ($primitive_type: ident, $scalar_value_type: ident) => {
        impl Accumulator for SumAccumulator<$primitive_type> {
            fn add(&mut self, value: ScalarValue, retract: ScalarValue) -> bool {
                let is_retraction = match retract {
                    ScalarValue::Boolean(x) => x,
                    _ => panic!("retraction shall be boolean"),
                };
                if !is_retraction {
                    self.count += 1;
                } else {
                    self.count -= 1;
                }
                if let ScalarValue::$scalar_value_type(x) = value {
                    if !is_retraction {
                        self.sum += x;
                    } else {
                        self.sum -= x;
                    }
                } else {
                    panic!("bad aggregate argument");
                }
                self.count != 0
            }

            fn trigger(&self) -> ScalarValue {
                return ScalarValue::$scalar_value_type(self.sum);
            }
        }
    }
}

impl_sum_accumulator!(i8, Int8);
impl_sum_accumulator!(i16, Int16);
impl_sum_accumulator!(i32, Int32);
impl_sum_accumulator!(i64, Int64);
impl_sum_accumulator!(u8, UInt8);
impl_sum_accumulator!(u16, UInt16);
impl_sum_accumulator!(u32, UInt32);
impl_sum_accumulator!(u64, UInt64);
impl_sum_accumulator!(f32, Float32);
impl_sum_accumulator!(f64, Float64);

pub struct Count {}

impl Aggregate for Count {
    fn output_type(&self, _input_type: &DataType) -> Result<DataType, Error> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self, _input_type: &DataType) -> Box<dyn Accumulator> {
        Box::new(CountAccumulator { count: 0 })
    }
}

#[derive(Debug)]
struct CountAccumulator {
    count: i64,
}

impl Accumulator for CountAccumulator {
    fn add(&mut self, _value: ScalarValue, retract: ScalarValue) -> bool {
        let is_retraction = match retract {
            ScalarValue::Boolean(x) => x,
            _ => panic!("retraction shall be boolean"),
        };
        let _multiplier = if !is_retraction { 1 } else { -1 };
        if is_retraction {
            self.count -= 1;
        } else {
            self.count += 1;
        }
        self.count != 0
    }

    fn trigger(&self) -> ScalarValue {
        return ScalarValue::Int64(self.count);
    }
}