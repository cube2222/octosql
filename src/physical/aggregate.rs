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

use std::ops::{AddAssign, SubAssign};

use anyhow::Result;
use arrow::datatypes::DataType;
use std::collections::BTreeMap;
use std::mem::drop;

use crate::physical::physical::ScalarValue;

pub trait Aggregate: Send + Sync {
    fn create_accumulator(&self, input_type: &DataType) -> Box<dyn Accumulator>;
}

pub trait Accumulator: std::fmt::Debug {
    fn add(&mut self, value: ScalarValue, retract: ScalarValue) -> bool;
    fn trigger(&self) -> ScalarValue;
}

pub struct Sum {}

impl Aggregate for Sum {
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

pub struct Avg {}

impl Aggregate for Avg {
    fn create_accumulator(&self, input_type: &DataType) -> Box<dyn Accumulator> {
        match input_type {
            DataType::Int8 => Box::new(AvgAccumulator::<i8> { underlying_sum: SumAccumulator::<i8> { sum: 0, count: 0 } }),
            DataType::Int16 => Box::new(AvgAccumulator::<i16> { underlying_sum: SumAccumulator::<i16> { sum: 0, count: 0 } }),
            DataType::Int32 => Box::new(AvgAccumulator::<i32> { underlying_sum: SumAccumulator::<i32> { sum: 0, count: 0 } }),
            DataType::Int64 => Box::new(AvgAccumulator::<i64> { underlying_sum: SumAccumulator::<i64> { sum: 0, count: 0 } }),
            DataType::UInt8 => Box::new(AvgAccumulator::<u8> { underlying_sum: SumAccumulator::<u8> { sum: 0, count: 0 } }),
            DataType::UInt16 => Box::new(AvgAccumulator::<u16> { underlying_sum: SumAccumulator::<u16> { sum: 0, count: 0 } }),
            DataType::UInt32 => Box::new(AvgAccumulator::<u32> { underlying_sum: SumAccumulator::<u32> { sum: 0, count: 0 } }),
            DataType::UInt64 => Box::new(AvgAccumulator::<u64> { underlying_sum: SumAccumulator::<u64> { sum: 0, count: 0 } }),
            DataType::Float32 => Box::new(AvgAccumulator::<f32> { underlying_sum: SumAccumulator::<f32> { sum: 0.0, count: 0 } }),
            DataType::Float64 => Box::new(AvgAccumulator::<f64> { underlying_sum: SumAccumulator::<f64> { sum: 0.0, count: 0 } }),
            _ => {
                dbg!(input_type);
                unimplemented!()
            }
        }
    }
}

#[derive(Debug)]
struct AvgAccumulator<T: AddAssign<T> + SubAssign<T>> {
    underlying_sum: SumAccumulator<T>,
}

macro_rules! impl_avg_accumulator {
    ($primitive_type: ident, $scalar_value_type: ident) => {
        impl Accumulator for AvgAccumulator<$primitive_type> {
            fn add(&mut self, value: ScalarValue, retract: ScalarValue) -> bool {
                self.underlying_sum.add(value, retract)
            }

            fn trigger(&self) -> ScalarValue {
                if self.underlying_sum.count == 0 {
                    return ScalarValue::Float64(0 as f64);
                } else {
                    return ScalarValue::Float64(self.underlying_sum.sum as f64 / self.underlying_sum.count as f64);
                }
            }
        }
    }
}

impl_avg_accumulator!(i8, Int8);
impl_avg_accumulator!(i16, Int16);
impl_avg_accumulator!(i32, Int32);
impl_avg_accumulator!(i64, Int64);
impl_avg_accumulator!(u8, UInt8);
impl_avg_accumulator!(u16, UInt16);
impl_avg_accumulator!(u32, UInt32);
impl_avg_accumulator!(u64, UInt64);
impl_avg_accumulator!(f32, Float32);
impl_avg_accumulator!(f64, Float64);

pub struct Min {}

impl Aggregate for Min {
    fn create_accumulator(&self, input_type: &DataType) -> Box<dyn Accumulator> {
        match input_type {
            DataType::Int8 => Box::new(MinAccumulator::<i8> { values_counts: BTreeMap::new() }),
            DataType::Int16 => Box::new(MinAccumulator::<i16> { values_counts: BTreeMap::new() }),
            DataType::Int32 => Box::new(MinAccumulator::<i32> { values_counts: BTreeMap::new() }),
            DataType::Int64 => Box::new(MinAccumulator::<i64> { values_counts: BTreeMap::new() }),
            DataType::UInt8 => Box::new(MinAccumulator::<u8> { values_counts: BTreeMap::new() }),
            DataType::UInt16 => Box::new(MinAccumulator::<u16> { values_counts: BTreeMap::new() }),
            DataType::UInt32 => Box::new(MinAccumulator::<u32> { values_counts: BTreeMap::new() }),
            DataType::UInt64 => Box::new(MinAccumulator::<u64> { values_counts: BTreeMap::new() }),
            _ => {
                dbg!(input_type);
                unimplemented!()
            }
        }
    }
}

#[derive(Debug)]
struct MinAccumulator<T> {
    values_counts: BTreeMap<T, i64>,
}

macro_rules! impl_min_accumulator {
    ($primitive_type: ident, $scalar_value_type: ident) => {
        impl Accumulator for MinAccumulator<$primitive_type> {
            fn add(&mut self, value: ScalarValue, retract: ScalarValue) -> bool {
                if let ScalarValue::$scalar_value_type(raw_value) = value {
                    let value_count = self.values_counts.entry(raw_value).or_insert(0);

                    let is_retraction = match retract {
                        ScalarValue::Boolean(x) => x,
                        _ => panic!("retraction shall be boolean"),
                    };
                    if !is_retraction {
                        *value_count += 1;
                    } else {
                        *value_count -= 1;
                    }

                    let should_remove = *value_count == 0; // we can clear the value if it was retracted

                    drop(value_count);

                    if should_remove {
                        self.values_counts.remove(&raw_value);
                    }

                    !should_remove
                } else {
                    panic!("bad aggregate argument");
                }
            }

            fn trigger(&self) -> ScalarValue {
                match self.values_counts.keys().next() {
                    Some(val) => return ScalarValue::$scalar_value_type(val.clone()),
                    None => return ScalarValue::Null,
                }
            }
        }
    }
}

impl_min_accumulator!(i8, Int8);
impl_min_accumulator!(i16, Int16);
impl_min_accumulator!(i32, Int32);
impl_min_accumulator!(i64, Int64);
impl_min_accumulator!(u8, UInt8);
impl_min_accumulator!(u16, UInt16);
impl_min_accumulator!(u32, UInt32);
impl_min_accumulator!(u64, UInt64);

pub struct Max {}

impl Aggregate for Max {
    fn create_accumulator(&self, input_type: &DataType) -> Box<dyn Accumulator> {
        match input_type {
            DataType::Int8 => Box::new(MaxAccumulator::<i8> { values_counts: BTreeMap::new() }),
            DataType::Int16 => Box::new(MaxAccumulator::<i16> { values_counts: BTreeMap::new() }),
            DataType::Int32 => Box::new(MaxAccumulator::<i32> { values_counts: BTreeMap::new() }),
            DataType::Int64 => Box::new(MaxAccumulator::<i64> { values_counts: BTreeMap::new() }),
            DataType::UInt8 => Box::new(MaxAccumulator::<u8> { values_counts: BTreeMap::new() }),
            DataType::UInt16 => Box::new(MaxAccumulator::<u16> { values_counts: BTreeMap::new() }),
            DataType::UInt32 => Box::new(MaxAccumulator::<u32> { values_counts: BTreeMap::new() }),
            DataType::UInt64 => Box::new(MaxAccumulator::<u64> { values_counts: BTreeMap::new() }),
            _ => {
                dbg!(input_type);
                unimplemented!()
            }
        }
    }
}

#[derive(Debug)]
struct MaxAccumulator<T> {
    values_counts: BTreeMap<T, i64>,
}

macro_rules! impl_max_accumulator {
    ($primitive_type: ident, $scalar_value_type: ident) => {
        impl Accumulator for MaxAccumulator<$primitive_type> {
            fn add(&mut self, value: ScalarValue, retract: ScalarValue) -> bool {
                if let ScalarValue::$scalar_value_type(raw_value) = value {
                    let value_count = self.values_counts.entry(raw_value).or_insert(0);

                    let is_retraction = match retract {
                        ScalarValue::Boolean(x) => x,
                        _ => panic!("retraction shall be boolean"),
                    };
                    if !is_retraction {
                        *value_count += 1;
                    } else {
                        *value_count -= 1;
                    }

                    let should_remove = *value_count == 0; // we can clear the value if it was retracted

                    drop(value_count);

                    if should_remove {
                        self.values_counts.remove(&raw_value);
                    }

                    !should_remove
                } else {
                    panic!("bad aggregate argument");
                }
            }

            fn trigger(&self) -> ScalarValue {
                match self.values_counts.keys().next_back() { // only difference to Min is here
                    Some(val) => return ScalarValue::$scalar_value_type(val.clone()),
                    None => return ScalarValue::Null,
                }
            }
        }
    }
}

impl_max_accumulator!(i8, Int8);
impl_max_accumulator!(i16, Int16);
impl_max_accumulator!(i32, Int32);
impl_max_accumulator!(i64, Int64);
impl_max_accumulator!(u8, UInt8);
impl_max_accumulator!(u16, UInt16);
impl_max_accumulator!(u32, UInt32);
impl_max_accumulator!(u64, UInt64);
