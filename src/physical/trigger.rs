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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{PrimitiveArrayOps, ArrayRef, Int64Builder, StringBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder};
use arrow::datatypes::{DataType, TimeUnit};

use crate::physical::arrow::{create_key, GroupByScalar};

pub trait TriggerPrototype: Send + Sync {
    fn create_trigger(&self, key_data_types: Vec<DataType>, time_col: Option<usize>) -> Box<dyn Trigger>;
}

#[derive(Debug)]
pub struct CountingTriggerPrototype {
    pub trigger_count: u64
}

impl CountingTriggerPrototype {
    pub fn new(trigger_count: u64) -> CountingTriggerPrototype {
        CountingTriggerPrototype {
            trigger_count,
        }
    }
}

impl TriggerPrototype for CountingTriggerPrototype {
    fn create_trigger(&self, key_data_types: Vec<DataType>, time_col: Option<usize>) -> Box<dyn Trigger> {
        Box::new(CountingTrigger::new(key_data_types, self.trigger_count))
    }
}

#[derive(Debug)]
pub struct WatermarkTriggerPrototype {
}

impl WatermarkTriggerPrototype {
    pub fn new() -> WatermarkTriggerPrototype {
        WatermarkTriggerPrototype {
        }
    }
}

impl TriggerPrototype for WatermarkTriggerPrototype {
    fn create_trigger(&self, key_data_types: Vec<DataType>, time_col: Option<usize>) -> Box<dyn Trigger> {
        Box::new(WatermarkTrigger::new(key_data_types, time_col.unwrap()))
    }
}

pub trait Trigger: std::fmt::Debug {
    fn end_of_stream_reached(&mut self);
    fn watermark_received(&mut self, watermark: i64);
    fn keys_received(&mut self, keys: Vec<ArrayRef>);
    fn poll(&mut self) -> Vec<ArrayRef>;
}

#[derive(Debug)]
pub struct CountingTrigger {
    key_data_types: Vec<DataType>,
    trigger_count: u64,
    counts: BTreeMap<Vec<GroupByScalar>, u64>,
    to_trigger: BTreeSet<Vec<GroupByScalar>>,
}

impl CountingTrigger {
    pub fn new(key_data_types: Vec<DataType>, trigger_count: u64) -> CountingTrigger {
        CountingTrigger {
            key_data_types,
            trigger_count,
            counts: Default::default(),
            to_trigger: Default::default(),
        }
    }
}

impl Trigger for CountingTrigger {
    fn end_of_stream_reached(&mut self) {
        let mut counts = BTreeMap::new();
        std::mem::swap(&mut counts, &mut self.counts);

        for (key, value) in counts {
            self.to_trigger.insert(key);
        }
    }

    fn watermark_received(&mut self, watermark: i64) {}

    fn keys_received(&mut self, keys: Vec<ArrayRef>) {
        let mut key_vec: Vec<GroupByScalar> = Vec::with_capacity(keys.len());
        for _i in 0..self.key_data_types.len() {
            key_vec.push(GroupByScalar::Int64(0))
        }

        for row in 0..keys[0].len() {
            create_key(keys.as_slice(), row, &mut key_vec).unwrap();

            let count = self.counts.entry(key_vec.clone()).or_insert(0);
            *count += 1;
            if *count == self.trigger_count {
                *count = 0; // TODO: Delete
                self.to_trigger.insert(key_vec.clone());
            }
        }
    }

    fn poll(&mut self) -> Vec<ArrayRef> {
        let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(self.key_data_types.len());
        for key_index in 0..self.key_data_types.len() {
            match self.key_data_types[key_index] {
                DataType::Utf8 => {
                    let mut array = StringBuilder::new(self.to_trigger.len());
                    self.to_trigger.iter().for_each(|k| {
                        match &k[key_index] {
                            GroupByScalar::Utf8(text) => array.append_value(text.as_str()).unwrap(),
                            _ => panic!("bug: key doesn't match schema"),
                            // TODO: Maybe use as_any -> downcast?
                        }
                    });
                    output_columns.push(Arc::new(array.finish()) as ArrayRef);
                }
                DataType::Int64 => {
                    let mut array = Int64Builder::new(self.to_trigger.len());
                    self.to_trigger.iter().for_each(|k| {
                        match k[key_index] {
                            GroupByScalar::Int64(n) => array.append_value(n).unwrap(),
                            _ => panic!("bug: key doesn't match schema"),
                            // TODO: Maybe use as_any -> downcast?
                        }
                    });
                    output_columns.push(Arc::new(array.finish()) as ArrayRef);
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    let mut array = TimestampNanosecondBuilder::new(self.to_trigger.len());
                    self.to_trigger.iter().for_each(|k| {
                        match k[key_index] {
                            GroupByScalar::Timestamp(n) => array.append_value(n).unwrap(),
                            _ => panic!("bug: key doesn't match schema"),
                            // TODO: Maybe use as_any -> downcast?
                        }
                    });
                    output_columns.push(Arc::new(array.finish()) as ArrayRef);
                }
                _ => unimplemented!(),
            }
        }
        self.to_trigger.clear();
        output_columns
    }
}

#[derive(Debug)]
pub struct WatermarkTrigger {
    key_data_types: Vec<DataType>,
    time_keys: BTreeSet<(i64, Vec<GroupByScalar>)>,
    time_key_element: usize,
    watermark: i64,
}

impl WatermarkTrigger {
    pub fn new(key_data_types: Vec<DataType>, time_key_element: usize) -> WatermarkTrigger {
        WatermarkTrigger {
            key_data_types,
            time_keys: Default::default(),
            time_key_element,
            watermark: 0,
        }
    }
}

impl Trigger for WatermarkTrigger {
    fn end_of_stream_reached(&mut self) {
        self.watermark = i64::max_value();
    }

    fn watermark_received(&mut self, watermark: i64) {
        self.watermark = watermark as i64;
    }

    fn keys_received(&mut self, keys: Vec<ArrayRef>) {
        let mut key_vec: Vec<GroupByScalar> = Vec::with_capacity(keys.len());
        for _i in 0..self.key_data_types.len() {
            key_vec.push(GroupByScalar::Int64(0))
        }

        let time_column = keys[self.time_key_element].as_any()
            .downcast_ref::<TimestampNanosecondArray>().unwrap();

        for row in 0..keys[0].len() {
            if time_column.value(row) < self.watermark {
                // TODO: Ignore late data for now.
                continue
            }
            create_key(keys.as_slice(), row, &mut key_vec).unwrap();

            self.time_keys.insert((time_column.value(row), key_vec.clone()));
        }
    }

    fn poll(&mut self) -> Vec<ArrayRef> {
        let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(self.key_data_types.len());

        let mut to_trigger = Vec::new();
        for (ts, key) in &self.time_keys {
            if ts.clone() > self.watermark {
                break
            }

            to_trigger.push((ts.clone(), key.clone()))
        }
        for k in &to_trigger {
            self.time_keys.remove(k);
        }
        for key_index in 0..self.key_data_types.len() {
            match self.key_data_types[key_index] {
                DataType::Utf8 => {
                    let mut array = StringBuilder::new(to_trigger.len());
                    to_trigger.iter().for_each(|(_, k)| {
                        match &k[key_index] {
                            GroupByScalar::Utf8(text) => array.append_value(text.as_str()).unwrap(),
                            _ => panic!("bug: key doesn't match schema"),
                            // TODO: Maybe use as_any -> downcast?
                        }
                    });
                    output_columns.push(Arc::new(array.finish()) as ArrayRef);
                }
                DataType::Int64 => {
                    let mut array = Int64Builder::new(to_trigger.len());
                    to_trigger.iter().for_each(|(_, k)| {
                        match k[key_index] {
                            GroupByScalar::Int64(n) => array.append_value(n).unwrap(),
                            _ => panic!("bug: key doesn't match schema"),
                            // TODO: Maybe use as_any -> downcast?
                        }
                    });
                    output_columns.push(Arc::new(array.finish()) as ArrayRef);
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    let mut array = TimestampNanosecondBuilder::new(to_trigger.len());
                    to_trigger.iter().for_each(|(_, k)| {
                        match k[key_index] {
                            GroupByScalar::Timestamp(n) => array.append_value(n).unwrap(),
                            _ => panic!("bug: key doesn't match schema"),
                            // TODO: Maybe use as_any -> downcast?
                        }
                    });
                    output_columns.push(Arc::new(array.finish()) as ArrayRef);
                }
                _ => unimplemented!(),
            }
        }
        output_columns
    }
}
