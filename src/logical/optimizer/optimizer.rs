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

use crate::logical::logical::Node;
use anyhow::Result;
use crate::logical::optimizer::merge_requalifiers::merge_requalifiers;
use crate::logical::optimizer::push_down_filters::push_down_filters;

pub fn optimize(mut logical_plan: Box<Node>) -> Result<Box<Node>> {
    let rules: Vec<Box<dyn Fn(&Node) -> Result<(Box<Node>, bool)>>> = vec![
        Box::new(merge_requalifiers),
        Box::new(push_down_filters),
    ];

    loop {
        let mut changed = false;

        for rule in &rules {
            let (transformed_plan, cur_changed) = (rule)(logical_plan.as_ref())?;
            if cur_changed {
                logical_plan = transformed_plan
            }
            changed = changed || cur_changed
        }

        if !changed {
            break
        }
    }

    Ok(logical_plan)
}
