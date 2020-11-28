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

use crate::logical::logical::{Node, Transformers, TransformationContext};
use crate::physical::physical::{EmptySchemaContext};
use std::sync::Arc;
use anyhow::Result;

pub fn merge_requalifiers(logical_plan: &Node) -> Result<(Box<Node>, bool)> {
    let (plan_tf, changed) = logical_plan.transform(
        &TransformationContext {
            schema_context: Arc::new(EmptySchemaContext {})
        },
        &Transformers::<bool> {
            node_fn: Some(Box::new(|_ctx, state, node| {
                Ok(match node {
                    Node::Requalifier { source: upper_requalifier_source, qualifier: upper_qualifier } => {
                        match upper_requalifier_source.as_ref() {
                            Node::Requalifier { source: lower_requalifier_source, qualifier: _ } => {
                                (Box::new(Node::Requalifier { source: lower_requalifier_source.clone(), qualifier: upper_qualifier.clone() }), true)
                            }
                            _ => (Box::new(node.clone()), state)
                        }
                    }
                    _ => (Box::new(node.clone()), state)
                })
            })),
            expr_fn: None,
            base_state: false,
            state_reduce: Box::new(|x, y| {
                x || y
            }),
        })?;
    Ok((plan_tf, changed))
}
