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

use anyhow::Result;

use crate::logical::logical::{Node, Transformers, TransformationContext, TableValuedFunction, Expression};
use crate::physical::physical::{Identifier, EmptySchemaContext, RETRACTIONS_FIELD};
use std::sync::Arc;
use itertools::Itertools;
use crate::parser::parser::parse_ident;

fn name_to_ident(name: &str) -> Identifier {
    if let Some(i) = name.find(".") {
        Identifier::NamespacedIdentifier(name[0..i].to_string(), name[i+1..name.len()].to_string())
    } else {
        Identifier::SimpleIdentifier(name.to_string())
    }
}

pub fn remove_wildcards(logical_plan: &Node) -> Result<Box<Node>> {
    let (plan_tf, _) = logical_plan.transform(
        &TransformationContext {
            schema_context: Arc::new(EmptySchemaContext {})
        },
        &Transformers::<Option<Vec<Identifier>>> {
            node_fn: Some(Box::new(|ctx, state, node| {
                match node {
                    Node::MapWithWildcards { source, expressions, wildcards, keep_source_fields } => {
                        if *keep_source_fields {
                            // Wildcards don't matter when keep_source_fields is true.
                            let source_schema = source.metadata(ctx.schema_context.clone())?.schema;
                            let mut new_expressions = expressions.clone();
                            new_expressions.extend(source_schema.fields().iter()
                                .map(|field| field.name().clone())
                                .filter(|name| name != RETRACTIONS_FIELD)
                                .map(|name| name_to_ident(name.as_str()))
                                .map(|ident| (Box::new(Expression::Variable(ident.clone())), ident)));

                            let new_wildcard_selectable_fields = {
                                let source_schema = source.metadata(ctx.schema_context.clone())?.schema;
                                source_schema.fields().iter()
                                    .filter(|field| field.name() != RETRACTIONS_FIELD)
                                    .map(|field| name_to_ident(field.name().as_str()))
                                    .collect()
                            };

                            Ok((Box::new(Node::Map {
                                source: source.clone(),
                                expressions: new_expressions,
                            }), Some(new_wildcard_selectable_fields)))
                        } else {
                            let wildcard_selectable_fields: Vec<_> = match state {
                                None => {
                                    let source_schema = source.metadata(ctx.schema_context.clone())?.schema;
                                    source_schema.fields().iter()
                                        .filter(|field| field.name() != RETRACTIONS_FIELD)
                                        .map(|field| name_to_ident(field.name().as_str()))
                                        .collect()
                                },
                                Some(idents) => {
                                   idents.clone()
                                },
                            };
                            let mut new_expressions: Vec<_> = wildcards.iter()
                                .flat_map(|wildcard| {
                                    wildcard_selectable_fields.iter()
                                        .filter(|ident| {
                                            match wildcard {
                                                None => true,
                                                Some(qualifier) => {
                                                    if let Identifier::NamespacedIdentifier(field_qualifier, _) = ident {
                                                        field_qualifier == qualifier
                                                    } else {
                                                        false
                                                    }
                                                }
                                            }
                                        })
                                        .map(|ident| (Box::new(Expression::Variable(ident.clone())), ident.clone()))
                                        .collect::<Vec<_>>()
                                })
                                .collect();
                            new_expressions.append(&mut expressions.clone());

                            Ok((Box::new(Node::Map {
                                source: source.clone(),
                                expressions: new_expressions,
                            }), Some(wildcard_selectable_fields)))
                        }
                    }
                    Node::Source { .. } => Ok((Box::new(node.clone()), None)),
                    Node::Filter { .. } => Ok((Box::new(node.clone()), state)),
                    Node::GroupBy { .. } => Ok((Box::new(node.clone()), None)),
                    Node::Join { .. } => Ok((Box::new(node.clone()), state)),
                    Node::Requalifier { source, qualifier } => {
                        let new_state: Option<Vec<_>> = state.map(|idents| idents.into_iter().map(|ident| match ident {
                            Identifier::SimpleIdentifier(name) => Identifier::NamespacedIdentifier(qualifier.clone(), name),
                            Identifier::NamespacedIdentifier(_, name) => Identifier::NamespacedIdentifier(qualifier.clone(), name),
                        }).collect());

                        Ok((Box::new(node.clone()), new_state))
                    },
                    Node::Function(tbv) => {
                        match tbv {
                            TableValuedFunction::MaxDiffWatermarkGenerator(_, _, _) => Ok((Box::new(node.clone()), state)),
                            TableValuedFunction::Range(_, _) => Ok((Box::new(node.clone()), None)),
                            TableValuedFunction::Tumble(_, _, _, _) => Ok((Box::new(node.clone()), state)),
                        }
                    }
                    Node::Map { .. } => unimplemented!(),
                }
            })),
            expr_fn: None,
            base_state: None,
            state_reduce: Box::new(|mut x, mut y| {
                let mut output: Option<Vec<Identifier>> = None;
                if let Some(mut tab) = x {
                    output = Some(tab)
                }
                if let Some(mut tab) = y {
                    match output {
                        None => output = Some(tab),
                        Some(mut output_tab) => {
                            output_tab.append(&mut tab);
                            output = Some(output_tab)
                        }
                    }
                }
                output
            }),
        })?;
    Ok(plan_tf)
}
