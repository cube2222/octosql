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

// TODO: Remove useless maps. Outschema = Inschema.

use crate::logical::logical::{Node, TransformationContext, Transformers, Expression};
use crate::physical::physical::{EmptySchemaContext, Identifier};
use anyhow::Result;
use std::sync::Arc;
use itertools::Itertools;

use std::collections::BTreeMap;

pub fn push_down_filters(logical_plan: &Node) -> Result<(Box<Node>, bool)> {
    let (plan_tf, changed) = logical_plan.transform(
        &TransformationContext {
            schema_context: Arc::new(EmptySchemaContext {})
        },
        &Transformers::<bool> {
            node_fn: Some(Box::new(|ctx, state, node| {
                Ok(match node {
                    Node::Filter { source: filter_source, filter_expr } => {
                        match filter_source.as_ref() {
                            Node::Map { source: map_source, expressions: map_exprs } => {
                                let mut changed = state;
                                let mut above_map_filters = vec![];
                                let mut below_map_filters = vec![];
                                let parts = split_by_and(filter_expr.as_ref());

                                'parts: for part in parts {
                                    if let Some(filter_vars) = variables_used(&part) {
                                        'filter_vars: for filter_var in filter_vars {
                                            for (expr, ident) in map_exprs {
                                                if &filter_var == ident {
                                                    if let Expression::Variable(var_ident) = expr.as_ref() {
                                                        if var_ident == ident {
                                                            continue 'filter_vars;
                                                        } else {
                                                            above_map_filters.push(Box::new(part));
                                                            continue 'parts;
                                                        }
                                                    } else {
                                                        above_map_filters.push(Box::new(part));
                                                        continue 'parts;
                                                    }
                                                }
                                            }
                                            continue 'filter_vars;
                                        }
                                        below_map_filters.push(Box::new(part));
                                    } else {
                                        above_map_filters.push(Box::new(part));
                                        continue 'parts;
                                    }
                                }

                                let mut out = map_source.clone();
                                if below_map_filters.len() > 0 {
                                    let below_map_filter_expr = if below_map_filters.len() == 1 {
                                        below_map_filters.into_iter().exactly_one().unwrap()
                                    } else {
                                        Box::new(Expression::Function(Identifier::SimpleIdentifier("AND".to_string()), below_map_filters))
                                    };
                                    out = Box::new(Node::Filter {
                                        source: out,
                                        filter_expr:
                                        below_map_filter_expr,
                                    });
                                    changed = true;
                                }
                                out = Box::new(Node::Map { source: out, expressions: map_exprs.clone() });
                                if above_map_filters.len() > 0 {
                                    let above_map_filter_expr = if above_map_filters.len() == 1 {
                                        above_map_filters.into_iter().exactly_one().unwrap()
                                    } else {
                                        Box::new(Expression::Function(Identifier::SimpleIdentifier("AND".to_string()), above_map_filters))
                                    };
                                    out = Box::new(Node::Filter {
                                        source: out,
                                        filter_expr:
                                        above_map_filter_expr,
                                    });
                                }

                                (out, changed)
                            }

                            Node::Requalifier { source, qualifier } => {
                                let mut changed = false;
                                let source_metadata = source.metadata(ctx.schema_context.clone())?;

                                let mut mapping = BTreeMap::new();
                                for field in source_metadata.schema.fields() {
                                    let ident = ident_from_string(field.name());
                                    let qualified_ident = requalify_ident(qualifier, &ident);

                                    mapping.insert(qualified_ident, ident);
                                }
                                dbg!(&mapping);

                                let mut above_requlafier_filters = vec![];
                                let mut below_requalifier_filters = vec![];
                                let parts = split_by_and(filter_expr.as_ref());

                                for part in parts {
                                    if let Some(derequalified_part) = derequalify_variables(&mut mapping, &part) {
                                        below_requalifier_filters.push(Box::new(derequalified_part));
                                    } else {
                                        above_requlafier_filters.push(Box::new(part));
                                    }
                                }

                                let mut out = source.clone();
                                if below_requalifier_filters.len() > 0 {
                                    let below_requalifier_filter_expr = if below_requalifier_filters.len() == 1 {
                                        below_requalifier_filters.into_iter().exactly_one().unwrap()
                                    } else {
                                        Box::new(Expression::Function(Identifier::SimpleIdentifier("AND".to_string()), below_requalifier_filters))
                                    };
                                    out = Box::new(Node::Filter {
                                        source: out,
                                        filter_expr:
                                        below_requalifier_filter_expr,
                                    });
                                    changed = true;
                                }
                                out = Box::new(Node::Requalifier { source: out, qualifier: qualifier.clone() });
                                if above_requlafier_filters.len() > 0 {
                                    let above_requalifier_filter_expr = if above_requlafier_filters.len() == 1 {
                                        above_requlafier_filters.into_iter().exactly_one().unwrap()
                                    } else {
                                        Box::new(Expression::Function(Identifier::SimpleIdentifier("AND".to_string()), above_requlafier_filters))
                                    };
                                    out = Box::new(Node::Filter {
                                        source: out,
                                        filter_expr:
                                        above_requalifier_filter_expr,
                                    });
                                }

                                (out, changed)
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

fn variables_used(expr: &Expression) -> Option<Vec<Identifier>> {
    let mut out = vec![];
    if variables_used_internal(expr, &mut out) {
        Some(out)
    } else {
        None
    }
}

fn variables_used_internal(expr: &Expression, vars: &mut Vec<Identifier>) -> bool {
    match expr {
        Expression::Variable(x) => vars.push(x.clone()),
        Expression::Constant(_x) => {}
        Expression::Function(_name, args) => {
            for arg in args {
                let ok = variables_used_internal(arg.as_ref(), vars);
                if !ok {
                    return false;
                }
            }
        }
        Expression::Wildcard(_) => {}
        Expression::Subquery(_) => return false,
    }
    true
}

fn split_by_and(expr: &Expression) -> Vec<Expression> {
    let mut out = vec![];
    split_by_and_internal(&mut out, expr);
    out
}

fn split_by_and_internal(parts: &mut Vec<Expression>, expr: &Expression) {
    match expr {
        Expression::Function(name, args) => {
            if name == &Identifier::SimpleIdentifier("AND".to_string()) {
                for arg in args {
                    split_by_and_internal(parts, arg);
                }
                return;
            }
        }
        _ => {}
    }
    parts.push(expr.clone())
}

fn ident_from_string(str: &str) -> Identifier {
    if let Some(i) = str.find(".") {
        Identifier::NamespacedIdentifier(str[..i].to_string(), str[i + 1..].to_string())
    } else {
        Identifier::SimpleIdentifier(str.to_string())
    }
}

fn requalify_ident(qualifier: &str, ident: &Identifier) -> Identifier {
    let name = match ident {
        Identifier::SimpleIdentifier(name) => name,
        Identifier::NamespacedIdentifier(_, name) => name,
    };
    Identifier::NamespacedIdentifier(qualifier.to_string(), name.clone())
}

fn derequalify_variables(mapping: &BTreeMap<Identifier, Identifier>, expr: &Expression) -> Option<Expression> {
    match expr {
        Expression::Variable(ident) => {
            Some(if mapping.contains_key(&ident) {
                Expression::Variable(mapping[&ident].clone())
            } else {
                Expression::Variable(ident.clone())
            })
        }
        Expression::Constant(_) => Some(expr.clone()),
        Expression::Function(name, args) => {
            let mut new_args = vec![];
            for arg in args {
                new_args.push(Box::new(derequalify_variables(mapping, arg)?));
            }
            Some(Expression::Function(name.clone(), new_args))
        }
        Expression::Wildcard(_) => Some(expr.clone()),
        Expression::Subquery(_) => None,
    }
}
