use anyhow::Result;

use crate::logical::logical::{Node, Transformers, TransformationContext, TableValuedFunction};
use crate::physical::physical::{Identifier, EmptySchemaContext};
use std::sync::Arc;

pub fn remove_wildcards(logical_plan: &Node) -> Result<Box<Node>> {
    let (plan_tf, _) = logical_plan.transform(
        &TransformationContext {
            schema_context: Arc::new(EmptySchemaContext {})
        },
        &Transformers::<Option<Vec<Identifier>>> {
            node_fn: Some(Box::new(|ctx, state, node| {
                match node {
                    Node::Map { source, expressions, wildcards, keep_source_fields } => {
                        if let None = state {}
                        match state {
                            None => unimplemented!(),  // TODO: All fields from source are wildcard-selectable.
                            Some(idents) => unimplemented!(), // TODO: Only these are selectable.
                        }
                        // TODO: Return a Map type which doesn't contain wildcards.
                        unimplemented!()
                    }
                    Node::Source { .. } => Ok((Box::new(node.clone()), None)),
                    Node::Filter { .. } => Ok((Box::new(node.clone()), state)),
                    Node::GroupBy { .. } => Ok((Box::new(node.clone()), None)),
                    Node::Join { .. } => Ok((Box::new(node.clone()), state)),
                    Node::Requalifier { .. } => unimplemented!(), // TODO: Requalify fields
                    Node::Function(tbv) => {
                        match tbv {
                            TableValuedFunction::Range(_, _) => Ok((Box::new(node.clone()), None)),
                            TableValuedFunction::MaxDiffWatermarkGenerator(_, _, _) => Ok((Box::new(node.clone()), state)),
                        }
                    }
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
