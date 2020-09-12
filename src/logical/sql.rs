use crate::physical::physical::{Identifier, ScalarValue};
use crate::logical::logical::{Node, Expression, Aggregate};
use crate::parser;
use crate::parser::{identifier, Value, Source};
use datafusion::logicalplan::FunctionType::Scalar;
use std::collections::BTreeMap;

pub fn query_to_logical_plan(query: &Box<parser::Query>) -> Box<Node> {
    match &**query {
        parser::Query::Select { expressions, filter, from, order_by } => {
            let mut plan = source_to_logical_plan(from);

            let mut variables: BTreeMap<Identifier, Box<Expression>> = BTreeMap::new();

            let mut topmost_map_fields = Vec::with_capacity(expressions.len());

            for (i, (expr, alias)) in expressions.iter().enumerate() {
                let name = (*alias).clone().unwrap_or_else(|| parser::Identifier::SimpleIdentifier(format!("column_{}", i)));
                let ident = identifier_to_logical_plan(&name);
                variables.insert(ident.clone(), expression_to_logical_plan(expr));

                topmost_map_fields.push(ident);
            }

            unimplemented!()
        }
    }
}

pub fn source_to_logical_plan(expr: &Box<parser::Source>) -> Box<Node> {
    match &**expr {
        parser::Source::Table(ident, alias) => {
            Box::new(Node::Source { name: identifier_to_logical_plan(&ident), alias: alias.clone().map(|ident| identifier_to_logical_plan(&ident)) })
        }
        parser::Source::Subquery(subquery, alias) => {
            query_to_logical_plan(&subquery)
        }
    }
}

pub fn expression_to_logical_plan(expr: &Box<parser::Expression>) -> Box<Expression> {
    match &**expr {
        parser::Expression::Variable(ident) => {
            Box::new(Expression::Variable(identifier_to_logical_plan(&ident)))
        }
        parser::Expression::Constant(value) => {
            Box::new(Expression::Constant(value_to_logical_plan(&value)))
        }
        parser::Expression::Function(name, args) => {
            unimplemented!()
        }
        parser::Expression::Operator(left, op, right) => {
            unimplemented!()
        }
    }
}

pub fn identifier_to_logical_plan(ident: &parser::Identifier) -> Identifier {
    match ident {
        parser::Identifier::SimpleIdentifier(id) => {
            Identifier::SimpleIdentifier(id.clone())
        }
        parser::Identifier::NamespacedIdentifier(namespace, id) => {
            Identifier::NamespacedIdentifier(namespace.clone(), id.clone())
        }
    }
}

pub fn value_to_logical_plan(val: &parser::Value) -> ScalarValue {
    match val {
        parser::Value::Integer(v) => {
            ScalarValue::Int64(v.clone())
        }
    }
}

#[test]
fn my_test() {}
