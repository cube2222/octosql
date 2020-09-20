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

pub mod parser;

#[derive(Debug, Eq, PartialEq)]
pub enum Query {
    Select {
        expressions: Vec<SelectExpression>,
        filter: Option<Box<Expression>>,
        from: Box<Source>,
        order_by:Vec<Box<Expression>>,
        group_by: Vec<Box<Expression>>,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub enum SelectExpression {
    Expression(Box<Expression>, Option<Identifier>),
    Wildcard(Option<String>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Source {
    Table(Identifier, Option<Identifier>),
    Subquery(Box<Query>, Option<Identifier>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Expression {
    Variable(Identifier),
    Constant(Value),
    Function(Identifier, Vec<Box<Expression>>),
    Operator(Box<Expression>, Operator, Box<Expression>),
    Wildcard(Option<String>),
    Subquery(Box<Query>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Identifier {
    SimpleIdentifier(String),
    NamespacedIdentifier(String, String),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Value {
    Integer(i64),
    String(String),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Operator {
    Lt,
    LtEq,
    Eq,
    GtEq,
    Gt,
    Plus,
    Minus,
    AND,
    OR,
}
