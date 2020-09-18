pub mod parser;

#[derive(Debug, Eq, PartialEq)]
pub enum Query {
    Select {
        expressions: Vec<(Box<Expression>, Option<Identifier>)>,
        filter: Option<Box<Expression>>,
        from: Box<Source>,
        order_by:Vec<Box<Expression>>,
        group_by: Vec<Box<Expression>>,
    },
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
    Wildcard,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Identifier {
    SimpleIdentifier(String),
    NamespacedIdentifier(String, String),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Value {
    Integer(i64),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Operator {
    Eq,
    Plus,
    Minus,
    AND,
    OR,
}
