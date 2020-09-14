pub mod parser;

use nom::{
    IResult,
    bytes::complete::{tag},
    combinator::map_res,
    sequence::tuple,
};
use nom::character::is_alphanumeric;
use nom::combinator::recognize;
use nom::sequence::{pair, terminated};
use nom::branch::alt;
use nom::character::complete::{alpha1, alphanumeric1, alphanumeric0, one_of, digit1};
use nom::multi::{many0, many1};

#[derive(Debug, Eq, PartialEq)]
pub enum Query {
    Select {
        expressions: Vec<(Box<Expression>, Option<Identifier>)>,
        filter: Option<Box<Expression>>,
        from: Box<Source>,
        order_by: Vec<Box<Expression>>,
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

pub fn simple_identifier(input: &str) -> IResult<&str, Identifier> {
    let (remaining, ident) = recognize(
        pair(
            alpha1,
            many0(alt((alphanumeric1, tag("_")))),
        ),
    )(input)?;
    Ok((remaining, Identifier::SimpleIdentifier(ident.to_string())))
}

pub fn namespaced_identifier(input: &str) -> IResult<&str, Identifier> {
    if let (remaining, (Identifier::SimpleIdentifier(namespace), _, Identifier::SimpleIdentifier(name))) = tuple((
        simple_identifier,
        tag("."),
        simple_identifier,
    ))(input)? {
        Ok((remaining, Identifier::NamespacedIdentifier(namespace, name)))
    } else {
        unimplemented!()
    }
}

pub fn identifier(input: &str) -> IResult<&str, Identifier> {
    alt((
        namespaced_identifier,
        simple_identifier,
    ))(input)
}

#[derive(Debug, Eq, PartialEq)]
pub enum Value {
    Integer(i64),
}

pub fn integer(input: &str) -> IResult<&str, Value> {
    let (remaining, digits) = recognize(
        digit1
    )(input)?;
    Ok((remaining, Value::Integer(digits.parse::<i64>().unwrap())))
}

#[derive(Debug, Eq, PartialEq)]
pub enum Operator {
    Eq,
    Plus,
    Minus,
    AND,
    OR,
}

#[test]
fn simple_test() {
    //let text = "my_happy.rainbow";
    let text = "my_happy.long.rainbow";
    let res = identifier(text);
    dbg!(res);
}

#[test]
fn simple_test2() {
    //let text = "my_happy.rainbow";
    let text = "012345";
    let res = integer(text);
    dbg!(res);
}

// pub fn query(input: &str) -> IResult<&str, Box<Query>> {}
