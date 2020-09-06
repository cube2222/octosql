use nom::{
    IResult,
    bytes::complete::{tag},
    combinator::map_res,
    sequence::tuple,
};
use nom::character::is_alphanumeric;
use nom::combinator::recognize;
use nom::sequence::pair;
use nom::branch::alt;
use nom::character::complete::{alpha1, alphanumeric1, alphanumeric0};
use nom::multi::many0;

pub mod parser;

#[derive(Debug, Eq, PartialEq)]
pub enum Query {
    Select {
        filter: Option<Box<Expression>>,
        from: Box<Source>,
        order_by: Vec<Box<Expression>>,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub enum Source {
    Table(Identifier),
    Subquery(Box<Query>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Expression {
    Variable(Identifier),
    Constant(Value),
    Function(Identifier, Box<Expression>),
    Operator(Box<Expression>, Operator, Box<Expression>),
}

#[derive(Debug, Eq, PartialEq)]
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
    Plus,
    Minus,
    AND,
    OR,
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
    // let simple_ident = recognize(
    //     pair(
    //         alpha1,
    //         many0(alt((alphanumeric1, tag("_")))),
    //     ),
    // );
    // let namespaced_ident = recognize(
    //     tuple((
    //         &simple_ident,
    //         tag("."),
    //         identifier,
    //     )),
    // );
    //
    // let (remaining, ident) = alt((
    //     namespaced_ident,
    //     &simple_ident,
    // ))(input)?;
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

#[test]
fn simple_test() {
    //let text = "my_happy.rainbow";
    let text = "my_happy.long.rainbow";
    let res = namespaced_identifier(text);
    dbg!(res);
}

// pub fn query(input: &str) -> IResult<&str, Box<Query>> {}
