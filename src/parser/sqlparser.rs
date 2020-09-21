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

// Copyright 2020 Andy Grove
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

use sqlparser::ast::{Cte, Expr, Fetch, Offset, OrderByExpr, Query, SelectItem, SetOperator, TableAlias, TableWithJoins, Top, Values, ObjectName, Join, JoinOperator, Ident, DateTimeField, Value, DataType, Function, ListAgg, BinaryOperator, UnaryOperator};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::dialect::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::parser::IsOptional::Optional;
use sqlparser::tokenizer::{Token, Tokenizer};

macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

pub enum OctoSQLStatement {
    Query(Box<OctoSQLQuery>),
}

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct OctoSQLQuery {
    /// WITH (common table expressions, or CTEs)
    pub ctes: Vec<Cte>,
    /// SELECT or UNION / EXCEPT / INTECEPT
    pub body: OctoSQLSetExpr,
    /// ORDER BY
    pub order_by: Vec<OrderByExpr>,
    /// `LIMIT { <N> | ALL }`
    pub limit: Option<OctoSQLExpr>,
    /// `OFFSET <N> [ { ROW | ROWS } ]`
    pub offset: Option<Offset>,
    /// `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
    pub fetch: Option<Fetch>,
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum OctoSQLSetExpr {
    /// Restricted SELECT .. FROM .. HAVING (no ORDER BY or set operations)
    Select(Box<OctoSQLSelect>),
    /// Parenthesized SELECT subquery, which may include more set operations
    /// in its body and an optional ORDER BY / LIMIT.
    Query(Box<Query>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: SetOperator,
        all: bool,
        left: Box<OctoSQLSetExpr>,
        right: Box<OctoSQLSetExpr>,
    },
    Values(Values),
    // TODO: ANSI SQL supports `TABLE` here.
}

/// A restricted variant of `SELECT` (without CTEs/`ORDER BY`), which may
/// appear either as the only body item of an `SQLQuery`, or as an operand
/// to a set operation like `UNION`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct OctoSQLSelect {
    pub distinct: bool,
    /// MSSQL syntax: `TOP (<N>) [ PERCENT ] [ WITH TIES ]`
    pub top: Option<Top>,
    /// projection expressions
    pub projection: Vec<SelectItem>,
    /// FROM
    pub from: Vec<OctoSQLTableWithJoins>,
    /// WHERE
    pub selection: Option<OctoSQLExpr>,
    /// GROUP BY
    pub group_by: Vec<OctoSQLExpr>,
    /// HAVING
    pub having: Option<OctoSQLExpr>,
}

/// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum OctoSQLSelectItem {
    /// Any expression, not followed by `[ AS ] alias`
    UnnamedExpr(OctoSQLExpr),
    /// An expression, followed by `[ AS ] alias`
    ExprWithAlias { expr: OctoSQLExpr, alias: Ident },
    /// `alias.*` or even `schema.table.*`
    QualifiedWildcard(ObjectName),
    /// An unqualified `*`
    Wildcard,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct OctoSQLTableWithJoins {
    pub relation: OctoSQLTableFactor,
    pub joins: Vec<OctoSQLJoin>,
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum OctoSQLTableFactor {
    Table {
        name: ObjectName,
        alias: Option<TableAlias>,
        /// Arguments of a table-valued function, as supported by Postgres
        /// and MSSQL. Note that deprecated MSSQL `FROM foo (NOLOCK)` syntax
        /// will also be parsed as `args`.
        args: Vec<OctoSQLExpr>,
        /// MSSQL-specific `WITH (...)` hints such as NOLOCK.
        with_hints: Vec<OctoSQLExpr>,
    },
    Derived {
        lateral: bool,
        subquery: Box<OctoSQLQuery>,
        alias: Option<TableAlias>,
    },
    /// Represents a parenthesized table factor. The SQL spec only allows a
    /// join expression (`(foo <JOIN> bar [ <JOIN> baz ... ])`) to be nested,
    /// possibly several times, but the parser also accepts the non-standard
    /// nesting of bare tables (`table_with_joins.joins.is_empty()`), so the
    /// name `NestedJoin` is a bit of misnomer.
    NestedJoin(Box<TableWithJoins>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct OctoSQLJoin {
    pub relation: OctoSQLTableFactor,
    pub join_operator: JoinOperator,
}

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum OctoSQLExpr {
    /// Identifier e.g. table name or column name
    Identifier(Ident),
    /// Unqualified wildcard (`*`). SQL allows this in limited contexts, such as:
    /// - right after `SELECT` (which is represented as a [SelectItem::Wildcard] instead)
    /// - or as part of an aggregate function, e.g. `COUNT(*)`,
    ///
    /// ...but we currently also accept it in contexts where it doesn't make
    /// sense, such as `* + *`
    Wildcard,
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    /// (Same caveats apply to `QualifiedWildcard` as to `Wildcard`.)
    QualifiedWildcard(Vec<Ident>),
    /// Multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
    CompoundIdentifier(Vec<Ident>),
    /// `IS NULL` expression
    IsNull(Box<OctoSQLExpr>),
    /// `IS NOT NULL` expression
    IsNotNull(Box<OctoSQLExpr>),
    /// `[ NOT ] IN (val1, val2, ...)`
    InList {
        expr: Box<OctoSQLExpr>,
        list: Vec<OctoSQLExpr>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        expr: Box<OctoSQLExpr>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// `<expr> [ NOT ] BETWEEN <low> AND <high>`
    Between {
        expr: Box<OctoSQLExpr>,
        negated: bool,
        low: Box<OctoSQLExpr>,
        high: Box<OctoSQLExpr>,
    },
    /// Binary operation e.g. `1 + 1` or `foo > bar`
    BinaryOp {
        left: Box<OctoSQLExpr>,
        op: BinaryOperator,
        right: Box<OctoSQLExpr>,
    },
    /// Unary operation e.g. `NOT foo`
    UnaryOp { op: UnaryOperator, expr: Box<OctoSQLExpr> },
    /// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
    Cast {
        expr: Box<OctoSQLExpr>,
        data_type: DataType,
    },
    Extract {
        field: DateTimeField,
        expr: Box<OctoSQLExpr>,
    },
    /// `expr COLLATE collation`
    Collate {
        expr: Box<OctoSQLExpr>,
        collation: ObjectName,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<OctoSQLExpr>),
    /// A literal value, such as string, number, date or NULL
    Value(Value),
    /// A constant of form `<data_type> 'value'`.
    /// This can represent ANSI SQL `DATE`, `TIME`, and `TIMESTAMP` literals (such as `DATE '2020-01-01'`),
    /// as well as constants of other types (a non-standard PostgreSQL extension).
    TypedString { data_type: DataType, value: String },
    /// Scalar function call e.g. `LEFT(foo, 5)`
    Function(Function),
    /// `CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END`
    ///
    /// Note we only recognize a complete single expression as `<condition>`,
    /// not `< 0` nor `1, 2, 3` as allowed in a `<simple when clause>` per
    /// <https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause>
    Case {
        operand: Option<Box<OctoSQLExpr>>,
        conditions: Vec<OctoSQLExpr>,
        results: Vec<OctoSQLExpr>,
        else_result: Option<Box<OctoSQLExpr>>,
    },
    /// An exists expression `EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE EXISTS (SELECT ...)`.
    Exists(Box<Query>),
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<OctoSQLQuery>),
    /// The `LISTAGG` function `SELECT LISTAGG(...) WITHIN GROUP (ORDER BY ...)`
    ListAgg(ListAgg),
}

pub struct OctoSQLParser {
    parser: Parser,
}

impl OctoSQLParser {
    /// Parse the specified tokens
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize()?;
        Ok(OctoSQLParser {
            parser: Parser::new(tokens),
        })
    }

    /// Parse a SQL statement and produce an Abstract Syntax Tree (AST)
    pub fn parse_sql(_dialect: &dyn Dialect, sql: &str) -> Result<Vec<OctoSQLStatement>, ParserError> {
        let mut parser = OctoSQLParser::new(sql)?;
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Parse a single top-level statement (such as SELECT, INSERT, CREATE, etc.),
    /// stopping before the statement separator, if any.
    pub fn parse_statement(&mut self) -> Result<OctoSQLStatement, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::SELECT | Keyword::WITH => {
                    self.parser.prev_token();
                    Ok(OctoSQLStatement::Query(Box::new(self.parse_query()?)))
                }
                _ => self.expected("a SQL query", Token::Word(w)),
            },
            Token::LParen => {
                self.parser.prev_token();
                Ok(OctoSQLStatement::Query(Box::new(self.parse_query()?)))
            }
            unexpected => self.expected("a SQL statement", unexpected),
        }
    }

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceeded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    pub fn parse_query(&mut self) -> Result<OctoSQLQuery, ParserError> {
        let ctes = if self.parser.parse_keyword(Keyword::WITH) {
            // TODO: optional RECURSIVE
            self.parse_comma_separated(OctoSQLParser::parse_cte)?
        } else {
            vec![]
        };

        let body = self.parse_query_body(0)?;

        let order_by = if self.parser.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            self.parser.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            vec![]
        };

        let limit = if self.parser.parse_keyword(Keyword::LIMIT) {
            self.parser.parse_limit()?
        } else {
            None
        };

        let offset = if self.parser.parse_keyword(Keyword::OFFSET) {
            Some(self.parser.parse_offset()?)
        } else {
            None
        };

        let fetch = if self.parser.parse_keyword(Keyword::FETCH) {
            Some(self.parser.parse_fetch()?)
        } else {
            None
        };

        Ok(OctoSQLQuery {
            ctes,
            body,
            limit,
            order_by,
            offset,
            fetch,
        })
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    pub fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>, ParserError>
        where
            F: FnMut(&mut OctoSQLParser) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if !self.parser.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(values)
    }

    /// Parse a CTE (`alias [( col1, col2, ... )] AS (subquery)`)
    fn parse_cte(&mut self) -> Result<Cte, ParserError> {
        let alias = TableAlias {
            name: self.parser.parse_identifier()?,
            columns: self.parser.parse_parenthesized_column_list(Optional)?,
        };
        self.parser.expect_keyword(Keyword::AS)?;
        self.parser.expect_token(&Token::LParen)?;
        let query = self.parser.parse_query()?;
        self.parser.expect_token(&Token::RParen)?;
        Ok(Cte { alias, query })
    }

    /// Parse a "query body", which is an expression with roughly the
    /// following grammar:
    /// ```text
    ///   query_body ::= restricted_select | '(' subquery ')' | set_operation
    ///   restricted_select ::= 'SELECT' [expr_list] [ from ] [ where ] [ groupby_having ]
    ///   subquery ::= query_body [ order_by_limit ]
    ///   set_operation ::= query_body { 'UNION' | 'EXCEPT' | 'INTERSECT' } [ 'ALL' ] query_body
    /// ```
    fn parse_query_body(&mut self, precedence: u8) -> Result<OctoSQLSetExpr, ParserError> {
        // We parse the expression using a Pratt parser, as in `parse_expr()`.
        // Start by parsing a restricted SELECT or a `(subquery)`:
        let mut expr = if self.parser.parse_keyword(Keyword::SELECT) {
            OctoSQLSetExpr::Select(Box::new(self.parse_select()?))
        } else if self.parser.consume_token(&Token::LParen) {
            // CTEs are not allowed here, but the parser currently accepts them
            let subquery = self.parser.parse_query()?;
            self.parser.expect_token(&Token::RParen)?;
            OctoSQLSetExpr::Query(Box::new(subquery))
        } else if self.parser.parse_keyword(Keyword::VALUES) {
            OctoSQLSetExpr::Values(self.parser.parse_values()?)
        } else {
            return self.expected(
                "SELECT, VALUES, or a subquery in the query body",
                self.parser.peek_token(),
            );
        };

        loop {
            // The query can be optionally followed by a set operator:
            let op = self.parse_set_operator(&self.parser.peek_token());
            let next_precedence = match op {
                // UNION and EXCEPT have the same binding power and evaluate left-to-right
                Some(SetOperator::Union) | Some(SetOperator::Except) => 10,
                // INTERSECT has higher precedence than UNION/EXCEPT
                Some(SetOperator::Intersect) => 20,
                // Unexpected token or EOF => stop parsing the query body
                None => break,
            };
            if precedence >= next_precedence {
                break;
            }
            self.parser.next_token(); // skip past the set operator
            expr = OctoSQLSetExpr::SetOperation {
                left: Box::new(expr),
                op: op.unwrap(),
                all: self.parser.parse_keyword(Keyword::ALL),
                right: Box::new(self.parse_query_body(next_precedence)?),
            };
        }

        Ok(expr)
    }

    fn parse_set_operator(&mut self, token: &Token) -> Option<SetOperator> {
        match token {
            Token::Word(w) if w.keyword == Keyword::UNION => Some(SetOperator::Union),
            Token::Word(w) if w.keyword == Keyword::EXCEPT => Some(SetOperator::Except),
            Token::Word(w) if w.keyword == Keyword::INTERSECT => Some(SetOperator::Intersect),
            _ => None,
        }
    }

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`),
    /// assuming the initial `SELECT` was already consumed
    pub fn parse_select(&mut self) -> Result<OctoSQLSelect, ParserError> {
        let distinct = self.parser.parse_all_or_distinct()?;

        let top = if self.parser.parse_keyword(Keyword::TOP) {
            Some(self.parser.parse_top()?)
        } else {
            None
        };

        let projection = self.parser.parse_comma_separated(Parser::parse_select_item)?;

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_COLUMN_ALIAS` / `RESERVED_FOR_TABLE_ALIAS`,
        // otherwise they may be parsed as an alias as part of the `projection`
        // or `from`.

        let from = if self.parser.parse_keyword(Keyword::FROM) {
            self.parser.parse_comma_separated(Parser::parse_table_and_joins)?
        } else {
            vec![]
        };

        let selection = if self.parser.parse_keyword(Keyword::WHERE) {
            Some(self.parser.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.parser.parse_keywords(&[Keyword::GROUP, Keyword::BY]) {
            self.parser.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let having = if self.parser.parse_keyword(Keyword::HAVING) {
            Some(self.parser.parse_expr()?)
        } else {
            None
        };

        Ok(OctoSQLSelect {
            distinct,
            top,
            projection,
            from,
            selection,
            group_by,
            having,
        })
    }

    fn expected<T>(&self, expected: &str, found: Token) -> Result<T, ParserError> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }
}
