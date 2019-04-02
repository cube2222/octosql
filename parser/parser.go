package parser

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/logical"
	"github.com/pkg/errors"
	"github.com/xwb1989/sqlparser"
)

// TODO: W sumie to jeszcze moze byc "boolean node expression" chociaz oczywiscie dziala przez (costam) = TRUE

func ParseUnionAll(statement *sqlparser.Union) (logical.Node, error) {
	switch statement.Type {
	case sqlparser.UnionAllStr:
		var err error

		if statement.Limit != nil {
			// BTW parser doesn't allow neither (OFFSET without LIMIT) nor (LIMIT ALL)
			var limitExpr, offsetExpr logical.Expression = logical.NewConstant(nil), logical.NewConstant(nil)

			if statement.Limit.Rowcount != nil {
				limitExpr, err = ParseExpression(statement.Limit.Rowcount)
				if err != nil {
					return nil, errors.Errorf("couldn't parse limit's Rowcount subexpression")
				}
			}

			if statement.Limit.Offset != nil {
				offsetExpr, err = ParseExpression(statement.Limit.Offset)
				if err != nil {
					return nil, errors.Errorf("couldn't parse limit's Offset subexpression")
				}
			}
			// I am *NOT* sure whether I can set statement.Limit = nil and thus make it unusable for any further operations
			// (always could back it up and restore before return, though)
			statement.Limit = nil
			node, err := ParseUnionAll(statement)
			if err != nil {
				return nil, errors.Errorf("couldn't parse limit's underlying node")
			}

			return logical.NewLimit(node, limitExpr, offsetExpr), nil
		}
		if statement.OrderBy != nil {
			return nil, errors.Errorf("order by is currently unsupported, got %+v", statement)
		}

		firstNode, err := ParseNode(statement.Left)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse first select expression")
		}

		secondNode, err := ParseNode(statement.Right)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse second select expression")
		}

		return logical.NewUnionAll(firstNode, secondNode), nil

	default:
		return nil, errors.Errorf("unsupported union %+v of type %v", statement, statement.Type)
	}
}

func ParseSelect(statement *sqlparser.Select) (logical.Node, error) {
	var err error
	var root logical.Node

	if statement.Limit != nil {
		// BTW parser doesn't allow neither (OFFSET without LIMIT) nor (LIMIT ALL)
		var limitExpr, offsetExpr logical.Expression = logical.NewConstant(nil), logical.NewConstant(nil)

		if statement.Limit.Rowcount != nil {
			limitExpr, err = ParseExpression(statement.Limit.Rowcount)
			if err != nil {
				return nil, errors.Errorf("couldn't parse limit's Rowcount subexpression")
			}
		}

		if statement.Limit.Offset != nil {
			offsetExpr, err = ParseExpression(statement.Limit.Offset)
			if err != nil {
				return nil, errors.Errorf("couldn't parse limit's Offset subexpression")
			}
		}
		// I am *NOT* sure whether I can set statement.Limit = nil and thus make it unusable for any further operations
		// (always could back it up and restore before return, though)
		statement.Limit = nil
		node, err := ParseSelect(statement)
		if err != nil {
			return nil, errors.Errorf("couldn't parse limit's underlying node")
		}

		return logical.NewLimit(node, limitExpr, offsetExpr), nil
	}

	if len(statement.From) != 1 {
		return nil, errors.Errorf("currently only one expression in from supported, got %v", len(statement.From))
	}

	aliasedTableFrom, ok := statement.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, errors.Errorf("expected aliased table expression in from, got %v %v",
			statement.From[0], reflect.TypeOf(statement.From[0]))
	}

	root, err = ParseTableExpression(aliasedTableFrom)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse from expression")
	}

	if statement.Where != nil {
		filterFormula, err := ParseLogic(statement.Where.Expr)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse where expression")
		}
		root = logical.NewFilter(filterFormula, root)
	}

	if len(statement.SelectExprs) == 1 {
		if _, ok := statement.SelectExprs[0].(*sqlparser.StarExpr); ok {
			return root, nil
		}
	}

	expressions := make([]logical.NamedExpression, len(statement.SelectExprs))
	for i := range statement.SelectExprs {
		aliasedExpression, ok := statement.SelectExprs[i].(*sqlparser.AliasedExpr)
		if !ok {
			return nil, errors.Errorf("expected aliased expression in select on index %v, got %v %v",
				i, statement.SelectExprs[i], reflect.TypeOf(statement.SelectExprs[i]))
		}

		expressions[i], err = ParseAliasedExpression(aliasedExpression)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't parse aliased expression with index %d", i)
		}
	}

	return logical.NewMap(expressions, root), nil
}

func ParseNode(statement sqlparser.SelectStatement) (logical.Node, error) {
	switch statement := statement.(type) {
	case *sqlparser.Select:
		return ParseSelect(statement)

	case *sqlparser.Union:
		return ParseUnionAll(statement)

	case *sqlparser.ParenSelect:
		return ParseNode(statement.Select)

	default:
		// Union
		return nil, errors.Errorf("unsupported select %+v of type %v", statement, reflect.TypeOf(statement))
	}
}

func ParseTableExpression(expr *sqlparser.AliasedTableExpr) (logical.Node, error) {
	switch subExpr := expr.Expr.(type) {
	case sqlparser.TableName:
		return logical.NewDataSource(subExpr.Name.String(), expr.As.String()), nil

	case *sqlparser.Subquery:
		subQuery, err := ParseNode(subExpr.Select)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse subquery")
		}
		return logical.NewRequalifier(expr.As.String(), subQuery), nil

	default:
		return nil, errors.Errorf("invalid table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	}
}

func ParseAliasedExpression(expr *sqlparser.AliasedExpr) (logical.NamedExpression, error) {
	subExpr, err := ParseExpression(expr.Expr)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse aliased expression: %+v", expr.Expr)
	}
	if expr.As.String() == "" {
		if named, ok := subExpr.(logical.NamedExpression); ok {
			return named, nil
		}
		return nil, errors.Wrap(err, "expressions in select statement must be named")
	}
	return logical.NewAliasedExpression(octosql.VariableName(expr.As.String()), subExpr), nil
}

func ParseExpression(expr sqlparser.Expr) (logical.Expression, error) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		name := expr.Name.String()
		if !expr.Qualifier.Name.IsEmpty() {
			name = fmt.Sprintf("%s.%s", expr.Qualifier.Name.String(), name)
		}
		return logical.NewVariable(octosql.VariableName(name)), nil

	case *sqlparser.Subquery:
		selectExpr, ok := expr.Select.(*sqlparser.Select)
		if !ok {
			return nil, errors.Errorf("expected select statement in subquery, go %v %v",
				expr.Select, reflect.TypeOf(expr.Select))
		}
		subquery, err := ParseNode(selectExpr)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse select expression")
		}
		return logical.NewNodeExpression(subquery), nil

	case *sqlparser.SQLVal:
		var value interface{}
		var err error
		switch expr.Type {
		case sqlparser.IntVal:
			var i int64
			i, err = strconv.ParseInt(string(expr.Val), 10, 64)
			value = int(i)
		case sqlparser.FloatVal:
			value, err = strconv.ParseFloat(string(expr.Val), 64)
		case sqlparser.StrVal:
			value = string(expr.Val)
		default:
			err = errors.Errorf("constant value type unsupported")
		}
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't parse constant %s", expr.Val)
		}
		return logical.NewConstant(value), nil

	case *sqlparser.NullVal:
		return logical.NewConstant(nil), nil

	case sqlparser.BoolVal:
		return logical.NewConstant(expr), nil

	default:
		return nil, errors.Errorf("unsupported expression %+v of type %v", expr, reflect.TypeOf(expr))
	}
}

func ParseLogic(expr sqlparser.Expr) (logical.Formula, error) {
	switch expr := expr.(type) {
	case sqlparser.BoolVal:
		return logical.NewBooleanConstant(bool(expr)), nil
	case *sqlparser.AndExpr:
		return ParseInfixOperator(expr.Left, expr.Right, "AND")
	case *sqlparser.OrExpr:
		return ParseInfixOperator(expr.Left, expr.Right, "OR")
	case *sqlparser.NotExpr:
		return ParsePrefixOperator(expr.Expr, "NOT")
	case *sqlparser.ComparisonExpr:
		return ParseInfixComparison(expr.Left, expr.Right, expr.Operator)
	case *sqlparser.ParenExpr:
		return ParseLogic(expr.Expr)
	default:
		return nil, errors.Errorf("unsupported logic expression %+v of type %v", expr, reflect.TypeOf(expr))
	}
}

func ParseInfixOperator(left, right sqlparser.Expr, operator string) (logical.Formula, error) {
	leftParsed, err := ParseLogic(left)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse left hand side of %s operator %+v", operator, left)
	}
	rightParsed, err := ParseLogic(right)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse right hand side of %s operator %+v", operator, right)
	}
	return logical.NewInfixOperator(leftParsed, rightParsed, operator), nil
}

func ParsePrefixOperator(child sqlparser.Expr, operator string) (logical.Formula, error) {
	childParsed, err := ParseLogic(child)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse child of %s operator %+v", operator, child)
	}
	return logical.NewPrefixOperator(childParsed, operator), nil
}

func ParseInfixComparison(left, right sqlparser.Expr, operator string) (logical.Formula, error) {
	leftParsed, err := ParseExpression(left)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse left hand side of %s comparator %+v", operator, left)
	}
	rightParsed, err := ParseExpression(right)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse right hand side of %s comparator %+v", operator, right)
	}
	return logical.NewPredicate(leftParsed, logical.NewRelation(operator), rightParsed), nil
}
