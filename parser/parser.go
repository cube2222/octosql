package parser

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/parser/sqlparser"
)

func ParseUnion(statement *sqlparser.Union) (logical.Node, error) {
	var err error
	var root logical.Node

	if statement.OrderBy != nil {
		return nil, errors.Errorf("order by is currently unsupported, got %+v", statement)
	}

	firstNode, _, err := ParseNode(statement.Left)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse first select expression")
	}

	secondNode, _, err := ParseNode(statement.Right)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse second select expression")
	}
	switch statement.Type {
	case sqlparser.UnionAllStr:
		root = logical.NewUnionAll(firstNode, secondNode)

	case sqlparser.UnionDistinctStr, sqlparser.UnionStr:
		root = logical.NewUnionDistinct(firstNode, secondNode)

	default:
		return nil, errors.Errorf("unsupported union %+v of type %v", statement, statement.Type)
	}

	return root, nil
}

func ParseSelect(statement *sqlparser.Select) (logical.Node, *logical.OutputOptions, error) {
	var err error
	var root logical.Node
	var outputOptions logical.OutputOptions

	if len(statement.From) != 1 {
		return nil, nil, errors.Errorf("currently only one expression in from supported, got %v", len(statement.From))
	}

	root, err = ParseTableExpression(statement.From[0], true)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't parse from expression")
	}

	// If we get a join we want to parse triggers for it. It is done here, because otherwise passing statement.Triggers
	// would have to get to like 4 functions, which is a bit of a pain, since the type check here.
	if joinRoot, ok := root.(*logical.Join); ok {
		triggers := make([]logical.Trigger, len(statement.Trigger))
		for i := range statement.Trigger {
			triggers[i], err = ParseTrigger(statement.Trigger[i])
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't parse trigger with index %d", i)
			}
		}

		root = joinRoot.WithTriggers(triggers)
	}

	// Separate star expressions so we can put them at last positions
	nonStarExpressions := make([]sqlparser.SelectExpr, 0)
	starExpressions := make([]sqlparser.SelectExpr, 0)

	for i := range statement.SelectExprs {
		if _, ok := statement.SelectExprs[i].(*sqlparser.StarExpr); ok {
			starExpressions = append(starExpressions, statement.SelectExprs[i])
		} else {
			nonStarExpressions = append(nonStarExpressions, statement.SelectExprs[i])
		}
	}

	// We want to have normal expressions first, star expressions later
	statement.SelectExprs = append(nonStarExpressions, starExpressions...)

	// A WHERE clause needs to have access to those variables, so this map comes first, keeping the old variables.
	expressions := make([]logical.NamedExpression, len(statement.SelectExprs))
	aggregateStars := make([]bool, len(statement.SelectExprs))
	aggregates := make([]logical.Aggregate, len(statement.SelectExprs))
	aggregatesAs := make([]octosql.VariableName, len(statement.SelectExprs))
	aggregating := false

	if len(statement.SelectExprs) >= 1 {
		for i := range statement.SelectExprs {
			if starExpr, ok := statement.SelectExprs[i].(*sqlparser.StarExpr); ok {
				expressions[i], err = ParseStarExpression(starExpr)
				if err != nil { // just in case ParseStarExpression changes in the future
					return nil, nil, errors.Wrap(err, "couldn't parse star expression")
				}

				continue
			}

			aliasedExpression, ok := statement.SelectExprs[i].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, nil, errors.Errorf("expected aliased expression in select on index %v, got %v %v",
					i, statement.SelectExprs[i], reflect.TypeOf(statement.SelectExprs[i]))
			}

			// Try to parse this as an aggregate expression.
			aggregates[i], expressions[i], err = ParseAggregate(aliasedExpression.Expr)
			if err == nil {
				aggregating = true
				if expressions[i] == nil {
					aggregateStars[i] = true
				}
				aggregatesAs[i] = octosql.NewVariableName(aliasedExpression.As.String())
				continue
			}
			if errors.Cause(err) != ErrNotAggregate {
				return nil, nil, errors.Wrapf(err, "couldn't parse aggregate with index %d", i)
			}

			// If this isn't an aggregate expression,
			// then we parse it as a normal select expression.

			expressions[i], err = ParseAliasedExpression(aliasedExpression)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't parse aliased expression with index %d", i)
			}
		}

		filteredExpressions := make([]logical.NamedExpression, 0, len(expressions))
		// Filter out the stars, keep is true, so all values will stay anyways
		for i := range expressions {
			if expressions[i] != nil {
				if _, ok := expressions[i].(*logical.StarExpression); !ok {
					filteredExpressions = append(filteredExpressions, expressions[i])
				}
			}
		}

		root = logical.NewMap(filteredExpressions, root, true)
	}

	if statement.Where != nil {
		filterFormula, err := ParseLogic(statement.Where.Expr)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't parse where expression")
		}
		root = logical.NewFilter(filterFormula, root)
	}

	if statement.GroupBy != nil {
		aggregating = true
	}

	if aggregating {
		key := make([]logical.Expression, len(statement.GroupBy))
		for i := range statement.GroupBy {
			key[i], err = ParseExpression(statement.GroupBy[i])
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't parse group key expression with index %v", i)
			}
		}
		if len(key) == 0 {
			key = []logical.Expression{logical.NewConstant(true)}
		}

		fields := make([]octosql.VariableName, len(expressions))
		for i := range expressions {
			if !aggregateStars[i] {
				fields[i] = expressions[i].Name()
			} else {
				fields[i] = octosql.StarExpressionName
			}
		}

		// If the user doesn't specify an aggregate, we default to the FIRST aggregate, or KEY if it's a part of the key.
		// However, we don't want to change the name of that field.
		for i := range aggregates {
			if len(aggregates[i]) == 0 {
				aggregates[i] = logical.First
				for j := range key {
					if namedKeyElement, ok := key[j].(logical.NamedExpression); ok {
						if namedKeyElement.Name().Equal(fields[i]) {
							aggregates[i] = logical.Key
						}
					}
				}
				aggregatesAs[i] = expressions[i].Name()
			}
		}

		triggers := make([]logical.Trigger, len(statement.Trigger))
		for i := range statement.Trigger {
			triggers[i], err = ParseTrigger(statement.Trigger[i])
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't parse trigger with index %d", i)
			}
		}

		root = logical.NewGroupBy(root, key, fields, aggregates, aggregatesAs, triggers)
	}

	if statement.OrderBy != nil {
		orderByExpressions, orderByDirections, err := parseOrderByExpressions(statement.OrderBy)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't parse arguments of order by")
		}

		outputOptions.OrderByDirections = orderByDirections
		outputOptions.OrderByExpressions = orderByExpressions
	}

	// Now we only keep the selected variables.
	if len(statement.SelectExprs) >= 1 {
		nameExpressions := make([]logical.NamedExpression, len(nonStarExpressions))

		// Handle non star expressions
		for i := range nonStarExpressions {
			if !aggregating {
				nameExpressions[i] = logical.NewVariable(expressions[i].Name())
			} else {
				if len(aggregatesAs[i]) > 0 {
					nameExpressions[i] = logical.NewVariable(aggregatesAs[i])
				} else {
					if !aggregateStars[i] {
						nameExpressions[i] = logical.NewVariable(octosql.NewVariableName(fmt.Sprintf("%v_%v", expressions[i].Name(), aggregates[i])))
					} else {
						nameExpressions[i] = logical.NewVariable(octosql.NewVariableName(fmt.Sprintf("%v_%v", octosql.StarExpressionName, aggregates[i])))
					}
				}
			}

			// We check for duplicated columns
			for j := 0; j < i; j++ {
				if nameExpressions[j].Name() == nameExpressions[i].Name() {
					return nil, nil, errors.New("select expressions contain duplicated columns")
				}
			}
		}

		// Handle star expressions collisions
		for i := len(nonStarExpressions); i < len(statement.SelectExprs); i++ {
			qualifier := expressions[i].Name().Source()

			// TODO: Actually it's not that simple, because "SELECT *, p.name as mename FROM people p" is legit
			if qualifier == "" { // a * expression collides with anything else
				if len(statement.SelectExprs) > 1 {
					return nil, nil, errors.New("select expressions contain a non-qualified star expression and some other fields")
				}
			} else {
				for j := 0; j < i; j++ { // a qualifier.* collides with any other qualifier.*something* field
					if expressions[j].Name().Source() == qualifier {
						return nil, nil, errors.Errorf("a star expression with qualifier %v collides with %v", qualifier, expressions[j].Name())
					}
				}
			}

			nameExpressions = append(nameExpressions, expressions[i])
		}

		root = logical.NewMap(nameExpressions, root, false)
	}

	if len(statement.Distinct) > 0 {
		root = logical.NewDistinct(root)
	}

	if statement.Limit != nil {
		limitExpr, offsetExpr, err := parseTwoSubexpressions(statement.Limit.Rowcount, statement.Limit.Offset)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't parse limit/offset clause subexpression")
		}

		if limitExpr != nil {
			outputOptions.Limit = limitExpr
		}
		if offsetExpr != nil {
			outputOptions.Offset = offsetExpr
		}
	}

	return root, &outputOptions, nil
}

func ParseWith(statement *sqlparser.With) (logical.Node, *logical.OutputOptions, error) {
	source, outputOptions, err := ParseNode(statement.Select)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't parse underlying select in WITH statement")
	}

	nodes := make([]logical.Node, len(statement.CommonTableExpressions))
	names := make([]string, len(statement.CommonTableExpressions))
	for i, cte := range statement.CommonTableExpressions {
		node, _, err := ParseNode(cte.Select)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't parse common table expression %s with index %d", cte.Name, i)
		}
		nodes[i] = node
		names[i] = cte.Name.String()
	}

	return logical.NewWith(names, nodes, source), outputOptions, nil
}

func ParseNode(statement sqlparser.SelectStatement) (logical.Node, *logical.OutputOptions, error) {
	switch statement := statement.(type) {
	case *sqlparser.Select:
		return ParseSelect(statement)

	case *sqlparser.Union:
		plan, err := ParseUnion(statement)
		return plan, &logical.OutputOptions{}, err

	case *sqlparser.ParenSelect:
		return ParseNode(statement.Select)

	case *sqlparser.With:
		return ParseWith(statement)

	default:
		return nil, nil, errors.Errorf("unsupported select %+v of type %v", statement, reflect.TypeOf(statement))
	}
}

func ParseTableExpression(expr sqlparser.TableExpr, mustBeAliased bool) (logical.Node, error) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return ParseAliasedTableExpression(expr, mustBeAliased)
	case *sqlparser.JoinTableExpr:
		return ParseJoinTableExpression(expr)
	case *sqlparser.ParenTableExpr:
		return ParseTableExpression(expr.Exprs[0], mustBeAliased)
	case *sqlparser.TableValuedFunction:
		return ParseTableValuedFunction(expr)
	default:
		return nil, errors.Errorf("invalid table expression %+v of type %v", expr, reflect.TypeOf(expr))
	}
}

func ParseAliasedTableExpression(expr *sqlparser.AliasedTableExpr, mustBeAliased bool) (logical.Node, error) {
	switch subExpr := expr.Expr.(type) {
	case sqlparser.TableName:
		if expr.As.IsEmpty() && mustBeAliased {
			return nil, errors.Errorf("table \"%v\" must have unique alias", subExpr.Name)
		}
		return logical.NewDataSource(subExpr.Name.String(), expr.As.String()), nil

	case *sqlparser.Subquery:
		subQuery, _, err := ParseNode(subExpr.Select)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse subquery")
		}
		return logical.NewRequalifier(expr.As.String(), subQuery), nil

	default:
		return nil, errors.Errorf("invalid aliased table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	}
}

func ParseJoinTableExpression(expr *sqlparser.JoinTableExpr) (logical.Node, error) {
	leftTable, err := ParseTableExpression(expr.LeftExpr, true)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse join left table expression")
	}
	rightTable, err := ParseTableExpression(expr.RightExpr, true)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse join right table expression")
	}

	var source, joined logical.Node
	switch expr.Join {
	case sqlparser.LeftJoinStr, sqlparser.JoinStr:
		source = leftTable
		joined = rightTable
	case sqlparser.RightJoinStr:
		source = rightTable
		joined = leftTable
	default:
		return nil, errors.Errorf("invalid join expression: %v", expr.Join)
	}

	if expr.Condition.On != nil {
		condition, err := ParseLogic(expr.Condition.On)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse ON condition in join")
		}

		joined = logical.NewFilter(condition, joined)
	}

	switch expr.Join {
	case sqlparser.LeftJoinStr, sqlparser.RightJoinStr:
		return logical.NewJoin(source, joined, execution.LEFT_JOIN), nil
	case sqlparser.JoinStr:
		return logical.NewJoin(source, joined, execution.INNER_JOIN), nil
	default:
		return nil, errors.Errorf("invalid join expression: %v", expr.Join)
	}
}

func ParseTableValuedFunction(expr *sqlparser.TableValuedFunction) (logical.Node, error) {
	name := expr.Name.String()
	arguments := make(map[octosql.VariableName]logical.TableValuedFunctionArgumentValue)
	for i := range expr.Args {
		parsed, err := ParseTableValuedFunctionArgument(expr.Args[i].Value)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't parse table valued function argument \"%v\" with index %v", expr.Args[i].Name.String(), i)
		}
		arguments[octosql.NewVariableName(expr.Args[i].Name.String())] = parsed
	}

	return logical.NewRequalifier(
		expr.As.String(),
		logical.NewTableValuedFunction(name, arguments),
	), nil
}

func ParseTableValuedFunctionArgument(expr sqlparser.TableValuedFunctionArgumentValue) (logical.TableValuedFunctionArgumentValue, error) {
	switch expr := expr.(type) {
	case *sqlparser.ExprTableValuedFunctionArgumentValue:
		parsed, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't parse table valued function argument expression \"%v\"", expr.Expr)
		}
		return logical.NewTableValuedFunctionArgumentValueExpression(parsed), nil

	case *sqlparser.TableDescriptorTableValuedFunctionArgumentValue:
		parsed, err := ParseTableExpression(expr.Table, false)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't parse table valued function argument table expression \"%v\"", expr.Table)
		}
		return logical.NewTableValuedFunctionArgumentValueTable(parsed), nil

	case *sqlparser.FieldDescriptorTableValuedFunctionArgumentValue:
		name := expr.Field.Name.String()
		if !expr.Field.Qualifier.Name.IsEmpty() {
			name = fmt.Sprintf("%s.%s", expr.Field.Qualifier.Name.String(), name)
		}
		return logical.NewTableValuedFunctionArgumentValueDescriptor(octosql.NewVariableName(name)), nil

	default:
		return nil, errors.Errorf("invalid table valued function argument: %v", expr)
	}
}

var ErrNotAggregate = errors.New("expression is not aggregate")

func ParseAggregate(expr sqlparser.Expr) (logical.Aggregate, logical.NamedExpression, error) {
	switch expr := expr.(type) {
	case *sqlparser.FuncExpr:
		curAggregate := logical.Aggregate(strings.ToLower(expr.Name.String()))
		_, ok := logical.AggregateFunctions[curAggregate]
		if !ok {
			return "", nil, errors.Wrapf(ErrNotAggregate, "aggregate not found: %v", expr.Name)
		}

		if expr.Distinct {
			curAggregate = logical.Aggregate(fmt.Sprintf("%v_distinct", curAggregate))
			_, ok := logical.AggregateFunctions[curAggregate]
			if !ok {
				return "", nil, errors.Errorf("aggregate %v can't be used with distinct", expr.Name)
			}
		}

		var parsedArg logical.NamedExpression
		switch arg := expr.Exprs[0].(type) {
		case *sqlparser.AliasedExpr:
			var err error
			parsedArg, err = ParseAliasedExpression(arg)
			if err != nil {
				return "", nil, errors.Wrap(err, "couldn't parse aggregate argument")
			}

		case *sqlparser.StarExpr:
			parsedArg = nil

		default:
			return "", nil, errors.Errorf(
				"invalid aggregate argument expression type: %v",
				reflect.TypeOf(expr.Exprs[0]),
			)
		}

		return curAggregate, parsedArg, nil
	}

	return "", nil, errors.Wrapf(ErrNotAggregate, "invalid group by select expression type")
}

func ParseTrigger(trigger sqlparser.Trigger) (logical.Trigger, error) {
	switch trigger := trigger.(type) {
	case *sqlparser.CountingTrigger:
		countExpr, err := ParseExpression(trigger.Count)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse count expression")
		}
		return logical.NewCountingTrigger(countExpr), nil

	case *sqlparser.DelayTrigger:
		delayExpr, err := ParseExpression(trigger.Delay)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse delay expression")
		}
		return logical.NewDelayTrigger(delayExpr), nil

	case *sqlparser.WatermarkTrigger:
		return logical.NewWatermarkTrigger(), nil
	}

	return nil, errors.Errorf("invalid trigger type: %v", trigger)
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
		return nil, errors.Errorf("expressions in select statement and aggregate expressions must be named")
	}
	return logical.NewAliasedExpression(octosql.NewVariableName(expr.As.String()), subExpr), nil
}

func ParseStarExpression(expr *sqlparser.StarExpr) (logical.NamedExpression, error) {
	return logical.NewStarExpression(expr.TableName.Name.String()), nil
}

func ParseFunctionArgument(expr *sqlparser.AliasedExpr) (logical.Expression, error) {
	subExpr, err := ParseExpression(expr.Expr)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse argument")
	}

	return subExpr, nil
}

func ParseExpression(expr sqlparser.Expr) (logical.Expression, error) {
	switch expr := expr.(type) {
	case *sqlparser.UnaryExpr:
		arg, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse left child expression")
		}

		return logical.NewFunctionExpression(expr.Operator, []logical.Expression{arg}), nil

	case *sqlparser.BinaryExpr:
		left, err := ParseExpression(expr.Left)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse left child expression")
		}

		right, err := ParseExpression(expr.Right)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse right child expression")
		}

		return logical.NewFunctionExpression(expr.Operator, []logical.Expression{left, right}), nil

	case *sqlparser.FuncExpr:
		functionName := strings.ToLower(expr.Name.String())

		arguments := make([]logical.Expression, 0)
		var logicArg logical.Expression
		var err error

		for i := range expr.Exprs {
			arg := expr.Exprs[i]

			switch arg := arg.(type) {
			case *sqlparser.AliasedExpr:
				logicArg, err = ParseFunctionArgument(arg)
				if err != nil {
					return nil, errors.Wrap(err, "couldn't parse an aliased expression argument")
				}
			default:
				return nil, errors.Errorf("Unsupported argument %v of type %v", arg, reflect.TypeOf(arg))
			}

			arguments = append(arguments, logicArg)
		}

		return logical.NewFunctionExpression(functionName, arguments), nil

	case *sqlparser.ColName:
		name := expr.Name.String()
		if !expr.Qualifier.Name.IsEmpty() {
			name = fmt.Sprintf("%s.%s", expr.Qualifier.Name.String(), name)
		}
		return logical.NewVariable(octosql.NewVariableName(name)), nil

	case *sqlparser.Subquery:
		selectExpr, ok := expr.Select.(*sqlparser.Select)
		if !ok {
			return nil, errors.Errorf("expected select statement in subquery, go %v %v",
				expr.Select, reflect.TypeOf(expr.Select))
		}
		subquery, _, err := ParseNode(selectExpr)
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

	case sqlparser.ValTuple:
		if len(expr) == 1 {
			return ParseExpression(expr[0])
		}
		expressions := make([]logical.Expression, len(expr))
		for i := range expr {
			subExpr, err := ParseExpression(expr[i])
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't parse tuple subexpression with index %v", i)
			}

			expressions[i] = subExpr
		}
		return logical.NewTuple(expressions), nil

	case *sqlparser.IntervalExpr:
		subExpr, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse expression in interval")
		}

		return logical.NewInterval(
			subExpr,
			logical.NewConstant(strings.TrimSuffix(strings.ToLower(expr.Unit), "s")),
		), nil

	case *sqlparser.AndExpr:
		return ParseLogicExpression(expr)
	case *sqlparser.OrExpr:
		return ParseLogicExpression(expr)
	case *sqlparser.NotExpr:
		return ParseLogicExpression(expr)
	case *sqlparser.ComparisonExpr:
		return ParseLogicExpression(expr)
	case *sqlparser.ParenExpr:
		return ParseExpression(expr.Expr)

	default:
		return nil, errors.Errorf("unsupported expression %+v of type %v", expr, reflect.TypeOf(expr))
	}
}

func ParseLogicExpression(expr sqlparser.Expr) (*logical.LogicExpression, error) {
	formula, err := ParseLogic(expr)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse logic formula")
	}

	return logical.NewLogicExpression(formula), nil
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

func parseOrderByExpressions(orderBy sqlparser.OrderBy) ([]logical.Expression, []logical.OrderDirection, error) {
	expressions := make([]logical.Expression, len(orderBy))
	directions := make([]logical.OrderDirection, len(orderBy))

	for i, field := range orderBy {
		expr, err := ParseExpression(field.Expr)
		if err != nil {
			return nil, nil, errors.Errorf("couldn't parse order by expression with index %v", i)
		}

		expressions[i] = expr
		directions[i] = logical.OrderDirection(field.Direction)
	}

	return expressions, directions, nil
}

func parseTwoSubexpressions(limit, offset sqlparser.Expr) (logical.Expression, logical.Expression, error) {
	/* 	to be strict neither LIMIT nor OFFSET is in SQL standard...
	*	parser doesn't support OFFSET clause without LIMIT clause - Google BigQuery syntax
	*	TODO (?): add support of OFFSET clause without LIMIT clause to parser:
	*	just append to limit_opt in sqlparser/sql.y clause:
	*		| OFFSET expression
	*		  {
	*			$$ = &Limit{Offset: $2}
	*		  }
	 */
	var limitExpr, offsetExpr logical.Expression = nil, nil
	var err error

	if limit != nil {
		limitExpr, err = ParseExpression(limit)
		if err != nil {
			return nil, nil, errors.Errorf("couldn't parse limit's Rowcount subexpression")
		}
	}

	if offset != nil {
		offsetExpr, err = ParseExpression(offset)
		if err != nil {
			return nil, nil, errors.Errorf("couldn't parse limit's Offset subexpression")
		}
	}

	return limitExpr, offsetExpr, nil
}
