package parser

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/aggregates"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/parser/sqlparser"
)

// func ParseUnion(statement *sqlparser.Union) (logical.Node, error) {
// 	var err error
// 	var root logical.Node
//
// 	if statement.OrderBy != nil {
// 		return nil, errors.Errorf("order by is currently unsupported, got %+v", statement)
// 	}
//
// 	firstNode, _, err := ParseNode(statement.Left)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "couldn't parse first select expression")
// 	}
//
// 	secondNode, _, err := ParseNode(statement.Right)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "couldn't parse second select expression")
// 	}
// 	switch statement.Type {
// 	case sqlparser.UnionAllStr:
// 		root = logical.NewUnionAll(firstNode, secondNode)
//
// 	case sqlparser.UnionDistinctStr, sqlparser.UnionStr:
// 		root = logical.NewUnionDistinct(firstNode, secondNode)
//
// 	default:
// 		return nil, errors.Errorf("unsupported union %+v of type %v", statement, statement.Type)
// 	}
//
// 	return root, nil
// }

type OutputOptions struct {
	Limit              *logical.Expression
	OrderByExpressions []logical.Expression
	OrderByDirections  []logical.OrderDirection
}

func ParseSelect(statement *sqlparser.Select) (logical.Node, *OutputOptions, error) {
	var err error
	var root logical.Node
	outputOptions := &OutputOptions{}

	root, err = ParseTableExpression(statement.From[len(statement.From)-1])
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't parse FROM expression")
	}

	for i := len(statement.From) - 2; i >= 0; i-- {
		next, err := ParseTableExpression(statement.From[i])
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't parse FROM expression with index %d", i)
		}
		root = logical.NewStreamJoin(root, next)
	}

	// We want to have normal expressions first, star expressions later

	if statement.Where != nil {
		filterFormula, err := ParseExpression(statement.Where.Expr)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't parse where expression")
		}
		root = logical.NewFilter(filterFormula, root)
	}

	isGroupBy := false
	for i := range statement.SelectExprs {
		if aliasedExpr, ok := statement.SelectExprs[i].(*sqlparser.AliasedExpr); ok {
			if isAggregateExpression(aliasedExpr.Expr) {
				isGroupBy = true
			}
		}
	}
	if isGroupBy {
		key := make([]logical.Expression, len(statement.GroupBy))
		for i := range statement.GroupBy {
			key[i], err = ParseExpression(statement.GroupBy[i])
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't parse group key expression with index %v", i)
			}
		}

		expressions := make([]logical.Expression, len(statement.SelectExprs))
		isAggregate := make([]bool, len(statement.SelectExprs))
		aggregates := make([]string, len(statement.SelectExprs))
		keyPart := make([]int, len(statement.SelectExprs))
		aliases := make([]string, len(statement.SelectExprs))
	selectExprLoop:
		for i := range statement.SelectExprs {
			inExpr := statement.SelectExprs[i].(*sqlparser.AliasedExpr).Expr
			aliases[i] = statement.SelectExprs[i].(*sqlparser.AliasedExpr).As.String()
			agg, expr, err := ParseAggregate(inExpr)
			if err == nil {
				isAggregate[i] = true
				aggregates[i] = agg
				expressions[i] = expr
				continue
			}
			expr, exprErr := ParseExpression(inExpr)
			if exprErr == nil {
				isAggregate[i] = false
				expressions[i] = expr
				for keyIndex := range key {
					if logical.EqualExpressions(expr, key[keyIndex]) {
						keyPart[i] = keyIndex
						continue selectExprLoop
					}
				}
				return nil, nil, errors.Errorf("non-aggregate expression with index %d in grouping must be part of group by key", i)
			}
			return nil, nil, errors.Errorf("couldn't parse expression as aggregate nor expression: %s %s", err, exprErr)
		}

		triggers := make([]logical.Trigger, len(statement.Trigger))
		for i := range statement.Trigger {
			triggers[i], err = ParseTrigger(statement.Trigger[i])
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't parse trigger with index %d", i)
			}
		}

		outputExprs := make([]logical.Expression, len(isAggregate))
		var nonKeyAggregates []string
		var aggregateExprs []logical.Expression
		var aggregateFieldNames []string
		keyFieldNames := make([]string, len(key))
		for i := range key {
			keyFieldNames[i] = fmt.Sprintf("key_%d", i)
		}
		nameCounter := map[string]int{}
		getUniqueName := func(name string) string {
			count, ok := nameCounter[name]
			if ok {
				name = fmt.Sprintf("%s_%d", name, count)
			}
			nameCounter[name] = count + 1
			return name
		}
		for i, ok := range isAggregate {
			if ok {
				nonKeyAggregates = append(nonKeyAggregates, aggregates[i])
				aggregateExprs = append(aggregateExprs, expressions[i])
				var name string
				if aliases[i] != "" {
					name = getUniqueName(aliases[i])
				} else if namer, ok := expressions[i].(logical.FieldNamer); ok {
					name = getUniqueName(fmt.Sprintf("%s_%s", aggregates[i], namer.FieldName()))
				} else {
					name = getUniqueName(aggregates[i])
				}
				aggregateFieldNames = append(aggregateFieldNames, name)
				outputExprs[i] = logical.NewVariable(name)
			} else {
				var name string
				if aliases[i] != "" {
					name = getUniqueName(aliases[i])
				} else if namer, ok := key[keyPart[i]].(logical.FieldNamer); ok {
					name = getUniqueName(namer.FieldName())
				} else {
					name = getUniqueName(fmt.Sprintf("key_%d", keyPart[i]))
				}
				keyFieldNames[keyPart[i]] = name
				outputExprs[i] = logical.NewVariable(name)
			}
		}

		root = logical.NewGroupBy(root, key, keyFieldNames, aggregateExprs, nonKeyAggregates, aggregateFieldNames, triggers)
		root = logical.NewMap(outputExprs, make([]string, len(outputExprs)), make([]string, len(outputExprs)), make([]bool, len(outputExprs)), make([]logical.Expression, len(outputExprs)), make([]bool, len(outputExprs)), root)
	} else {
		expressions := make([]logical.Expression, len(statement.SelectExprs))
		starQualifiers := make([]string, len(statement.SelectExprs))
		isStar := make([]bool, len(statement.SelectExprs))
		objectExplosions := make([]logical.Expression, len(statement.SelectExprs))
		isObjectExplosion := make([]bool, len(statement.SelectExprs))
		aliases := make([]string, len(statement.SelectExprs))

		for i := range statement.SelectExprs {
			if starExpr, ok := statement.SelectExprs[i].(*sqlparser.StarExpr); ok {
				starQualifiers[i] = starExpr.TableName.Name.String()
				isStar[i] = true
				continue
			}

			if objectExplosion, ok := statement.SelectExprs[i].(*sqlparser.ObjectExplode); ok {
				expr, err := ParseExpression(objectExplosion.Object)
				if err != nil {
					return nil, nil, errors.Wrapf(err, "couldn't parse object explode expression with index %d", i)
				}

				objectExplosions[i] = expr
				isObjectExplosion[i] = true
				continue
			}

			aliasedExpression, ok := statement.SelectExprs[i].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, nil, errors.Errorf("expected aliased expression in select on index %v, got %v %v",
					i, statement.SelectExprs[i], reflect.TypeOf(statement.SelectExprs[i]))
			}

			expressions[i], aliases[i], err = ParseAliasedExpression(aliasedExpression)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't parse aliased expression with index %d", i)
			}
		}

		if !(len(expressions) == 1 && isStar[0] && starQualifiers[0] == "") {
			// Only create a map node if this is not 'SELECT * FROM xyz'
			root = logical.NewMap(expressions, aliases, starQualifiers, isStar, objectExplosions, isObjectExplosion, root)
		}
	}

	if len(statement.Distinct) > 0 {
		root = logical.NewDistinct(root)
	}

	if statement.OrderBy != nil {
		orderByExpressions, orderByDirections, err := parseOrderByExpressions(statement.OrderBy)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't parse keys of order by")
		}

		outputOptions.OrderByExpressions = orderByExpressions
		outputOptions.OrderByDirections = orderByDirections
	}

	if statement.Limit != nil {
		limitExpr, err := ParseExpression(statement.Limit.Rowcount)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't parse limit")
		}
		outputOptions.Limit = &limitExpr
	}

	return root, outputOptions, nil
}

func ParseWith(statement *sqlparser.With) (logical.Node, *OutputOptions, error) {
	source, outputOptions, err := ParseNode(statement.Select)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't parse underlying select in WITH statement")
	}

	nodes := make([]logical.Node, len(statement.CommonTableExpressions))
	names := make([]string, len(statement.CommonTableExpressions))
	for i, cte := range statement.CommonTableExpressions {
		node, err := ParseNestedNode(cte.Select)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't parse common table expression %s with index %d", cte.Name, i)
		}
		nodes[i] = node
		names[i] = cte.Name.String()
	}

	return logical.NewWith(names, nodes, source), outputOptions, nil
}

func ParseNode(statement sqlparser.SelectStatement) (logical.Node, *OutputOptions, error) {
	switch statement := statement.(type) {
	case *sqlparser.Select:
		return ParseSelect(statement)

	// case *sqlparser.Union:
	// 	plan, err := ParseUnion(statement)
	// 	return plan, &logical.OutputOptions{}, err

	case *sqlparser.ParenSelect:
		return ParseNode(statement.Select)

	case *sqlparser.With:
		return ParseWith(statement)

	default:
		return nil, nil, errors.Errorf("unsupported select %+v of type %v", statement, reflect.TypeOf(statement))
	}
}

func ParseNestedNode(statement sqlparser.SelectStatement) (logical.Node, error) {
	node, outputOptions, err := ParseNode(statement)
	if err != nil {
		return nil, err
	}
	if len(outputOptions.OrderByExpressions) > 0 || outputOptions.Limit != nil {
		node = logical.NewOrderSensitiveTransform(outputOptions.OrderByExpressions, outputOptions.OrderByDirections, outputOptions.Limit, node)
	}
	return node, nil
}

func ParseTableExpression(expr sqlparser.TableExpr) (logical.Node, error) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return ParseAliasedTableExpression(expr)
	case *sqlparser.JoinTableExpr:
		return ParseJoinTableExpression(expr)
	case *sqlparser.ParenTableExpr:
		return ParseTableExpression(expr.Exprs[0])
	case *sqlparser.TableValuedFunction:
		return ParseTableValuedFunction(expr)
	default:
		return nil, errors.Errorf("invalid table expression %+v of type %v", expr, reflect.TypeOf(expr))
	}
}

func ParseAliasedTableExpression(expr *sqlparser.AliasedTableExpr) (logical.Node, error) {
	switch subExpr := expr.Expr.(type) {
	case sqlparser.TableName:
		name := subExpr.Name.String()
		if !subExpr.Qualifier.IsEmpty() {
			name = fmt.Sprintf("%s.%s", subExpr.Qualifier.String(), name)
		}
		options := make(map[string]string)
		if optionsStartIndex := strings.Index(name, "?"); optionsStartIndex != -1 {
			optionsString := name[optionsStartIndex+1:]
			name = name[:optionsStartIndex]
			for i, option := range strings.Split(optionsString, "&") {
				parts := strings.SplitN(option, "=", 2)
				if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
					return nil, errors.Errorf("invalid option with index %d: %s", i, option)
				}
				options[parts[0]] = parts[1]
			}
		}
		var alias string
		if !expr.As.IsEmpty() {
			alias = expr.As.String()
		} else {
			alias = strings.TrimSuffix(name, ".csv")
			alias = strings.TrimSuffix(alias, ".json")
			alias = strings.TrimSuffix(alias, ".parquet")
			if index := strings.Index(alias, "."); index != -1 {
				alias = alias[index+1:]
			}
			if index := strings.LastIndex(alias, "/"); index != -1 {
				alias = alias[index+1:]
			}
		}
		var out logical.Node = logical.NewDataSource(name, alias, options)
		return out, nil

	case *sqlparser.Subquery:
		subQuery, err := ParseNestedNode(subExpr.Select)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse subquery")
		}
		return logical.NewRequalifier(expr.As.String(), subQuery), nil

	default:
		return nil, errors.Errorf("invalid aliased table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	}
}

func ParseJoinTableExpression(expr *sqlparser.JoinTableExpr) (logical.Node, error) {
	leftTable, err := ParseTableExpression(expr.LeftExpr)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse join left table expression")
	}
	rightTable, err := ParseTableExpression(expr.RightExpr)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse join right table expression")
	}

	var joinOn *logical.Expression
	if expr.Condition.On != nil {
		predicate, err := ParseExpression(expr.Condition.On)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse ON predicate in join")
		}
		joinOn = &predicate
	}

	var node logical.Node
	if expr.Strategy == sqlparser.LookupJoinStrategy {
		switch expr.Join {
		case sqlparser.JoinStr:
			node = logical.NewLookupJoin(leftTable, rightTable)
			if joinOn != nil {
				node = logical.NewFilter(*joinOn, node)
			}
		default:
			return nil, errors.Errorf("invalid join expression: %v", expr.Join)
		}
	} else {
		switch expr.Join {
		case sqlparser.JoinStr:
			node = logical.NewStreamJoin(leftTable, rightTable)
			if joinOn != nil {
				node = logical.NewFilter(*joinOn, node)
			}
		case sqlparser.LeftJoinStr, sqlparser.RightJoinStr, sqlparser.OuterJoinStr:
			if joinOn == nil {
				return nil, errors.Errorf("outer join without ON predicate")
			}
			node = logical.NewOuterJoin(leftTable, rightTable, *joinOn, expr.Join == sqlparser.LeftJoinStr || expr.Join == sqlparser.OuterJoinStr, expr.Join == sqlparser.RightJoinStr || expr.Join == sqlparser.OuterJoinStr)
		default:
			return nil, errors.Errorf("invalid join expression: %v", expr.Join)
		}
	}

	return node, nil
}

func ParseTableValuedFunction(expr *sqlparser.TableValuedFunction) (logical.Node, error) {
	name := expr.Name.String()
	arguments := make(map[string]logical.TableValuedFunctionArgumentValue)
	for i := range expr.Args {
		parsed, err := ParseTableValuedFunctionArgument(expr.Args[i].Value)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't parse table valued function argument \"%v\" with index %v", expr.Args[i].Name.String(), i)
		}
		arguments[expr.Args[i].Name.String()] = parsed
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
		parsed, err := ParseTableExpression(expr.Table)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't parse table valued function argument table expression \"%v\"", expr.Table)
		}
		return logical.NewTableValuedFunctionArgumentValueTable(parsed), nil

	case *sqlparser.FieldDescriptorTableValuedFunctionArgumentValue:
		name := expr.Field.Name.String()
		if !expr.Field.Qualifier.Name.IsEmpty() {
			name = fmt.Sprintf("%s.%s", expr.Field.Qualifier.Name.String(), name)
		}
		return logical.NewTableValuedFunctionArgumentValueDescriptor(name), nil

	default:
		return nil, errors.Errorf("invalid table valued function argument: %v", expr)
	}
}

var ErrNotAggregate = errors.New("expression is not aggregate")

func ParseAggregate(expr sqlparser.Expr) (string, logical.Expression, error) {
	switch expr := expr.(type) {
	case *sqlparser.FuncExpr:
		curAggregate := strings.ToLower(expr.Name.String())
		if expr.Distinct {
			curAggregate = fmt.Sprintf("%v_distinct", curAggregate)
		}
		_, ok := aggregates.Aggregates[curAggregate]
		if !ok {
			return "", nil, errors.Wrapf(ErrNotAggregate, "aggregate not found: %v", expr.Name)
		}

		var parsedArg logical.Expression
		switch arg := expr.Exprs[0].(type) {
		case *sqlparser.AliasedExpr:
			var err error
			parsedArg, err = ParseExpression(arg.Expr)
			if err != nil {
				return "", nil, errors.Wrap(err, "couldn't parse aggregate argument")
			}

		case *sqlparser.StarExpr:
			parsedArg = logical.NewConstant(octosql.NewBoolean(true))

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
		c, ok := trigger.Count.(*sqlparser.SQLVal)
		if !ok {
			return nil, errors.Errorf("counting trigger parameter must be constant, is: %+v", trigger.Count)
		}
		if c.Type != sqlparser.IntVal {
			return nil, errors.Errorf("counting trigger parameter must be Int constant, is: %+v", c)
		}
		i, err := strconv.ParseInt(string(c.Val), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "counting trigger parameter must be Int constant, couldn't parse")
		}
		return logical.NewCountingTrigger(uint(i)), nil

	case *sqlparser.DelayTrigger:
		delayExpr, err := ParseExpression(trigger.Delay)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse delay expression")
		}
		return logical.NewDelayTrigger(delayExpr), nil

	case *sqlparser.WatermarkTrigger:
		return logical.NewWatermarkTrigger(), nil

	case *sqlparser.EndOfStreamTrigger:
		return logical.NewEndOfStreamTrigger(), nil
	}

	return nil, errors.Errorf("invalid trigger type: %v", trigger)
}

func ParseAliasedExpression(expr *sqlparser.AliasedExpr) (logical.Expression, string, error) {
	subExpr, err := ParseExpression(expr.Expr)
	if err != nil {
		return nil, "", errors.Wrapf(err, "couldn't parse aliased expression: %+v", expr.Expr)
	}

	return subExpr, expr.As.String(), nil
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

		if functionName == "coalesce" {
			return logical.NewCoalesce(arguments), nil
		}

		return logical.NewFunctionExpression(functionName, arguments), nil

	case *sqlparser.ColName:
		name := expr.Name.String()
		if !expr.Qualifier.Name.IsEmpty() {
			name = fmt.Sprintf("%s.%s", expr.Qualifier.Name.String(), name)
		}
		return logical.NewVariable(name), nil

	case *sqlparser.Subquery:
		selectExpr, ok := expr.Select.(*sqlparser.Select)
		if !ok {
			return nil, errors.Errorf("expected select statement in subquery, go %v %v",
				expr.Select, reflect.TypeOf(expr.Select))
		}
		subquery, err := ParseNestedNode(selectExpr)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse select expression")
		}
		return logical.NewQueryExpression(subquery), nil

	case *sqlparser.SQLVal:
		var value octosql.Value
		var err error
		switch expr.Type {
		case sqlparser.IntVal:
			var i int64
			i, err = strconv.ParseInt(string(expr.Val), 10, 64)
			value = octosql.NewInt(int(i))
		case sqlparser.FloatVal:
			var val float64
			val, err = strconv.ParseFloat(string(expr.Val), 64)
			value = octosql.NewFloat(val)
		case sqlparser.StrVal:
			value = octosql.NewString(string(expr.Val))
		default:
			err = errors.Errorf("constant value type unsupported")
		}
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't parse constant %s", expr.Val)
		}
		return logical.NewConstant(value), nil

	case *sqlparser.NullVal:
		return logical.NewConstant(octosql.NewNull()), nil

	case sqlparser.BoolVal:
		return logical.NewConstant(octosql.NewBoolean(bool(expr))), nil

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
		c, ok := expr.Expr.(*sqlparser.SQLVal)
		if !ok {
			return nil, errors.Errorf("interval expression parameter must be constant, is: %+v", expr.Expr)
		}
		if c.Type != sqlparser.IntVal {
			return nil, errors.Errorf("interval expression parameter must be Int constant, is: %+v", c)
		}
		i, err := strconv.ParseInt(string(c.Val), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "interval expression parameter must be Int constant, couldn't parse")
		}

		var unit time.Duration
		switch strings.TrimSuffix(strings.ToLower(expr.Unit), "s") {
		case "nanosecond":
			unit = time.Nanosecond
		case "microsecond":
			unit = time.Microsecond
		case "millisecond":
			unit = time.Millisecond
		case "second":
			unit = time.Second
		case "minute":
			unit = time.Minute
		case "hour":
			unit = time.Hour
		case "day":
			unit = time.Hour * 24
		default:
			return nil, errors.Errorf("invalid interval expression unit: %s, must be one of: nanosecond, microsecond, millisecond, second, minute, hour, day", expr.Unit)
		}

		return logical.NewConstant(octosql.NewDuration(time.Duration(i) * unit)), nil

	case *sqlparser.AndExpr:
		return ParseInfixOperator(expr.Left, expr.Right, "AND")
	case *sqlparser.OrExpr:
		return ParseInfixOperator(expr.Left, expr.Right, "OR")
	case *sqlparser.NotExpr:
		childParsed, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't parse child of not operator %+v", expr.Expr)
		}
		return logical.NewFunctionExpression("not", []logical.Expression{childParsed}), nil
	case *sqlparser.ComparisonExpr:
		return ParseInfixComparison(expr.Left, expr.Right, expr.Operator)
	case *sqlparser.ParenExpr:
		return ParseExpression(expr.Expr)
	case *sqlparser.IsExpr:
		arg, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse left child expression")
		}

		var funcName string
		switch expr.Operator {
		case sqlparser.IsNullStr:
			funcName = "is null"
		case sqlparser.IsNotNullStr:
			funcName = "is not null"
		default:
			return nil, errors.Errorf("unsupported IS operator: %s", expr.Operator)
		}

		return logical.NewFunctionExpression(funcName, []logical.Expression{arg}), nil
	case *sqlparser.ConvertExpr:
		arg, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse expression being cast")
		}
		targetType, err := ParseType(expr.Type)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse type to cast to")
		}

		return logical.NewTypeCast(arg, targetType), nil
	case *sqlparser.ObjectFieldAccess:
		arg, err := ParseExpression(expr.Object)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse object")
		}
		out := logical.NewObjectFieldAccess(arg, expr.Field.String())
		return out, nil
	default:
		return nil, errors.Errorf("unsupported expression %+v of type %v", expr, reflect.TypeOf(expr))
	}
}

func ParseType(t sqlparser.ConvertType) (octosql.TypeID, error) {
	switch t := t.(type) {
	case *sqlparser.ConvertTypeList:
		return octosql.TypeIDList, nil
	case *sqlparser.ConvertTypeObject:
		return octosql.TypeIDStruct, nil
	case *sqlparser.ConvertTypeSimple:
		switch tName := strings.ToLower(t.Name); tName {
		case "null":
			return octosql.TypeIDNull, nil
		case "int":
			return octosql.TypeIDInt, nil
		case "int64":
			return octosql.TypeIDInt64, nil
		case "float":
			return octosql.TypeIDFloat, nil
		case "boolean":
			return octosql.TypeIDBoolean, nil
		case "string":
			return octosql.TypeIDString, nil
		case "time":
			return octosql.TypeIDTime, nil
		case "duration":
			return octosql.TypeIDDuration, nil
		default:
			return 0, errors.Errorf("unknown type: %s", tName)
		}
	default:
		return 0, errors.Errorf("unsupported type %+v of type %v", t, reflect.TypeOf(t))
	}
}

func isAggregateExpression(expr sqlparser.Expr) bool {
	switch expr := expr.(type) {
	case *sqlparser.FuncExpr:
		functionName := strings.ToLower(expr.Name.String())

		if _, ok := aggregates.Aggregates[functionName]; ok {
			return true
		}

	case *sqlparser.ParenExpr:
		return isAggregateExpression(expr.Expr)
	}
	return false
}

func ParseInfixOperator(left, right sqlparser.Expr, operator string) (logical.Expression, error) {
	leftParsed, err := ParseExpression(left)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse left hand side of %s operator %+v", operator, left)
	}
	rightParsed, err := ParseExpression(right)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse right hand side of %s operator %+v", operator, right)
	}
	if operator == "AND" {
		return logical.NewAnd(leftParsed, rightParsed), nil
	} else if operator == "OR" {
		return logical.NewOr(leftParsed, rightParsed), nil
	} else {
		panic("invalid operator")
	}
}

func ParsePrefixOperator(child sqlparser.Expr, operator string) (logical.Expression, error) {
	childParsed, err := ParseExpression(child)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse child of %s operator %+v", operator, child)
	}
	return logical.NewFunctionExpression(operator, []logical.Expression{childParsed}), nil
}

func ParseInfixComparison(left, right sqlparser.Expr, operator string) (logical.Expression, error) {
	leftParsed, err := ParseExpression(left)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse left hand side of %s comparator %+v", operator, left)
	}
	rightParsed, err := ParseExpression(right)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse right hand side of %s comparator %+v", operator, right)
	}
	if operator == sqlparser.NotLikeStr {
		return logical.NewFunctionExpression(
			"not",
			[]logical.Expression{
				logical.NewFunctionExpression("like", []logical.Expression{leftParsed, rightParsed}),
			},
		), nil
	} else if operator == sqlparser.NotLikeRegexpStr {
		return logical.NewFunctionExpression(
			"not",
			[]logical.Expression{
				logical.NewFunctionExpression("~", []logical.Expression{leftParsed, rightParsed}),
			},
		), nil
	} else if operator == sqlparser.NotLikeRegexpCaseInsensitiveStr {
		return logical.NewFunctionExpression(
			"not",
			[]logical.Expression{
				logical.NewFunctionExpression("~*", []logical.Expression{leftParsed, rightParsed}),
			},
		), nil
	}
	return logical.NewFunctionExpression(operator, []logical.Expression{leftParsed, rightParsed}), nil
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
