package logical

import (
	"fmt"
	"log"
	"reflect"

	"github.com/pkg/errors"
)

func EqualNodes(node1, node2 Node) error {
	switch node1 := node1.(type) {
	case *With:
		if node2, ok := node2.(*With); ok {
			if len(node1.cteNames) != len(node2.cteNames) {
				return errors.Errorf("counts of cte's not equal: %d vs %d", len(node1.cteNames), len(node2.cteNames))
			}
			for i := range node1.cteNames {
				if node1.cteNames[i] != node2.cteNames[i] {
					return errors.Errorf("cte names with index %d not equal: %s vs %s", i, node1.cteNames[i], node2.cteNames[i])
				}
				if err := EqualNodes(node1.cteNodes[i], node2.cteNodes[i]); err != nil {
					return errors.Wrapf(err, "cte nodes with index %d not equal: %+v, %+v", i, node1.cteNodes[i], node2.cteNodes[i])
				}
			}
			if err := EqualNodes(node1.source, node2.source); err != nil {
				return errors.Wrapf(err, "source node not equal: %+v, %+v", node1.source, node2.source)
			}
			return nil
		}

	case *UnionAll:
		if node2, ok := node2.(*UnionAll); ok {
			if err := EqualNodes(node1.first, node2.first); err != nil {
				return errors.Wrapf(err, "first statements not equal: %+v, %+v", node1.first, node2.first)
			}
			if err := EqualNodes(node1.second, node2.second); err != nil {
				return errors.Wrapf(err, "second statements not equal: %+v, %+v", node1.second, node2.second)
			}
			return nil
		}

	case *UnionDistinct:
		if node2, ok := node2.(*UnionDistinct); ok {
			if err := EqualNodes(node1.first, node2.first); err != nil {
				return errors.Wrapf(err, "first statements not equal: %+v, %+v", node1.first, node2.first)
			}
			if err := EqualNodes(node1.second, node2.second); err != nil {
				return errors.Wrapf(err, "second statements not equal: %+v, %+v", node1.second, node2.second)
			}
			return nil
		}

	case *Map:
		if node2, ok := node2.(*Map); ok {
			if len(node1.expressions) != len(node2.expressions) {
				return fmt.Errorf("expressions count not equal: %v, %v", len(node1.expressions), len(node2.expressions))
			}
			for i := range node1.expressions {
				if err := EqualExpressions(node1.expressions[i], node2.expressions[i]); err != nil {
					return errors.Wrapf(err, "expression %v not equal", i)
				}
			}
			if err := EqualNodes(node1.source, node2.source); err != nil {
				return errors.Wrap(err, "sources not equal")
			}

			if node1.keep != node2.keep {
				return errors.New("keep values for maps are not equal")
			}

			return nil
		}

	case *Filter:
		if node2, ok := node2.(*Filter); ok {
			if err := EqualFormula(node1.formula, node2.formula); err != nil {
				return errors.Wrap(err, "formulas not equal")
			}
			if err := EqualNodes(node1.source, node2.source); err != nil {
				return errors.Wrap(err, "sources not equal")
			}
			return nil
		}

	case *Requalifier:
		if node2, ok := node2.(*Requalifier); ok {
			if node1.qualifier != node2.qualifier {
				return fmt.Errorf("qualifiers not equal: %v, %v", node1.qualifier, node2.qualifier)
			}
			if err := EqualNodes(node1.source, node2.source); err != nil {
				return errors.Wrap(err, "sources not qual")
			}
			return nil
		}

	case *DataSource:
		if node2, ok := node2.(*DataSource); ok {
			if node1.name != node2.name {
				return fmt.Errorf("names not equal: %v, %v", node1.name, node2.name)
			}
			if node1.alias != node2.alias {
				return fmt.Errorf("aliases not equal: %v, %v", node1.alias, node2.alias)
			}
			return nil
		}

	case *Distinct:
		if node2, ok := node2.(*Distinct); ok {
			if err := EqualNodes(node1.child, node2.child); err != nil {
				return errors.Wrap(err, "distinct's children not equal")
			}
			return nil
		}

	case *Join:
		if node2, ok := node2.(*Join); ok {
			if err := EqualNodes(node1.source, node2.source); err != nil {
				return errors.Wrap(err, "source nodes underneath not equal")
			}
			if err := EqualNodes(node1.joined, node2.joined); err != nil {
				return errors.Wrap(err, "joined nodes underneath not equal")
			}

			if node1.joinType != node2.joinType {
				return errors.New("joins differ on isLeftJoin")
			}

			return nil
		}

	case *GroupBy:
		if node2, ok := node2.(*GroupBy); ok {
			if err := EqualNodes(node1.source, node2.source); err != nil {
				return errors.Wrap(err, "sources not equal")
			}

			if len(node1.key) != len(node2.key) {
				return fmt.Errorf("key count not equal: %v, %v", len(node1.key), len(node2.key))
			}
			for i := range node1.key {
				if err := EqualExpressions(node1.key[i], node2.key[i]); err != nil {
					return errors.Wrapf(err, "key expression with index %v not equal", i)
				}
			}

			if len(node1.fields) != len(node2.fields) {
				return fmt.Errorf("field count not equal: %v, %v", len(node1.fields), len(node2.fields))
			}
			for i := range node1.fields {
				if node1.fields[i] != node2.fields[i] {
					return fmt.Errorf("field with index %v not equal: %v and %v", i, node1.fields[i], node2.fields[i])
				}
			}

			if len(node1.aggregates) != len(node2.aggregates) {
				return fmt.Errorf("aggregate count not equal: %v, %v", len(node1.aggregates), len(node2.aggregates))
			}
			for i := range node1.aggregates {
				if node1.aggregates[i] != node2.aggregates[i] {
					return fmt.Errorf("aggregate with index %v not equal: %v and %v", i, node1.aggregates[i], node2.aggregates[i])
				}
			}

			if len(node1.as) != len(node2.as) {
				return fmt.Errorf("'as' count not equal: %v, %v", len(node1.as), len(node2.as))
			}
			for i := range node1.as {
				if node1.as[i] != node2.as[i] {
					return fmt.Errorf("'as' with index %v not equal: %v and %v", i, node1.as[i], node2.as[i])
				}
			}

			return nil
		}

	case *TableValuedFunction:
		if node2, ok := node2.(*TableValuedFunction); ok {
			if node1.name != node2.name {
				return fmt.Errorf("names not equal: %v and %v", node1.name, node2.name)
			}

			if len(node1.arguments) != len(node2.arguments) {
				return fmt.Errorf("argument counts not equal: %v and %v", len(node1.arguments), len(node2.arguments))
			}

			for arg, value1 := range node1.arguments {
				value2, ok := node2.arguments[arg]
				if !ok {
					return fmt.Errorf("arguments not equal: %v missing", arg)
				}
				if err := EqualTableValuedFunctionArgumentValue(value1, value2); err != nil {
					return errors.Wrapf(err, "argument %v values not equal", arg)
				}
			}
			return nil
		}

	default:
		log.Fatalf("Unsupported equality comparison %v and %v", reflect.TypeOf(node1), reflect.TypeOf(node2))
	}

	return fmt.Errorf("incompatible types: %v and %v", reflect.TypeOf(node1), reflect.TypeOf(node2))
}

func EqualFormula(expr1, expr2 Formula) error {
	switch expr1 := expr1.(type) {
	case *BooleanConstant:
		if expr2, ok := expr2.(*BooleanConstant); ok {
			if expr1.Value != expr2.Value {
				return fmt.Errorf("values not equal: %v, %v", expr1.Value, expr2.Value)

			}
			return nil
		}

	case *InfixOperator:
		if expr2, ok := expr2.(*InfixOperator); ok {
			if expr1.Operator != expr2.Operator {
				return fmt.Errorf("operators not equal: %v, %v", expr1.Operator, expr2.Operator)

			}
			if err := EqualFormula(expr1.Left, expr2.Left); err != nil {
				return errors.Wrap(err, "left formula not equal")
			}
			if err := EqualFormula(expr1.Right, expr2.Right); err != nil {
				return errors.Wrap(err, "right formula not equal")
			}
			return nil
		}

	case *PrefixOperator:
		if expr2, ok := expr2.(*PrefixOperator); ok {
			if expr1.Operator != expr2.Operator {
				return fmt.Errorf("operators not equal: %v, %v", expr1.Operator, expr2.Operator)

			}
			if err := EqualFormula(expr1.Child, expr2.Child); err != nil {
				return errors.Wrap(err, "child formula not equal")
			}
			return nil
		}

	case *Predicate:
		if expr2, ok := expr2.(*Predicate); ok {
			if expr1.Relation != expr2.Relation {
				return fmt.Errorf("relations not equal: %v, %v", expr1.Relation, expr2.Relation)

			}
			if err := EqualExpressions(expr1.Left, expr2.Left); err != nil {
				return errors.Wrap(err, "left expression not equal")
			}
			if err := EqualExpressions(expr1.Right, expr2.Right); err != nil {
				return errors.Wrap(err, "right expression not equal")
			}
			return nil
		}

	default:
		log.Fatalf("Unsupported equality comparison %v and %v", reflect.TypeOf(expr1), reflect.TypeOf(expr2))
	}

	return fmt.Errorf("incompatible types: %v and %v", reflect.TypeOf(expr1), reflect.TypeOf(expr2))
}

func EqualExpressions(expr1, expr2 Expression) error {
	switch expr1 := expr1.(type) {
	case *StarExpression:
		if expr2, ok := expr2.(*StarExpression); ok {
			if expr1.Name() != expr2.Name() {
				return errors.Errorf("qualifiers not equal: %v %v", expr1.Name().Source(), expr2.Name().Source())
			}

			return nil
		}
	case *Constant:
		if expr2, ok := expr2.(*Constant); ok {
			if expr1.value != expr2.value {
				return fmt.Errorf("values not equal: %v %v, %v %v", reflect.TypeOf(expr1.value), expr1.value, reflect.TypeOf(expr2.value), expr2.value)
			}
			return nil
		}

	case *Variable:
		if expr2, ok := expr2.(*Variable); ok {
			if expr1.name != expr2.name {
				return fmt.Errorf("names not equal: %v, %v", expr1.name, expr2.name)
			}
			return nil
		}

	case *Tuple:
		if expr2, ok := expr2.(*Tuple); ok {
			if len(expr1.expressions) != len(expr2.expressions) {
				return fmt.Errorf("expressions count not equal: %v, %v", len(expr1.expressions), len(expr2.expressions))
			}
			for i := range expr1.expressions {
				if err := EqualExpressions(expr1.expressions[i], expr2.expressions[i]); err != nil {
					return errors.Wrapf(err, "expression %v not equal", i)
				}
			}
			return nil
		}

	case *NodeExpression:
		if expr2, ok := expr2.(*NodeExpression); ok {
			if err := EqualNodes(expr1.node, expr2.node); err != nil {
				return errors.Wrap(err, "nodes not equal")
			}
			return nil
		}

	case *AliasedExpression:
		if expr2, ok := expr2.(*AliasedExpression); ok {
			if expr1.name != expr2.name {
				return fmt.Errorf("names not equal: %v, %v", expr1.name, expr2.name)
			}
			if err := EqualExpressions(expr1.expr, expr2.expr); err != nil {
				return errors.Wrap(err, "expressions not equal")
			}
			return nil
		}

	case *FunctionExpression:
		if expr2, ok := expr2.(*FunctionExpression); ok {
			if expr1.name != expr2.name {
				return fmt.Errorf("names not equal: %v, %v", expr1.name, expr2.name)
			}
			if len(expr1.arguments) != len(expr2.arguments) {
				return fmt.Errorf("argument counts not equal: %v and %v", len(expr1.arguments), len(expr2.arguments))
			}
			for i := range expr1.arguments {
				if err := EqualExpressions(expr1.arguments[i], expr2.arguments[i]); err != nil {
					return errors.Wrapf(err, "argument with index %v not equal", i)
				}
			}
			return nil
		}

	case *Interval:
		if expr2, ok := expr2.(*Interval); ok {
			if err := EqualExpressions(expr1.count, expr2.count); err != nil {
				return errors.Wrap(err, "count not equal")
			}
			if err := EqualExpressions(expr1.unit, expr2.unit); err != nil {
				return errors.Wrap(err, "units not equal")
			}
			return nil
		}

	default:
		log.Fatalf("Unsupported equality comparison %v and %v", reflect.TypeOf(expr1), reflect.TypeOf(expr2))
	}

	return fmt.Errorf("incompatible types: %v and %v", reflect.TypeOf(expr1), reflect.TypeOf(expr2))
}

func EqualTableValuedFunctionArgumentValue(value1 TableValuedFunctionArgumentValue, value2 TableValuedFunctionArgumentValue) error {
	switch value1 := value1.(type) {
	case *TableValuedFunctionArgumentValueExpression:
		if value2, ok := value2.(*TableValuedFunctionArgumentValueExpression); ok {
			if err := EqualExpressions(value1.expression, value2.expression); err != nil {
				return errors.Wrap(err, "expressions not equal")
			}
			return nil
		}

	case *TableValuedFunctionArgumentValueTable:
		if value2, ok := value2.(*TableValuedFunctionArgumentValueTable); ok {
			if err := EqualNodes(value1.source, value2.source); err != nil {
				return errors.Wrap(err, "sources not equal")
			}
			return nil
		}

	case *TableValuedFunctionArgumentValueDescriptor:
		if value2, ok := value2.(*TableValuedFunctionArgumentValueDescriptor); ok {
			if value1.descriptor != value2.descriptor {
				return fmt.Errorf("descriptors not equal: %v, %v", value1.descriptor, value2.descriptor)
			}
			return nil
		}

	default:
		log.Fatalf("Unsupported equality comparison %v and %v", reflect.TypeOf(value1), reflect.TypeOf(value2))
	}

	return fmt.Errorf("incompatible types: %v and %v", reflect.TypeOf(value1), reflect.TypeOf(value2))
}
