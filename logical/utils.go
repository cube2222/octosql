package logical

import (
	"log"
	"reflect"

	"github.com/pkg/errors"
)

// W przyszłości można zrobić z tego może metodę na Node,
// jeśli np. optymalizatorowi się przyda, a nie tylko do testów.
func EqualNodes(node1, node2 Node) error {
	switch node1 := node1.(type) {
	case *UnionAll:
		if node2, ok := node2.(*UnionAll); ok {
			if err := EqualNodes(node1.first, node2.first); err != nil {
				return errors.Errorf("first statements not equal: %+v, %+v", node1.first, node2.first)
			}
			if err := EqualNodes(node1.second, node2.second); err != nil {
				return errors.Errorf("second statements not equal: %+v, %+v", node1.second, node2.second)
			}
			return nil
		}

	case *Map:
		if node2, ok := node2.(*Map); ok {
			if len(node1.expressions) != len(node2.expressions) {
				return errors.Errorf("expressions count not equal: %v, %v", len(node1.expressions), len(node2.expressions))
			}
			for i := range node1.expressions {
				if err := EqualExpressions(node1.expressions[i], node2.expressions[i]); err != nil {
					return errors.Wrapf(err, "expression %v not equal", i)
				}
			}
			if err := EqualNodes(node1.source, node2.source); err != nil {
				return errors.Wrap(err, "sources not equal")
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
				return errors.Errorf("qualifiers not equal: %v, %v", node1.qualifier, node2.qualifier)
			}
			if err := EqualNodes(node1.source, node2.source); err != nil {
				return errors.Wrap(err, "sources not qual")
			}
			return nil
		}

	case *DataSource:
		if node2, ok := node2.(*DataSource); ok {
			if node1.name != node2.name {
				return errors.Errorf("names not equal: %v, %v", node1.name, node2.name)
			}
			if node1.alias != node2.alias {
				return errors.Errorf("aliases not equal: %v, %v", node1.alias, node2.alias)
			}
			return nil
		}

	case *Limit:
		if node2, ok := node2.(*Limit); ok {
			if (node1.limitExpr == nil) != (node2.limitExpr == nil){
				return errors.New("exactly one of limit subexpressions is nil")
			}
			if node1.limitExpr != nil {
				if err := EqualExpressions(node1.limitExpr, node2.limitExpr); err != nil {
					return errors.Wrap(err, "limit subexpressions not equal")
				}
			}
			if err := EqualNodes(node1.data, node2.data); err != nil {
				return errors.Wrap(err, "data nodes underneath not equal")
			}
			return nil
		}

	case *Offset:
		if node2, ok := node2.(*Offset); ok {
			if (node1.offsetExpr == nil) != (node2.offsetExpr == nil){
				return errors.New("exactly one of offset subexpressions is nil")
			}
			if node1.offsetExpr != nil {
				if err := EqualExpressions(node1.offsetExpr, node2.offsetExpr); err != nil {
					return errors.Wrap(err, "offset subexpressions not equal")
				}
			}
			if err := EqualNodes(node1.data, node2.data); err != nil {
				return errors.Wrap(err, "data nodes underneath not equal")
			}
			return nil
		}

	default:
		log.Fatalf("Unsupported equality comparison %v and %v", reflect.TypeOf(node1), reflect.TypeOf(node2))
	}

	return errors.Errorf("incompatible types: %v and %v", reflect.TypeOf(node1), reflect.TypeOf(node2))
}

func EqualFormula(expr1, expr2 Formula) error {
	switch expr1 := expr1.(type) {
	case *BooleanConstant:
		if expr2, ok := expr2.(*BooleanConstant); ok {
			if expr1.Value != expr2.Value {
				return errors.Errorf("values not equal: %v, %v", expr1.Value, expr2.Value)

			}
			return nil
		}

	case *InfixOperator:
		if expr2, ok := expr2.(*InfixOperator); ok {
			if expr1.Operator != expr2.Operator {
				return errors.Errorf("operators not equal: %v, %v", expr1.Operator, expr2.Operator)

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
				return errors.Errorf("operators not equal: %v, %v", expr1.Operator, expr2.Operator)

			}
			if err := EqualFormula(expr1.Child, expr2.Child); err != nil {
				return errors.Wrap(err, "child formula not equal")
			}
			return nil
		}

	case *Predicate:
		if expr2, ok := expr2.(*Predicate); ok {
			if expr1.Relation != expr2.Relation {
				return errors.Errorf("relations not equal: %v, %v", expr1.Relation, expr2.Relation)

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

	return errors.Errorf("incompatible types: %v and %v", reflect.TypeOf(expr1), reflect.TypeOf(expr2))
}

func EqualExpressions(expr1, expr2 Expression) error {
	switch expr1 := expr1.(type) {
	case *Constant:
		if expr2, ok := expr2.(*Constant); ok {
			if expr1.value != expr2.value {
				return errors.Errorf("values not equal: %v %v, %v %v", reflect.TypeOf(expr1.value), expr1.value, reflect.TypeOf(expr2.value), expr2.value)
			}
		}
		return nil

	case *Variable:
		if expr2, ok := expr2.(*Variable); ok {
			if expr1.name != expr2.name {
				return errors.Errorf("names not equal: %v, %v", expr1.name, expr2.name)
			}
		}
		return nil

	case *NodeExpression:
		if expr2, ok := expr2.(*NodeExpression); ok {
			if err := EqualNodes(expr1.node, expr2.node); err != nil {
				return errors.Wrap(err, "nodes not equal")
			}
		}
		return nil

	case *AliasedExpression:
		if expr2, ok := expr2.(*AliasedExpression); ok {
			if expr1.name != expr2.name {
				return errors.Errorf("names not equal: %v, %v", expr1.name, expr2.name)
			}
			if err := EqualExpressions(expr1.expr, expr2.expr); err != nil {
				return errors.Wrap(err, "expressions not equal")
			}
			return nil
		}

	default:
		log.Fatalf("Unsupported equality comparison %v and %v", reflect.TypeOf(expr1), reflect.TypeOf(expr2))
	}

	return errors.Errorf("incompatible types: %v and %v", reflect.TypeOf(expr1), reflect.TypeOf(expr2))
}
