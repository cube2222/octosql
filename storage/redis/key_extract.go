package redis

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

func GetAllKeys(filter physical.Formula, key octosql.VariableName) (map[octosql.VariableName]interface{}, error) {
	switch formula := filter.(type) {
	case *physical.And:
		leftKeys, err := GetAllKeys(formula.Left, key)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get left keys")
		}
		rightKeys, err := GetAllKeys(formula.Right, key)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get right keys")
		}

		if AreKeysSetsEqual(leftKeys, rightKeys) {
			return leftKeys, nil
		}
		return nil, errors.Errorf("left and right keys are not equal")

	case *physical.Or:
		leftKeys, err := GetAllKeys(formula.Left, key)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get left keys")
		}
		leftVariables := octosql.NewVariables(leftKeys)

		rightKeys, err := GetAllKeys(formula.Right, key)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get right keys")
		}
		rightVariables := octosql.NewVariables(rightKeys)

		leftVariables, err = leftVariables.MergeWith(rightVariables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't merge left and right keys")
		}
		return leftKeys, nil

	case *physical.Predicate:
		// TODO
		switch formula.Right.(type) {
		case physical.Variable:

		}

		return nil, nil

	default:
		return nil, errors.Errorf("wrong formula type for redis database")
	}
}

func AreKeysSetsEqual(keys1, keys2 map[octosql.VariableName]interface{}) bool {
	if len(keys1) == len(keys2) {
		for k := range keys1 {
			_, ok := keys2[k]
			if !ok {
				return false
			}
		}
		return true
	}
	return false
}
