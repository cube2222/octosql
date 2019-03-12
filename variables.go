package octosql

import "github.com/pkg/errors"

type VariableName string
type Variables map[VariableName]interface{}

func NoVariables() Variables {
	return make(Variables)
}

func NewVariables(variables map[VariableName]interface{}) Variables {
	return Variables(variables)
}

var ErrVariableNotFound = errors.New("variable not found")

func (vs Variables) Get(k VariableName) (interface{}, error) {
	out, ok := vs[k]
	if !ok {
		return nil, ErrVariableNotFound
	}

	return out, nil
}

func (vs Variables) MergeWith(other Variables) (Variables, error) {
	out := make(Variables)
	for k, v := range vs {
		out[k] = v
	}
	for k, v := range other {
		if vOld, ok := out[k]; ok {
			return nil, errors.Errorf("%v already defined as %+v, wanted to add %+v", k, vOld, v)
		}
		out[k] = v
	}
	return out, nil
}
