package octosql

import (
	"strings"

	"github.com/pkg/errors"
)

type VariableName string

func NewVariableName(varname string) VariableName {
	return VariableName(varname)
}

func (vn *VariableName) String() string {
	return string(*vn)
}

func (vn *VariableName) Source() string {
	parts := strings.Split(vn.String(), ".")
	if len(parts) == 1 {
		return ""
	} else {
		return parts[0]
	}
}

func (vn *VariableName) Name() string {
	i := strings.Index(vn.String(), ".")
	if i == -1 {
		return vn.String()
	}
	return vn.String()[i+1:]
}

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

func (vs Variables) Equal(vs1 Variables) bool {
	if len(vs) == len(vs1) {
		for k := range vs {
			_, ok := vs1[k]
			if !ok {
				return false
			}
		}
		return true
	}
	return false
}
