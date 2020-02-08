package octosql

import (
	"strings"

	"github.com/pkg/errors"
)

type VariableName string

func NewVariableName(varname string) VariableName {
	if len(varname) > 0 && varname[0] == '.' {
		varname = varname[1:]
	}
	return VariableName(strings.ToLower(varname))
}

func (vn VariableName) String() string {
	return string(vn)
}

func (vn VariableName) Source() string {
	parts := strings.Split(vn.String(), ".")
	if len(parts) == 1 {
		return ""
	} else {
		return parts[0]
	}
}

func (vn VariableName) Name() string {
	i := strings.Index(vn.String(), ".")
	if i == -1 {
		return vn.String()
	}
	return vn.String()[i+1:]
}

func (vn VariableName) Empty() bool {
	return len(vn) == 0
}

func (vn VariableName) Equal(other VariableName) bool {
	return vn == other
}

type Variables map[VariableName]Value

func NoVariables() Variables {
	return make(Variables)
}

func NewVariables(variables map[VariableName]Value) Variables {
	return Variables(variables)
}

var ErrVariableNotFound = errors.New("variable not found")

func (vs Variables) Get(k VariableName) (Value, error) {
	out, ok := vs[k]
	if !ok {
		return MakeNull(), nil
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

func StringsToVariableNames(strings []string) []VariableName {
	result := make([]VariableName, len(strings))
	for i, s := range strings {
		result[i] = VariableName(s) //TODO: it can be either this, or NewVariableName
	}

	return result
}

func VariableNamesToStrings(vars []VariableName) []string {
	result := make([]string, len(vars))
	for i, v := range vars {
		result[i] = v.String()
	}

	return result
}
