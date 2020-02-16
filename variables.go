package octosql

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

func NewVariableName(varname string) VariableName {
	if len(varname) > 0 && varname[0] == '.' {
		varname = varname[1:]
	}
	var namespace, name string
	if i := strings.Index(varname, "."); i != -1 {
		namespace = varname[:i]
		name = varname[i+1:]
	} else {
		name = varname
	}
	return VariableName{Namespace: namespace, VarName: name}
}

func (vn VariableName) String() string {
	return fmt.Sprintf("%s.%s", vn.Namespace, vn.VarName)
}

func (vn VariableName) Source() string {
	return vn.Namespace
}

func (vn VariableName) Name() string {
	return vn.VarName
}

func (vn VariableName) Empty() bool {
	return len(vn.VarName) == 0
}

func (vn VariableName) Equal(other VariableName) bool {
	return vn.Namespace == other.Namespace && vn.VarName == other.VarName
}

func (vn VariableName) LessThan(other VariableName) bool {
	if vn.Source() == other.Source() {
		return vn.Name() < other.Name()
	}
	return vn.Source() < other.Source()
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
		result[i] = NewVariableName(s) // TODO: it can be either this, or NewVariableName
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
