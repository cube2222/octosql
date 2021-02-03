package physical

import (
	"fmt"
	"strings"

	"github.com/cube2222/octosql"
)

type Environment struct {
	Datasources *DatasourceRepository
	Functions   *FunctionRepository
	// aggregates *Aggregates
	PhysicalConfig map[string]interface{}
}

type DatasourceRepository struct {
	Datasources map[string]func(name string) DatasourceDescriptor
}

func (dr *DatasourceRepository) GetDatasource(name string) (DatasourceDescriptor, error) {
	// TODO: Special name.json handling. Best would be even 'path/to/file.json', but maybe achieve that using a function.
	if strings.HasSuffix(name, ".json") {
		return dr.Datasources["json"](name), nil
	}
	return dr.Datasources[name](name), nil
}

type DatasourceDescriptor interface {
	Schema() (Schema, error)
}

type FunctionRepository struct {
	FunctionOverloads map[string][]struct {
		ArgTypes []octosql.Type
		FunctionDescriptor
	}
}

func (fr *FunctionRepository) GetFunction(name string, arguments []octosql.Type) (FunctionDescriptor, error) {
	panic("implement me")
}

type FunctionDescriptor interface {
}

type State struct {
	variableCounter int
}

func (state *State) GetVariableName() (out string) {
	out = fmt.Sprintf("var_%d", state.variableCounter)
	state.variableCounter++
	return
}
