package physical

import (
	"fmt"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

type Environment struct {
	Aggregates  map[string][]AggregateDescriptor
	Datasources *DatasourceRepository
	// Functions       *FunctionRepository
	PhysicalConfig  map[string]interface{}
	VariableContext *VariableContext
}

func (env Environment) WithRecordSchema(schema Schema) Environment {
	newEnv := env
	newEnv.VariableContext = newEnv.VariableContext.WithRecordSchema(schema)
	return newEnv
}

type VariableContext struct {
	Parent *VariableContext
	Fields []SchemaField
}

func (varCtx *VariableContext) WithRecordSchema(schema Schema) *VariableContext {
	return &VariableContext{
		Parent: varCtx,
		Fields: schema.Fields,
	}
}

type AggregateDescriptor struct {
	ArgumentType octosql.Type
	OutputType   octosql.Type
	Prototype    func() Aggregate
}

type DatasourceRepository struct {
	Datasources map[string]func(name string) (DatasourceImplementation, error)
}

func (dr *DatasourceRepository) GetDatasource(name string) (DatasourceImplementation, error) {
	// TODO: Special name.json handling. Best would be even 'path/to/file.json', but maybe achieve that using a function.
	if strings.HasSuffix(name, ".json") {
		return dr.Datasources["json"](name)
	}
	// All this doesn't really make any sense TBH.
	if ds, ok := dr.Datasources[name]; ok {
		return ds(name)
	}
	return nil, fmt.Errorf("unknown datasource: %s", name)
}

type DatasourceImplementation interface {
	Schema() (Schema, error)
	Materialize() (execution.Node, error)
	// TODO: Function checking for push-down
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
}
