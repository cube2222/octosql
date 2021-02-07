package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
)

type Environment struct {
	Aggregates           map[string][]AggregateDescriptor
	Datasources          *DatasourceRepository
	Functions            map[string][]FunctionDescriptor
	PhysicalConfig       map[string]interface{}
	TableValuedFunctions map[string][]TableValuedFunctionDescriptor
	VariableContext      *VariableContext
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
	Prototype    func() nodes.Aggregate
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
	Materialize(ctx context.Context, env Environment) (execution.Node, error)
	// TODO: Function checking for push-down
}

type FunctionDescriptor struct {
	ArgumentTypes []octosql.Type
	OutputType    octosql.Type
	Function      func([]octosql.Value) (octosql.Value, error)
}

type TableValuedFunctionDescriptor struct {
	Arguments map[string]TableValuedFunctionArgumentMatcher
	// Here we can check the inputs.
	OutputSchema func(context.Context, Environment, map[string]TableValuedFunctionArgument) (Schema, error)
	Materialize  func(context.Context, Environment, map[string]TableValuedFunctionArgument) (execution.Node, error)
}

type TableValuedFunctionArgumentMatcher struct {
	Required                               bool
	TableValuedFunctionArgumentMatcherType TableValuedFunctionArgumentType
	// Only one of the below may be non-null.
	Expression *TableValuedFunctionArgumentMatcherExpression
	Table      *TableValuedFunctionArgumentMatcherTable
	Descriptor *TableValuedFunctionArgumentMatcherDescriptor
}

type TableValuedFunctionArgumentMatcherExpression struct {
	Type octosql.Type
}

type TableValuedFunctionArgumentMatcherTable struct {
}

type TableValuedFunctionArgumentMatcherDescriptor struct {
}

type State struct {
}
