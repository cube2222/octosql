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
	TypeFn       func(octosql.Type) (octosql.Type, bool)
	Prototype    func() nodes.Aggregate
}

type DatasourceRepository struct {
	// TODO: A może jednak ten bardziej dynamiczny interfejs? Że database.<table> i wtedy sie resolvuje
	// Bo inaczej będzie na start strasznie dużo rzeczy ładować niepotrzebnych dla wszystkich
	// skonfigurowanych baz danych.
	Databases       map[string]Database
	DefaultDatabase string
	FileHandlers    map[string]func(name string) (DatasourceImplementation, error)
}

type Database interface {
	ListTables(ctx context.Context) ([]string, error)
	GetTable(ctx context.Context, name string) (DatasourceImplementation, error)
}

func (dr *DatasourceRepository) GetDatasource(ctx context.Context, name string) (DatasourceImplementation, error) {
	// TODO: Special name.json handling. Best would be even 'path/to/file.json', but maybe achieve that using a function.
	// Should maybe be file.`path/to/file.json`?
	if strings.HasSuffix(name, ".json") {
		return dr.FileHandlers["json"](name)
	}
	if strings.HasSuffix(name, ".csv") {
		return dr.FileHandlers["csv"](name)
	}
	if index := strings.Index(name, "."); index != -1 {
		dbName := name[:index]
		db, ok := dr.Databases[dbName]
		if !ok {
			return nil, fmt.Errorf("no such database: %s, as in %s", dbName, name)
		}

		return db.GetTable(ctx, name[index+1:])
	}
	db, ok := dr.Databases[dr.DefaultDatabase]
	if ok {
		return db.GetTable(ctx, name)
	}
	// TODO: If there is no dot, iterate over all tables in all datasources.
	return nil, fmt.Errorf("unknown datasource: %s", name)
}

type DatasourceImplementation interface {
	Schema() (Schema, error)
	Materialize(ctx context.Context, env Environment, pushedDownPredicates []Expression) (execution.Node, error)
	PushDownPredicates(newPredicates, pushedDownPredicates []Expression) (rejected []Expression, pushedDown []Expression, changed bool)
	// TODO: Function checking for push-down
}

type FunctionDescriptor struct {
	ArgumentTypes []octosql.Type
	OutputType    octosql.Type
	Strict        bool
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
