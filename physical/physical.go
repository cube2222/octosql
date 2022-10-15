package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
)

// TODO: There should be a seperate MaterializationContext.
type Environment struct {
	Aggregates      map[string]AggregateDetails
	Datasources     *DatasourceRepository
	Functions       map[string]FunctionDetails
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

type AggregateDetails struct {
	Description string
	Descriptors []AggregateDescriptor
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
	Databases    map[string]func() (Database, error)
	FileHandlers map[string]func(ctx context.Context, name string, options map[string]string) (DatasourceImplementation, Schema, error)
}

type Database interface {
	ListTables(ctx context.Context) ([]string, error)
	GetTable(ctx context.Context, name string, options map[string]string) (DatasourceImplementation, Schema, error)
}

func (dr *DatasourceRepository) GetDatasource(ctx context.Context, name string, options map[string]string) (DatasourceImplementation, Schema, error) {
	if index := strings.Index(name, "."); index != -1 {
		dbName := name[:index]
		dbConstructor, ok := dr.Databases[dbName]
		if ok {
			db, err := dbConstructor()
			if err != nil {
				return nil, Schema{}, fmt.Errorf("couldn't initialize database '%s': %w", dbName, err)
			}

			return db.GetTable(ctx, name[index+1:], options)
		}
	}
	if index := strings.LastIndex(name, "."); index != -1 {
		extension := name[index+1:]
		if handler, ok := dr.FileHandlers[extension]; ok {
			return handler(ctx, name, options)
		}
	}

	return nil, Schema{}, fmt.Errorf("no such table: %s", name)
}

type DatasourceImplementation interface {
	Materialize(ctx context.Context, env Environment, schema Schema, pushedDownPredicates []Expression) (execution.Node, error)
	PushDownPredicates(newPredicates, pushedDownPredicates []Expression) (rejected, pushedDown []Expression, changed bool)
}

type FunctionDetails struct {
	Description string
	Descriptors []FunctionDescriptor
}

type FunctionDescriptor struct {
	ArgumentTypes []octosql.Type
	OutputType    octosql.Type
	TypeFn        func([]octosql.Type) (octosql.Type, bool) `json:"-"`
	Strict        bool
	Function      func([]octosql.Value) (octosql.Value, error) `json:"-"`
}
