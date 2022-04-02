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
	Databases       map[string]func() (Database, error)
	DefaultDatabase string
	FileHandlers    map[string]func(name string, options map[string]string) (DatasourceImplementation, Schema, error)
}

type Database interface {
	ListTables(ctx context.Context) ([]string, error)
	GetTable(ctx context.Context, name string, options map[string]string) (DatasourceImplementation, Schema, error)
}

func (dr *DatasourceRepository) GetDatasource(ctx context.Context, name string, options map[string]string) (DatasourceImplementation, Schema, error) {
	// TODO: Special name.json handling. Best would be even 'path/to/file.json', but maybe achieve that using a function.
	// Should maybe be file.`path/to/file.json`?
	if strings.HasSuffix(name, ".json") {
		return dr.FileHandlers["json"](name, options)
	}
	if strings.HasSuffix(name, ".csv") {
		return dr.FileHandlers["csv"](name, options)
	}
	if index := strings.Index(name, "."); index != -1 {
		dbName := name[:index]
		dbConstructor, ok := dr.Databases[dbName]
		if !ok {
			return nil, Schema{}, fmt.Errorf("no such database: %s, as in %s", dbName, name)
		}
		db, err := dbConstructor()
		if err != nil {
			return nil, Schema{}, fmt.Errorf("couldn't initialize database '%s': %w", dbName, err)
		}

		return db.GetTable(ctx, name[index+1:], options)
	}

	// TODO: If there is no dot, iterate over all tables in all datasources.
	//   Eh, that would be *really* expensive, as it would force-initialize all databases. How about no?
	dbConstructor, ok := dr.Databases[dr.DefaultDatabase]
	if !ok {
		return nil, Schema{}, fmt.Errorf("unknown datasource: %s", name)
	}
	db, err := dbConstructor()
	if err != nil {
		return nil, Schema{}, fmt.Errorf("couldn't initialize database '%s': %w", dr.DefaultDatabase, err)
	}

	return db.GetTable(ctx, name, options)
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
