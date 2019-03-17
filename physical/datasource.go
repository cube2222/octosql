package physical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

// FieldType describes if a key is a primary or secondary attribute.
type FieldType string

const (
	Primary   FieldType = "primary"
	Secondary FieldType = "secondary"
)

// DataSourceRepository is used to register factories for builders for any data source.
// It can also later create a builder for any of those data source.
type DataSourceRepository struct {
	factories map[string]func(alias string) *DataSourceBuilder
}

func NewDataSourceRepository() *DataSourceRepository {
	return &DataSourceRepository{
		factories: make(map[string]func(alias string) *DataSourceBuilder),
	}
}

// Get gets a new builder for a given data source.
func (repo *DataSourceRepository) Get(dataSourceName, alias string) (*DataSourceBuilder, error) {
	ds, ok := repo.factories[dataSourceName]
	if !ok {
		var dss []string
		for k := range repo.factories {
			dss = append(dss, k)
		}
		return nil, errors.Errorf("no such datasource: %s, available datasources: %+v", dataSourceName, dss)
	}

	return ds(alias), nil
}

// Register registers a builder factory for the given data source name.
func (repo *DataSourceRepository) Register(dataSourceName string, factory func(alias string) *DataSourceBuilder) error {
	_, ok := repo.factories[dataSourceName]
	if ok {
		return errors.Errorf("data Source with name %s already registered", dataSourceName)
	}
	repo.factories[dataSourceName] = factory
	return nil
}

// DataSourceBuilder is used to build a data source instance with an alias.
// It may be given filters, which are later executed at the database level.
type DataSourceBuilder struct {
	Executor         func(formula Formula, alias string) (execution.Node, error)
	PrimaryKeys      []octosql.VariableName
	AvailableFilters map[FieldType]map[Relation]struct{}
	Filter           Formula
	Alias            string
}

func NewDataSourceBuilderFactory(executor func(filter Formula, alias string) (execution.Node, error), primaryKeys []octosql.VariableName, availableFilters map[FieldType]map[Relation]struct{}) func(alias string) *DataSourceBuilder {
	return func(alias string) *DataSourceBuilder {
		return &DataSourceBuilder{
			Executor:         executor,
			PrimaryKeys:      primaryKeys,
			AvailableFilters: availableFilters,
			Filter:           NewConstant(true),
			Alias:            alias,
		}
	}
}

func (dsb *DataSourceBuilder) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &DataSourceBuilder{
		Executor:         dsb.Executor,
		PrimaryKeys:      dsb.PrimaryKeys,
		AvailableFilters: dsb.AvailableFilters,
		Filter:           dsb.Filter.Transform(ctx, transformers),
		Alias:            dsb.Alias,
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (dsb *DataSourceBuilder) Materialize(ctx context.Context) (execution.Node, error) {
	return dsb.Executor(dsb.Filter, dsb.Alias)
}
