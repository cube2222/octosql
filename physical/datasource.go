package physical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type FieldType string

const (
	Primary   FieldType = "primary"
	Secondary FieldType = "secondary"
)

type DataSourceRepository struct {
	factories map[string]func(alias string) *DataSourceBuilder
}

func NewDataSourceRepository() *DataSourceRepository {
	return &DataSourceRepository{
		factories: make(map[string]func(alias string) *DataSourceBuilder),
	}
}

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

func (repo *DataSourceRepository) Register(dataSourceName string, factory func(alias string) *DataSourceBuilder) error {
	_, ok := repo.factories[dataSourceName]
	if ok {
		return errors.Errorf("data Source with name %s already registered", dataSourceName)
	}
	repo.factories[dataSourceName] = factory
	return nil
}

type DataSourceBuilder struct {
	executor         func(formula Formula, alias string) (execution.Node, error)
	primaryKeys      []octosql.VariableName
	availableFilters map[FieldType]map[Relation]struct{}
	filter           Formula
	alias            string
}

func NewDataSourceBuilderFactory(executor func(filter Formula, alias string) (execution.Node, error), primaryKeys []octosql.VariableName, availableFilters map[FieldType]map[Relation]struct{}) func(alias string) *DataSourceBuilder {
	return func(alias string) *DataSourceBuilder {
		return &DataSourceBuilder{
			executor:         executor,
			primaryKeys:      primaryKeys,
			availableFilters: availableFilters,
			filter:           NewConstant(true),
			alias:            alias,
		}
	}
}

func (ds *DataSourceBuilder) PrimaryKeys() map[octosql.VariableName]struct{} {
	out := make(map[octosql.VariableName]struct{})
	for _, k := range ds.primaryKeys {
		out[k] = struct{}{}
	}
	return out
}

func (ds *DataSourceBuilder) AvailableFilters() map[FieldType]map[Relation]struct{} {
	return ds.availableFilters
}

func (ds *DataSourceBuilder) AddFilter(formula Formula) {
	ds.filter = NewAnd(ds.filter, formula)
}

func (ds *DataSourceBuilder) Materialize(ctx context.Context) (execution.Node, error) {
	return ds.executor(ds.filter, ds.alias)
}
