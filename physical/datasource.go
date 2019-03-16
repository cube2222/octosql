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

func (ds *DataSourceBuilder) Materialize(ctx context.Context) (execution.Node, error) {
	return ds.Executor(ds.Filter, ds.Alias)
}
