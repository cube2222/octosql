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
	factories map[string]func() *DataSource // ?
}

func (repo *DataSourceRepository) Get(dataSourceName string) (*DataSource, error) {
	ds, ok := repo.factories[dataSourceName]
	if !ok {
		var dss []string
		for k := range repo.factories {
			dss = append(dss, k)
		}
		return nil, errors.Errorf("no such datasource: %s, available datasources: %+v", dataSourceName, dss)
	}

	return ds(), nil
}

func (repo *DataSourceRepository) Register(dataSourceName string, factory func() *DataSource) error {
	_, ok := repo.factories[dataSourceName]
	if ok {
		return errors.Errorf("data source with name %s already registered", dataSourceName)
	}
	return nil
}

type DataSource struct {
	executor         func(formula Formula) execution.Node
	primaryKeys      []octosql.VariableName
	availableFilters map[FieldType]map[Relation]struct{}
	filter           Formula
}

func NewDataSourceFactory(executor func(filter Formula) execution.Node, primaryKeys []octosql.VariableName, availableFilters map[FieldType]map[Relation]struct{}) func() *DataSource {
	return func() *DataSource {
		return &DataSource{
			executor:         executor,
			primaryKeys:      primaryKeys,
			availableFilters: availableFilters,
			filter:           NewConstant(true),
		}
	}
}

func (ds *DataSource) PrimaryKeys() map[octosql.VariableName]struct{} {
	out := make(map[octosql.VariableName]struct{})
	for _, k := range ds.primaryKeys {
		out[k] = struct{}{}
	}
	return out
}

func (ds *DataSource) AvailableFilters() map[FieldType]map[Relation]struct{} {
	return ds.availableFilters
}

func (ds *DataSource) AddFilter(formula Formula) {
	ds.filter = NewAnd(ds.filter, formula)
}

func (ds *DataSource) Materialize(ctx context.Context) execution.Node {
	return ds.executor(ds.filter)
}
