package physical

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
)

// FieldType describes if a key is a primary or secondary attribute.
type FieldType string

const (
	Primary   FieldType = "primary"
	Secondary FieldType = "secondary"
)

// DataSourceBuilderFactory is a function used to create a new aliased data source builder.
type DataSourceBuilderFactory func(name, alias string) *DataSourceBuilder

// DataSourceRepository is used to register factories for builders for any data source.
// It can also later create a builder for any of those data source.
type DataSourceRepository interface {
	Get(dataSourceName, alias string) (*DataSourceBuilder, error)
}

type dataSourceRepository struct {
	factories map[string]DataSourceBuilderFactory
}

func NewDataSourceRepository() *dataSourceRepository {
	return &dataSourceRepository{
		factories: make(map[string]DataSourceBuilderFactory),
	}
}

// Get gets a new builder for a given data source.
func (repo *dataSourceRepository) Get(dataSourceName, alias string) (*DataSourceBuilder, error) {
	ds, ok := repo.factories[dataSourceName]
	if !ok {
		var dss []string
		for k := range repo.factories {
			dss = append(dss, k)
		}
		return nil, errors.Errorf("no such datasource: %s, available datasources: %+v", dataSourceName, dss)
	}

	return ds(dataSourceName, alias), nil
}

// Register registers a builder factory for the given data source ColumnName.
func (repo *dataSourceRepository) Register(dataSourceName string, factory DataSourceBuilderFactory) error {
	_, ok := repo.factories[dataSourceName]
	if ok {
		return errors.Errorf("data Source with ColumnName %s already registered", dataSourceName)
	}
	repo.factories[dataSourceName] = factory
	return nil
}

// DataSourceBuilder is used to build a data source instance with an alias.
// It may be given filters, which are later executed at the database level.
type DataSourceBuilder struct {
	Materializer     func(ctx context.Context, matCtx *MaterializationContext, dbConfig map[string]interface{}, filter Formula, alias string) (execution.Node, error)
	PrimaryKeys      []octosql.VariableName
	AvailableFilters map[FieldType]map[Relation]struct{}
	Filter           Formula
	Name             string
	Alias            string

	// This field will be used to decide on join strategies or if the source is a stream.
	Cardinality metadata.Cardinality
}

func NewDataSourceBuilderFactory(materializer func(ctx context.Context, matCtx *MaterializationContext, dbConfig map[string]interface{}, filter Formula, alias string) (execution.Node, error), primaryKeys []octosql.VariableName, availableFilters map[FieldType]map[Relation]struct{}, cardinality metadata.Cardinality) DataSourceBuilderFactory {
	return func(name, alias string) *DataSourceBuilder {
		return &DataSourceBuilder{
			Materializer:     materializer,
			PrimaryKeys:      primaryKeys,
			AvailableFilters: availableFilters,
			Filter:           NewConstant(true),
			Name:             name,
			Alias:            alias,
			Cardinality:      cardinality,
		}
	}
}

func (dsb *DataSourceBuilder) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &DataSourceBuilder{
		Materializer:     dsb.Materializer,
		PrimaryKeys:      dsb.PrimaryKeys,
		AvailableFilters: dsb.AvailableFilters,
		Filter:           dsb.Filter.Transform(ctx, transformers),
		Name:             dsb.Name,
		Alias:            dsb.Alias,
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (dsb *DataSourceBuilder) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	dbConfig, err := matCtx.Config.GetDataSourceConfig(dsb.Name)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't get config for database %v", dsb.Name)
	}

	return dsb.Materializer(ctx, matCtx, dbConfig, dsb.Filter, dsb.Alias)
}

func (dsb *DataSourceBuilder) Metadata() *metadata.NodeMetadata {
	return metadata.NewNodeMetadata(dsb.Cardinality, octosql.NewVariableName(""))
}

func (dsb *DataSourceBuilder) Visualize() *graph.Node {
	n := graph.NewNode("Data Source Builder")

	n.AddField("name", dsb.Name)
	n.AddField("alias", dsb.Alias)

	primaryKeys := make([]string, len(dsb.PrimaryKeys))
	for i := range dsb.PrimaryKeys {
		primaryKeys[i] = dsb.PrimaryKeys[i].String()
	}

	var primary []string
	for filter := range dsb.AvailableFilters[Primary] {
		primary = append(primary, string(filter))
	}
	var secondary []string
	for filter := range dsb.AvailableFilters[Secondary] {
		secondary = append(secondary, string(filter))
	}
	n.AddField("available primary filters", fmt.Sprintf("%+v", primary))
	n.AddField("available secondary filters", fmt.Sprintf("%+v", secondary))

	n.AddField("cardinality", string(dsb.Cardinality))

	n.AddChild("filter", dsb.Filter.Visualize())
	return n
}
