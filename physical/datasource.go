package physical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"

	"github.com/pkg/errors"
)

// FieldType describes if a key is a primary or secondary attribute.
type FieldType string

const (
	Primary   FieldType = "primary"
	Secondary FieldType = "secondary"
)

// DataSourceBuilderFactory is a function used to create a new aliased data source builder.
type DataSourceBuilderFactory func(name, alias string) []Node

// DataSourceRepository is used to register factories for builders for any data source.
// It can also later create a builder for any of those data source.
type DataSourceRepository struct {
	factories map[string]DataSourceBuilderFactory
}

func NewDataSourceRepository() *DataSourceRepository {
	return &DataSourceRepository{
		factories: make(map[string]DataSourceBuilderFactory),
	}
}

// Get gets a new builder for a given data source.
func (repo *DataSourceRepository) Get(dataSourceName, alias string) ([]Node, error) {
	ds, ok := repo.factories[dataSourceName]
	if !ok {
		var dss []string
		for k := range repo.factories {
			dss = append(dss, k)
		}
		return nil, errors.Errorf("no such datasource or common table expression: %s, available datasources and CTEs: %+v", dataSourceName, dss)
	}

	return ds(dataSourceName, alias), nil
}

// Register registers a builder factory for the given data source ColumnName.
func (repo *DataSourceRepository) Register(dataSourceName string, factory DataSourceBuilderFactory) error {
	_, ok := repo.factories[dataSourceName]
	if ok {
		return errors.Errorf("data Source with ColumnName %s already registered", dataSourceName)
	}
	repo.factories[dataSourceName] = factory
	return nil
}

// Register registers a builder factory for the given data source ColumnName.
func (repo *DataSourceRepository) WithFactory(dataSourceName string, factory DataSourceBuilderFactory) *DataSourceRepository {
	newRepo := &DataSourceRepository{
		factories: make(map[string]DataSourceBuilderFactory),
	}

	for oldName, oldFactory := range repo.factories {
		newRepo.factories[oldName] = oldFactory
	}
	newRepo.factories[dataSourceName] = factory

	return newRepo
}

type DataSourceMaterializerFunc func(ctx context.Context, matCtx *MaterializationContext, dbConfig map[string]interface{}, filter Formula, alias string, partition int) (execution.Node, error)

// DataSourceBuilder is used to build a data source instance with an alias.
// It may be given filters, which are later executed at the database level.
type DataSourceBuilder struct {
	Materializer     DataSourceMaterializerFunc
	PrimaryKeys      []octosql.VariableName
	AvailableFilters map[FieldType]map[Relation]struct{}
	Filter           Formula
	Name             string
	Alias            string
	Partition        int

	// This field will be used to decide on join strategies or if the source is a stream.
	Cardinality metadata.Cardinality
}

func NewDataSourceBuilderFactory(materializer DataSourceMaterializerFunc, primaryKeys []octosql.VariableName, availableFilters map[FieldType]map[Relation]struct{}, cardinality metadata.Cardinality, partitions int) DataSourceBuilderFactory {
	return func(name, alias string) []Node {
		outNodes := make([]Node, partitions)
		for partition := 0; partition < partitions; partition++ {
			outNodes[partition] = &DataSourceBuilder{
				Materializer:     materializer,
				PrimaryKeys:      primaryKeys,
				AvailableFilters: availableFilters,
				Filter:           NewConstant(true),
				Name:             name,
				Alias:            alias,
				Partition:        partition,
				Cardinality:      cardinality,
			}
		}
		return outNodes
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

	return dsb.Materializer(ctx, matCtx, dbConfig, dsb.Filter, dsb.Alias, dsb.Partition)
}

func (dsb *DataSourceBuilder) Metadata() *metadata.NodeMetadata {
	namespace := metadata.EmptyNamespace()
	namespace.AddPrefix(dsb.Alias)
	return metadata.NewNodeMetadata(dsb.Cardinality, octosql.NewVariableName(""), namespace)
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
