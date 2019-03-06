package octosql

import "context"

type FieldType string

// TODO: composite primary key?
const (
	Primary   FieldType = "primary"
	Secondary FieldType = "secondary"
)

// For databases this should return a static list of available filters based on the database type
// and probably initialize some templated query for the given predicates when asked to initialize.
// For operators (like where or join) this should return filters based on the filters in the data sources under this.
// For a file data source this could create an index for the predicated fields to achieve faster access.
type DataSourceDescription interface {
	Initialize(ctx context.Context, predicates Formula) (DataSource, error)
	PrimaryKeys() map[string]struct{}
	AvailableFilters() map[FieldType]map[RelationType]struct{}
}

type DataSource interface {
	Get(primitiveValues map[string]interface{}) (RecordStream, error)
}
