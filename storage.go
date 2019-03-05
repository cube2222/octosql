package octosql

import "context"

/* TODO: To też w sumie niepotrzebne, bo to filtrowanie bez predykatów.
type GetAll interface {
	GetAll(ctx context.Context) (RecordStream, error)
}*/

/*
TODO: Może tak, może nie?
Zobaczymy, czy warto mieć taki single record Get. Ale to chyba przerost formy nad treścią,
jak wszędzie trzeba będzie operować na strumieniach.
type GetByPrimaryKey interface {
	GetByPrimaryKey(ctx context.Context, key interface{}) (*Record, error)
}*/

type FieldType string

// TODO: composite primary key?
const (
	Primary   FieldType = "primary"
	Secondary FieldType = "secondary"
)

type FilterType string

const (
	Equal    FilterType = "equal"
	NotEqual FilterType = "not_equal"
	MoreThan FilterType = "more_than"
	LessThan FilterType = "less_than"
	Like     FilterType = "like"
	In       FilterType = "in"
)

// TODO: Co jesli zaleznosc miedzy kolumnami?
type Predicate struct {
	Field  string
	Filter FilterType
}

// For databases this should return a static list of available filters based on the database type
// and probably initialize some templated query for the given predicates when asked to initialize.
// For operators (like where or join) this should return filters based on the filters in the data sources under this.
// For a file data source this could create an index for the predicated fields to achieve faster access.
type DataSourceDescription interface {
	Initialize(ctx context.Context, predicates []Predicate) (DataSource, error)
	AvailableFilters() map[FieldType]map[FilterType]struct{}
}

type DataSource interface {
	Get(predicateValues []interface{}) (RecordStream, error)
}
