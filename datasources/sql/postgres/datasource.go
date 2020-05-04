package postgres

import (
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/datasources/sql"
	"github.com/cube2222/octosql/physical"
)

type PostgresTemplate struct{}

func (t *PostgresTemplate) GetAvailableFilters() map[physical.FieldType]map[physical.Relation]struct{} {
	return map[physical.FieldType]map[physical.Relation]struct{}{
		physical.Primary: {
			physical.Equal:        {},
			physical.NotEqual:     {},
			physical.MoreThan:     {},
			physical.LessThan:     {},
			physical.GreaterEqual: {},
			physical.LessEqual:    {},
			physical.Like:         {},
		},
		physical.Secondary: {
			physical.Equal:        {},
			physical.NotEqual:     {},
			physical.MoreThan:     {},
			physical.LessThan:     {},
			physical.Like:         {},
			physical.GreaterEqual: {},
			physical.LessEqual:    {},
		},
	}
}

func (t *PostgresTemplate) GetIPAddress(dbConfig map[string]interface{}) (string, int, error) {
	return config.GetIPAddress(dbConfig, "address", config.WithDefault([]interface{}{"localhost", 5432}))
}

func (t *PostgresTemplate) GetDSNAndDriverName(user, password, host, dbName string, port int) (string, string) {
	// Build dsn
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("host=%s port=%d user=%s ", host, port, user))
	if password != "" {
		sb.WriteString(fmt.Sprintf("password=%s ", password))
	}
	sb.WriteString(fmt.Sprintf("dbname=%s sslmode=disable", dbName))

	psqlInfo := sb.String()

	return psqlInfo, "postgres"
}

func (t *PostgresTemplate) GetPlaceholders(alias string) sql.PlaceholderMap {
	return newPostgresPlaceholders(alias)
}

var template = &PostgresTemplate{}

func NewDataSourceBuilderFactory(primaryKeys []octosql.VariableName) physical.DataSourceBuilderFactory {
	return sql.NewDataSourceBuilderFactoryFromTemplate(template)(primaryKeys)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	primaryKeysStrings, err := config.GetStringList(dbConfig, "primaryKeys", config.WithDefault([]string{}))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get primaryKeys")
	}

	var primaryKeys []octosql.VariableName
	for _, str := range primaryKeysStrings {
		primaryKeys = append(primaryKeys, octosql.NewVariableName(str))
	}

	return NewDataSourceBuilderFactory(primaryKeys), nil
}
