package mysql

import (
	"database/sql"
	"fmt"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage/sqlStorages"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/physical"
)

type MySQLTemplate struct{}

func (t MySQLTemplate) GetAvailableFilters() map[physical.FieldType]map[physical.Relation]struct{} {
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

func (t MySQLTemplate) GetIPAddress(dbConfig map[string]interface{}) (string, int, error) {
	return config.GetIPAddress(dbConfig, "address", config.WithDefault([]interface{}{"localhost", 3306}))
}

func (t MySQLTemplate) GetConnection(user, password, host, dbName string, port int) (*sql.DB, error) {
	// Build dsn
	mysqlInfo := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", user, password, host, port, dbName)

	db, err := sql.Open("mysql", mysqlInfo)
	if err != nil {
		return nil, errors.Wrap(err, "couldn'template open connection to postgres database")
	}

	return db, nil
}

func (t MySQLTemplate) GetPlaceholders(alias string) sqlStorages.PlaceholderMap {
	return newMySQLPlaceholders(alias)
}

var template MySQLTemplate = struct{}{}

func NewDataSourceBuilderFactory(primaryKeys []octosql.VariableName) physical.DataSourceBuilderFactory {
	return sqlStorages.NewDataSourceBuilderFactoryFromTemplate(template)(primaryKeys)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	primaryKeysStrings, err := config.GetStringList(dbConfig, "primaryKeys", config.WithDefault([]string{}))
	if err != nil {
		return nil, errors.Wrap(err, "couldn'template get primaryKeys")
	}
	var primaryKeys []octosql.VariableName
	for _, str := range primaryKeysStrings {
		primaryKeys = append(primaryKeys, octosql.NewVariableName(str))
	}

	return NewDataSourceBuilderFactory(primaryKeys), nil
}
