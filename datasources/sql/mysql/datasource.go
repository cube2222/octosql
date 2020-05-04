package mysql

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/datasources/sql"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/physical"
)

type MySQLTemplate struct{}

func (t *MySQLTemplate) GetAvailableFilters() map[physical.FieldType]map[physical.Relation]struct{} {
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

func (t *MySQLTemplate) GetIPAddress(dbConfig map[string]interface{}) (string, int, error) {
	return config.GetIPAddress(dbConfig, "address", config.WithDefault([]interface{}{"localhost", 3306}))
}

func (t *MySQLTemplate) GetDSNAndDriverName(user, password, host, dbName string, port int) (string, string) {
	// Build dsn
	mysqlInfo := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", user, password, host, port, dbName)

	return mysqlInfo, "mysql"
}

func (t *MySQLTemplate) GetPlaceholders(alias string) sql.PlaceholderMap {
	return newMySQLPlaceholders(alias)
}

var template = &MySQLTemplate{}

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
