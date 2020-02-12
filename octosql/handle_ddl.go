package octosql

import (
	"context"
	"errors"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
)

// DDL SQL handler
// This handler takes every sql DDL statement (see parser/ast.go for more details) and tries to execute it
func (e *OctosqlExecutor) handleDDLStatement(ctx context.Context, statement *sqlparser.DDL) error {
	// Handle CREATE DATASOURCE statements
	if statement.Action == sqlparser.CreateDataSourceStr {

		// Create empty source configuration
		sourceConfig := config.DataSourceConfig{
			Name:   "",
			Type:   "",
			Config: map[string]interface{}{},
		}

		specs := statement.CrateDatasourceSpecs
		sourceName, _ := specs.TypeName.(*sqlparser.SQLVal)
		if sourceName.Type == sqlparser.StrVal {
			sourceConfig.Name = statement.Table.Name.String()
			sourceConfig.Type = string(sourceName.Val)
			for k, v := range statement.CrateDatasourceSpecs.OptionsSpecs.Options {
				sourceConfig.Config[k] = v
			}
		} else {
			// TODO: Maybe handle sql values in other way?
			return errors.New("Source name in CREATE DATASOURCE statement must be a string value.")
		}

		rep, err := physical.CreateDataSourceRepositoryFromConfig(e.dataSourceFactories, &config.Config{
			DataSources: []config.DataSourceConfig{ sourceConfig },
			Execution:   nil,
		})
		if err != nil {
			return err
		}
		newDataSources := append(e.cfg.DataSources, sourceConfig)

		// Add new data source
		err = e.dataSources.MergeFrom(*rep)
		if err != nil {
			return err
		}
		e.cfg.DataSources = newDataSources

		return nil
	}
	return errors.New("Unsupported DDL statement was used.")
}