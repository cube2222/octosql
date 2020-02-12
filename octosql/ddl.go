package octosql

import (
	"context"
	"errors"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
)


func (e *OctosqlExecutor) handleDDLStatement(ctx context.Context, statement *sqlparser.DDL) error {
	if statement.Action == sqlparser.CreateDataSourceStr {

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
		}
		// TODO: Handle invalid source names
		rep, err := physical.CreateDataSourceRepositoryFromConfig(e.dataSourceFactories, &config.Config{
			DataSources: []config.DataSourceConfig{ sourceConfig },
			Execution:   nil,
		})
		if err != nil {
			return err
		}
		newDataSources := append(e.cfg.DataSources, sourceConfig)

		err = e.dataSources.MergeFrom(*rep)
		if err != nil {
			return err
		}
		e.cfg.DataSources = newDataSources

		return nil
	}
	return errors.New("Unsupported DDL statement was used.")
}