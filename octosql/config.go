package octosql

import (
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/physical"
	"os"
)

// Load configuration from the file using provided path
func (e *OctosqlExecutor) LoadConfiguration(configPath string) error {

	cfg := &config.Config{
		DataSources: []config.DataSourceConfig{},
		Execution:   map[string]interface{}{},
	}

	if _, err := os.Stat(configPath); err == nil {
		cfg, err = config.ReadConfig(configPath)
		if err != nil {
			return err
		}
	}

	dataSourceRespository, err := physical.CreateDataSourceRepositoryFromConfig(
		e.dataSourceFactories,
		cfg,
	)
	if err != nil {
		return err
	}

	err = e.dataSources.MergeFrom(*dataSourceRespository)
	if err != nil {
		return err
	}

	e.cfg = cfg
	return nil
}