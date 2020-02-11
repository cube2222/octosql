package octosql

import (
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage/csv"
	"github.com/cube2222/octosql/storage/excel"
	"github.com/cube2222/octosql/storage/json"
	"github.com/cube2222/octosql/storage/mysql"
	"github.com/cube2222/octosql/storage/postgres"
	"github.com/cube2222/octosql/storage/redis"
)

func (e *OctosqlExecutor) LoadConfiguration(configPath string) error {
	cfg, err := config.ReadConfig(configPath)
	if err != nil {
		return err
	}

	dataSourceRespository, err := physical.CreateDataSourceRepositoryFromConfig(
		map[string]physical.Factory{
			"csv":      csv.NewDataSourceBuilderFactoryFromConfig,
			"json":     json.NewDataSourceBuilderFactoryFromConfig,
			"mysql":    mysql.NewDataSourceBuilderFactoryFromConfig,
			"postgres": postgres.NewDataSourceBuilderFactoryFromConfig,
			"redis":    redis.NewDataSourceBuilderFactoryFromConfig,
			"excel":    excel.NewDataSourceBuilderFactoryFromConfig,
		},
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