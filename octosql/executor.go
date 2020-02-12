package octosql

import (
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/output"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage/csv"
	"github.com/cube2222/octosql/storage/excel"
	"github.com/cube2222/octosql/storage/json"
	"github.com/cube2222/octosql/storage/mysql"
	"github.com/cube2222/octosql/storage/postgres"
	"github.com/cube2222/octosql/storage/redis"
)

var DEFAULT_FACTORIES = map[string]physical.Factory{
	"csv":      csv.NewDataSourceBuilderFactoryFromConfig,
	"json":     json.NewDataSourceBuilderFactoryFromConfig,
	"mysql":    mysql.NewDataSourceBuilderFactoryFromConfig,
	"postgres": postgres.NewDataSourceBuilderFactoryFromConfig,
	"redis":    redis.NewDataSourceBuilderFactoryFromConfig,
	"excel":    excel.NewDataSourceBuilderFactoryFromConfig,
}

type OctosqlExecutor struct {
	dataSources *physical.DataSourceRepository
	output output.Output
	cfg *config.Config
	tokenizer *sqlparser.Tokenizer
	dataSourceFactories map[string]physical.Factory
}

func NewOctosqlExecutor() (*OctosqlExecutor, error) {
	outputHandler, err := translateOutputName(DEFAULT_OUTPUT_FORMAT)
	if err != nil {
		return nil, err
	}
	return &OctosqlExecutor{
		dataSources: physical.NewDataSourceRepository(),
		output: outputHandler,
		cfg: nil,
		tokenizer: nil,
		dataSourceFactories: DEFAULT_FACTORIES,
	}, err
}