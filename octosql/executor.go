package octosql

import (
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/output"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
)

type OctosqlExecutor struct {
	dataSources *physical.DataSourceRepository
	output output.Output
	cfg *config.Config
	tokenizer *sqlparser.Tokenizer
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
	}, err
}