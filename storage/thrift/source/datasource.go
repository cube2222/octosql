//
// Thrift data source implementation
//
//
package source

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/cube2222/octosql/storage/thrift/analyzer"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"sort"
)

// Filters for Thrift endpoint
var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   make(map[physical.Relation]struct{}),
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	alias        string
	thriftAddr   string
	protocol     string
	secure       bool
	thriftMeta   *analyzer.ThriftMeta
	inputStream  io.Reader
	outputStream io.Writer
	overrideIO   bool
}

func NewThriftDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string) (execution.Node, error) {

			thriftAddr, err := config.GetString(dbConfig, "ip", config.WithDefault("localhost:9090"))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get thrift server address")
			}

			protocol, err := config.GetString(dbConfig, "protocol", config.WithDefault("binary"))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get thrift server protocol")
			}

			secure, err := config.GetBool(dbConfig, "secure", config.WithDefault(false))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get thrift server secure option")
			}

			var thriftMeta *analyzer.ThriftMeta = nil
			thriftSpecsPath, err := config.GetString(dbConfig, "thriftSpecs", config.WithDefault(""))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get thrift specs option")
			}

			inputStream, err := config.GetInterface(dbConfig, "inputStream", config.WithDefault(nil))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get thrift inputStream option")
			}

			outputStream, err := config.GetInterface(dbConfig, "outputStream", config.WithDefault(nil))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get thrift outputStream option")
			}

			// Load thrift specification and parse it creating metadata
			if len(thriftSpecsPath) > 0 {
				thriftSpecsContent, err := ioutil.ReadFile(thriftSpecsPath)
				if err != nil {
					return nil, errors.Wrap(err, "couldn't open thrift specs file: "+thriftSpecsPath)
				}

				err, thriftMeta = analyzer.AnalyzeThriftSpecs(string(thriftSpecsContent))
				if err != nil {
					return nil, errors.Wrap(err, "couldn't load thrift specs file: "+err.Error())
				}
			} else {
				// Override thrift meta with inline string - thriftSpecsString
				thriftSpecsContents, err := config.GetString(dbConfig, "thriftSpecsString", config.WithDefault(""))
				if err != nil {
					return nil, errors.Wrap(err, "couldn't get thriftSpecsString option")
				}
				err, thriftMeta = analyzer.AnalyzeThriftSpecs(string(thriftSpecsContents))
				if err != nil {
					return nil, errors.Wrap(err, "couldn't load thrift specs file: "+err.Error())
				}
			}

			return &DataSource{
				alias:       alias,
				thriftAddr:  thriftAddr,
				protocol:    protocol,
				secure:      secure,
				thriftMeta:  thriftMeta,
				overrideIO: inputStream != nil || outputStream != nil,
				inputStream: inputStream.(io.Reader),
				outputStream: outputStream.(io.Writer),
			}, nil
		},
		nil,
		availableFilters,
		metadata.BoundedFitsInLocalStorage,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	return NewThriftDataSourceBuilderFactory(), nil
}

func (ds *DataSource) GetWithIOStreams(ctx context.Context, variables octosql.Variables, overrideIO bool, inputStream io.Reader, outputStream io.Writer) (execution.RecordStream, error) {
	return &RecordStream{
		isDone:                        false,
		alias:                         ds.alias,
		source:                        *ds,
		streamID:                      0,
		isOpen:                        false,
		thriftMeta:                    ds.thriftMeta,
		thriftConnection:              nil,
		overrideIO:                    overrideIO,
		overrideStreamIn:              inputStream,
		overrideStreamOut:             outputStream,
	}, nil
}

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables) (execution.RecordStream, error) {
	return ds.GetWithIOStreams(ctx, variables, ds.overrideIO, ds.inputStream, ds.outputStream)
}

type RecordStream struct {
	isDone                        bool
	alias                         string
	source                        DataSource
	streamID                      int32
	isOpen                        bool
	overrideIO                    bool
	overrideStreamOut             io.Writer
	overrideStreamIn              io.Reader
	thriftMeta                    *analyzer.ThriftMeta
	thriftConnection              *ThriftConnection
}

func (rs *RecordStream) Close() error {
	return nil
}

func GetErrorContextDescription(rs *RecordStream) string {
	return "Thrift data source \"" + rs.alias + "\""
}

func (rs *RecordStream) Next(ctx context.Context) (*execution.Record, error) {
	if rs.isDone {
		err := CloseClient(rs)
		if err != nil {
			return nil, err
		}
		return nil, execution.ErrEndOfStream
	}

	result, err := RunClient(rs)
	if err != nil {
		err.WithContextDescription(GetErrorContextDescription(rs))
		return nil, err
	}

	if len(result.Fields) == 0 {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	aliasedRecord := result.Fields

	fields := make([]octosql.VariableName, 0)
	for k := range aliasedRecord {
		fields = append(fields, k)
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i] < fields[j]
	})
	return execution.NewRecord(fields, aliasedRecord), nil
}
