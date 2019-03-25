package redis

import (
	"fmt"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary: {
		physical.Equal: {},
	},
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	client *redis.Client
	alias  string // todo - ?
}

func NewDataSourceBuilderFactory(hostname, password string, port, dbIndex int) func(alias string) *physical.DataSourceBuilder {
	return physical.NewDataSourceBuilderFactory(
		func(filter physical.Formula, alias string) (execution.Node, error) {
			client := redis.NewClient(
				&redis.Options{
					Addr:     fmt.Sprintf("%s:%d", hostname, port),
					Password: password,
					DB:       dbIndex,
				},
			)

			return &DataSource{
				client: client,
				alias:  alias,
			}, nil
		},
		nil, // TODO - ?
		availableFilters,
	)
}

func (ds *DataSource) Get(variables octosql.Variables) (execution.RecordStream, error) {
	var allKeys *redis.ScanCmd

	switch len(variables) {
	case 1: // there is 1 key
		for k := range variables {
			allKeys = ds.client.Scan(0, k.String(), 0)
		}

	case 0: // taking all keys
		allKeys = ds.client.Scan(0, "*", 0)

	default:
		return nil, errors.Errorf("couldn't extract record for multiple variables")
	}

	return &RecordStream{
		client:     ds.client,
		dbIterator: allKeys.Iterator(),
		isDone:     false,
		alias:      ds.alias,
	}, nil
}

type RecordStream struct {
	client     *redis.Client
	dbIterator *redis.ScanIterator
	isDone     bool
	alias      string
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if !rs.dbIterator.Next() {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}
	key := rs.dbIterator.Val()

	recordValues, err := rs.client.HGetAll(key).Result()
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could't get hash for key %s", key))
	}

	aliasedRecord := make(map[octosql.VariableName]interface{})
	for k, v := range recordValues {
		fieldName := octosql.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, k))
		aliasedRecord[fieldName] = v
	}

	fieldNames := make([]octosql.VariableName, len(recordValues))
	for k := range aliasedRecord {
		fieldNames = append(fieldNames, k)
	}

	return execution.NewRecord(fieldNames, aliasedRecord), nil
}
