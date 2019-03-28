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
	client  *redis.Client
	allKeys map[octosql.VariableName]interface{}
	alias   string
}

func NewDataSourceBuilderFactory(hostname, password string, port, dbIndex int, dbKey []octosql.VariableName) func(alias string) *physical.DataSourceBuilder {
	return physical.NewDataSourceBuilderFactory(
		func(filter physical.Formula, alias string) (execution.Node, error) {
			client := redis.NewClient(
				&redis.Options{
					Addr:     fmt.Sprintf("%s:%d", hostname, port),
					Password: password,
					DB:       dbIndex,
				},
			)

			allKeys, err := GetAllKeys(filter, dbKey[0])
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get all keys from filter")
			}

			return &DataSource{
				client:  client,
				allKeys: allKeys,
				alias:   alias,
			}, nil
		},
		dbKey,
		availableFilters,
	)
}

func (ds *DataSource) Get(variables octosql.Variables) (execution.RecordStream, error) {
	var allKeys *redis.ScanCmd

	if len(ds.allKeys) == 0 {
		allKeys = ds.client.Scan(0, "*", 0)

		return &EntireBaseStream{
			client:     ds.client,
			dbIterator: allKeys.Iterator(),
			isDone:     false,
			alias:      ds.alias,
		}, nil
	}

	sliceKeys := make([]string, len(ds.allKeys))
	for k := range ds.allKeys {
		value, err := variables.Get(k)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get value from variables map")
		}

		switch value := value.(type) {
		case string:
			sliceKeys = append(sliceKeys, value)

		default:
			return nil, errors.Errorf("wrong value type for key")
		}
	}

	return &KeySpecificStream{
		client:  ds.client,
		keys:    sliceKeys,
		counter: 0,
		isDone:  false,
		alias:   ds.alias,
	}, nil
}

type RecordStream interface{}

type KeySpecificStream struct {
	client  *redis.Client
	keys    []string
	counter int
	isDone  bool
	alias   string
}

type EntireBaseStream struct {
	client     *redis.Client
	dbIterator *redis.ScanIterator
	isDone     bool
	alias      string
}

func (rs *KeySpecificStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if rs.counter == len(rs.keys) {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	key := rs.keys[rs.counter]
	rs.counter++

	return GetNewRecord(rs.client, rs.alias, key)
}

func (rs *EntireBaseStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if !rs.dbIterator.Next() {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}
	key := rs.dbIterator.Val()

	return GetNewRecord(rs.client, rs.alias, key)
}

func GetNewRecord(client *redis.Client, key, alias string) (*execution.Record, error) {
	recordValues, err := client.HGetAll(key).Result()
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could't get hash for key %s", key))
	}

	aliasedRecord := make(map[octosql.VariableName]interface{})
	for k, v := range recordValues {
		fieldName := octosql.NewVariableName(fmt.Sprintf("%s.%s", alias, k))
		aliasedRecord[fieldName] = v
	}

	fieldNames := make([]octosql.VariableName, len(recordValues))
	for k := range aliasedRecord {
		fieldNames = append(fieldNames, k)
	}

	return execution.NewRecord(fieldNames, aliasedRecord), nil
}
