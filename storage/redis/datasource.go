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
	client     *redis.Client
	keyFormula KeyFormula
	alias      string
}

// dbKey is the name for hard-coded key alias used in future formulas for redis queries
func NewDataSourceBuilderFactory(hostname, password string, port, dbIndex int, dbKey string) func(alias string) *physical.DataSourceBuilder {
	return physical.NewDataSourceBuilderFactory(
		func(filter physical.Formula, alias string) (execution.Node, error) {
			client := redis.NewClient(
				&redis.Options{
					Addr:     fmt.Sprintf("%s:%d", hostname, port),
					Password: password,
					DB:       dbIndex,
				},
			)

			keyFormula, err := NewKeyFormula(filter, dbKey, alias)
			if err != nil {
				return nil, errors.Errorf("couldn't create KeyFormula")
			}

			return &DataSource{
				client:     client,
				keyFormula: keyFormula,
				alias:      alias,
			}, nil
		},
		[]octosql.VariableName{
			octosql.NewVariableName(dbKey),
		},
		availableFilters,
	)
}

func (ds *DataSource) Get(variables octosql.Variables) (execution.RecordStream, error) {
	keysWanted, err := ds.keyFormula.getAllKeys(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get all keys from filter")
	}

	if len(keysWanted.keys) == 0 {
		allKeys := ds.client.Scan(0, "*", 0)

		return &EntireDatabaseStream{
			client:     ds.client,
			dbIterator: allKeys.Iterator(),
			isDone:     false,
			alias:      ds.alias,
		}, nil
	}

	sliceKeys := make([]string, 0)
	for k := range keysWanted.keys {
		sliceKeys = append(sliceKeys, k)
	}

	return &KeySpecificStream{
		client:  ds.client,
		keys:    sliceKeys,
		counter: 0,
		isDone:  false,
		alias:   ds.alias,
	}, nil
}

type KeySpecificStream struct {
	client  *redis.Client
	keys    []string
	counter int
	isDone  bool
	alias   string
}

type EntireDatabaseStream struct {
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

	return GetNewRecord(rs.client, key, rs.alias)
}

func (rs *EntireDatabaseStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if !rs.dbIterator.Next() {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}
	key := rs.dbIterator.Val()

	record, err := GetNewRecord(rs.client, key, rs.alias)
	if err != nil {
		if err == execution.ErrNotFound { // key was not in redis database so we skip it
			return rs.Next()
		}
		return nil, err
	}

	return record, nil
}

func GetNewRecord(client *redis.Client, key, alias string) (*execution.Record, error) {
	recordValues, err := client.HGetAll(key).Result()
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could't get hash for key %s", key))
	}

	// We skip this record
	if len(recordValues) == 0 {
		return nil, execution.ErrNotFound
	}

	recordValues["key"] = key

	aliasedRecord := make(map[octosql.VariableName]interface{})
	for k, v := range recordValues {
		fieldName := octosql.NewVariableName(fmt.Sprintf("%s.%s", alias, k))
		aliasedRecord[fieldName] = v
	}

	fieldNames := make([]octosql.VariableName, 0)
	for k := range aliasedRecord {
		fieldNames = append(fieldNames, k)
	}

	aliasedRecord, ok := execution.NormalizeType(aliasedRecord).(map[octosql.VariableName]interface{})
	if !ok {
		return nil, errors.Errorf("couldn't normalize aliased map to map[octosql.VariableName]interface{}")
	}

	return execution.NewRecord(fieldNames, aliasedRecord), nil
}
