package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Aggregate interface {
	/* Aggregates 1 column (aggregated) from record set */
	Aggregate(bucketValue []*Record, aggregated octosql.VariableName) (interface{}, error)
}

type Count struct {
}

func NewCount() Aggregate {
	return &Count{}
}

func (aggr *Count) Aggregate(bucketValue []*Record, aggregated octosql.VariableName) (interface{}, error) {
	result := 0

	for i := 0; i < len(bucketValue); i++ {
		record := bucketValue[i]
		recordData := record.Value(aggregated)
		if recordData != nil {
			result++
		}
	}

	return result, nil
}

type Max struct {
}

func NewMax() Aggregate {
	return &Max{}
}

func (aggr *Max) Aggregate(bucketValue []*Record, aggregated octosql.VariableName) (interface{}, error) {

	/* TODO */
	for record := range bucketValue {
		recordData := record.Value(aggregated)

		if recordData != nil {
		}
	}

}

type Min struct {
}

func NewMin() Aggregate {
	return &Min{}
}

func (aggr *Min) Aggregate(bucketValue []*Record, aggregated octosql.VariableName) (interface{}, error) {

}

type Avg struct {
}

func NewAvg() Aggregate {
	return &Avg{}
}

func (aggr *Avg) Aggregate(bucketValue []*Record, aggregated octosql.VariableName) (interface{}, error) {

}

type GroupBy struct {
	selectExpr []NamedExpression
	source     Node
	group      []NamedExpression             // po czym grupujemy
	aggregates map[NamedExpression]Aggregate // zbior agregatow: agregat -> co agreguje
}

func NewGroupBy(selectExpr []NamedExpression, source Node, group []NamedExpression, aggregates map[NamedExpression]Aggregate) *GroupBy {
	return &GroupBy{selectExpr: selectExpr, source: source, group: group, aggregates: aggregates}
}

func (node *GroupBy) Get(variables octosql.Variables) (RecordStream, error) {
	stream, err := node.source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get stream for source in group by")
	}

	recordBucket := newRecordSet() // buckets with records

	for { /* Creating record set */
		record, err := stream.Next()
		if err != nil {
			if err == ErrEndOfStream {
				break
			}
			return nil, errors.Wrap(err, "couldn't get record from stream in GroupByStream")
		}

		// this will be a key in recordBucket
		groupElemFields := make([]octosql.VariableName, len(node.group))
		groupElemData := make(map[octosql.VariableName]interface{}, len(node.group))

		/* TODO - creating bucket key */
		for i := 0; i < len(node.group); i++ {
			exprName := node.group[i].Name()

			groupElemFields[i] = octosql.NewVariableName(exprName.String())
			groupElemData[groupElemFields[i]] = record.Value(groupElemFields[i])
		}

		/* TODO - Adding element to record set */
		recordBucketKey := NewRecord(groupElemFields, groupElemData)

		err = recordBucket.InsertAll(recordBucketKey)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get hash of record while inserting")
		}
	}

	outRecords := make([]Record, len(recordBucket.set))

	for key, records := range recordBucket.set {
		for i := 0; i < len(node.selectExpr); i++ {
			columnName := node.selectExpr[i].Name()

		}
	}

	return &GroupByStream{
		index:   0,
		records: outRecords,
	}, nil
}

type GroupByStream struct {
	index   int
	records []Record
}

func (node *GroupByStream) Next() (*Record, error) {
	if node.index < len(node.records) {
		record := node.records[node.index]
		node.index++
		return &record, nil
	}

	return nil, ErrEndOfStream
}
