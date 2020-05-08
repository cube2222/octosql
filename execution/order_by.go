package execution

import (
	"context"
	"reflect"
	"sort"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"

	"github.com/pkg/errors"
)

type OrderDirection string

const (
	Ascending  OrderDirection = "asc"
	Descending OrderDirection = "desc"
)

type OrderBy struct {
	expressions []Expression
	directions  []OrderDirection
	source      Node
}

func NewOrderBy(exprs []Expression, directions []OrderDirection, source Node) *OrderBy {
	return &OrderBy{
		expressions: exprs,
		directions:  directions,
		source:      source,
	}
}

func isSorteable(x octosql.Value) bool {
	switch x.GetType() {
	case octosql.TypeBool:
		return true
	case octosql.TypeInt:
		return true
	case octosql.TypeFloat:
		return true
	case octosql.TypeString:
		return true
	case octosql.TypeTime:
		return true
	case octosql.TypeNull, octosql.TypePhantom, octosql.TypeDuration, octosql.TypeTuple, octosql.TypeObject:
		return false
	}

	panic("unreachable")
}

func (ob *OrderBy) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	sourceStream, execOutput, err := ob.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get underlying stream in order by")
	}

	orderedStream, err := createOrderedStream(ctx, ob.expressions, ob.directions, variables, sourceStream)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create ordered stream from source stream")
	}

	return orderedStream, NewExecutionOutput(NewZeroWatermarkGenerator(), execOutput.NextShuffles, execOutput.TasksToRun), nil
}

func createOrderedStream(ctx context.Context, expressions []Expression, directions []OrderDirection, variables octosql.Variables, sourceStream RecordStream) (stream RecordStream, outErr error) {
	records := make([]*Record, 0)

	for {
		rec, err := sourceStream.Next(ctx)
		if err == ErrEndOfStream {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "couldn't get all records")
		}

		records = append(records, rec)
	}

	defer func() {
		if err := recover(); err != nil {
			stream = nil
			outErr = errors.Wrap(err.(error), "couldn't sort records")
		}
	}()
	sort.Slice(records, func(i, j int) bool {
		iRec := records[i]
		jRec := records[j]

		for num, expr := range expressions {
			// TODO: Aggressive caching of these expressions...
			iVars, err := variables.MergeWith(iRec.AsVariables())
			if err != nil {
				panic(errors.Wrap(err, "couldn't merge variables"))
			}
			jVars, err := variables.MergeWith(jRec.AsVariables())
			if err != nil {
				panic(errors.Wrap(err, "couldn't merge variables"))
			}

			x, err := expr.ExpressionValue(ctx, iVars)
			if err != nil {
				panic(errors.Wrapf(err, "couldn't get order by expression with index %v value", num))
			}
			y, err := expr.ExpressionValue(ctx, jVars)
			if err != nil {
				panic(errors.Wrapf(err, "couldn't get order by expression with index %v value", num))
			}

			if !isSorteable(x) {
				panic(errors.Errorf("value %v of type %v is not comparable", x, reflect.TypeOf(x).String()))
			}
			if !isSorteable(y) {
				panic(errors.Errorf("value %v of type %v is not comparable", y, reflect.TypeOf(y).String()))
			}

			cmp, err := octosql.Compare(x, y)
			if err != nil {
				panic(errors.Errorf("failed to compare values %v and %v", x, y))
			}

			answer := false

			if cmp == 0 {
				continue
			} else if cmp > 0 {
				answer = true
			}

			if directions[num] == Ascending {
				answer = !answer
			}

			return answer
		}

		return false
	})

	return NewInMemoryStream(ctx, records), nil
}
