package nodes

import (
	"fmt"
	"runtime"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/cube2222/octosql/arrowexec/execution"
	"github.com/cube2222/octosql/arrowexec/nodes/hashtable"
)

type StreamJoin struct {
	Left, Right       *execution.NodeWithMeta
	LeftKey, RightKey []execution.Expression
}

func (s *StreamJoin) Run(ctx execution.Context, produce execution.ProduceFunc) error {
	// TODO: The channel should also be able to pass errors.
	leftRecordChannel := make(chan execution.Record, 8)
	rightRecordChannel := make(chan execution.Record, 8)

	go func() {
		defer close(leftRecordChannel)
		if err := s.Left.Node.Run(ctx, func(produceCtx execution.ProduceContext, record execution.Record) error {
			leftRecordChannel <- record
			return nil
		}); err != nil {
			panic(fmt.Errorf("implement error handling: %w", err))
		}
	}()

	go func() {
		defer close(rightRecordChannel)
		if err := s.Right.Node.Run(ctx, func(produceCtx execution.ProduceContext, record execution.Record) error {
			rightRecordChannel <- record
			return nil
		}); err != nil {
			panic(fmt.Errorf("implement error handling: %w", err))
		}
	}()

	var leftClosed bool
	var leftRecords, rightRecords []execution.Record
receiveLoop:
	for {
		select {
		case record, ok := <-leftRecordChannel:
			if !ok {
				leftClosed = true
				break receiveLoop
			}
			leftRecords = append(leftRecords, record)
		case record, ok := <-rightRecordChannel:
			if !ok {
				break receiveLoop
			}
			rightRecords = append(rightRecords, record)
		}
	}
	var tableRecords, joinedRecords []execution.Record
	var tableKeyExprs, joinedKeyExprs []execution.Expression
	var joinedRecordChannel chan execution.Record
	var tableIsLeft bool
	if leftClosed {
		tableRecords = leftRecords
		joinedRecords = rightRecords
		tableKeyExprs = s.LeftKey
		joinedKeyExprs = s.RightKey
		joinedRecordChannel = rightRecordChannel
		tableIsLeft = true
	} else {
		tableRecords = rightRecords
		joinedRecords = leftRecords
		tableKeyExprs = s.RightKey
		joinedKeyExprs = s.LeftKey
		joinedRecordChannel = leftRecordChannel
		tableIsLeft = false
	}

	tableKeyColumns := make([][]arrow.Array, len(tableRecords))
	// TODO: Parallelize.
	for recordIndex, record := range tableRecords {
		tableKeyColumns[recordIndex] = make([]arrow.Array, len(tableKeyExprs))
		for i, expr := range tableKeyExprs {
			exprValue, err := expr.Evaluate(ctx, record)
			if err != nil {
				return fmt.Errorf("couldn't evaluate key expression: %w", err)
			}
			tableKeyColumns[recordIndex][i] = exprValue
		}
	}

	table := hashtable.BuildJoinTable(tableRecords, tableKeyColumns, tableIsLeft)

	outputRecordChannelChan := make(chan (<-chan execution.Record), runtime.GOMAXPROCS(0))
	go func() {
		defer close(outputRecordChannelChan)
		processJoinedRecord := func(joinedRecord execution.Record) {
			outputRecordChannel := make(chan execution.Record, 8)
			outputRecordChannelChan <- outputRecordChannel

			go func() {
				keys := make([]arrow.Array, len(joinedKeyExprs))
				for i, expr := range joinedKeyExprs {
					exprValue, err := expr.Evaluate(ctx, joinedRecord)
					if err != nil {
						panic("implement me")
					}
					keys[i] = exprValue
				}
				defer close(outputRecordChannel)
				table.JoinWithRecord(joinedRecord, keys, func(record execution.Record) {
					outputRecordChannel <- record
				})
			}()
		}
		for _, joinedRecord := range joinedRecords {
			processJoinedRecord(joinedRecord)
		}
		for joinedRecord := range joinedRecordChannel {
			processJoinedRecord(joinedRecord)
		}
	}()

	for outputRecordChannel := range outputRecordChannelChan {
		for record := range outputRecordChannel {
			if err := produce(execution.ProduceContext{Context: ctx}, record); err != nil {
				return err
			}
		}
	}

	return nil
}
