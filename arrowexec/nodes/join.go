package nodes

import (
	"runtime"

	"github.com/cube2222/octosql/arrowexec/execution"
	"github.com/cube2222/octosql/arrowexec/nodes/hashtable"
)

type StreamJoin struct {
	Left, Right                     execution.NodeWithMeta
	LeftKeyIndices, RightKeyIndices []int
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
			panic("implement me")
		}
	}()

	go func() {
		defer close(rightRecordChannel)
		if err := s.Right.Node.Run(ctx, func(produceCtx execution.ProduceContext, record execution.Record) error {
			rightRecordChannel <- record
			return nil
		}); err != nil {
			panic("implement me")
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
	var tableKeyIndices, joinedKeyIndices []int
	var joinedRecordChannel chan execution.Record
	var tableIsLeft bool
	if leftClosed {
		tableRecords = leftRecords
		joinedRecords = rightRecords
		tableKeyIndices = s.LeftKeyIndices
		joinedKeyIndices = s.RightKeyIndices
		joinedRecordChannel = rightRecordChannel
		tableIsLeft = true
	} else {
		tableRecords = rightRecords
		joinedRecords = leftRecords
		tableKeyIndices = s.RightKeyIndices
		joinedKeyIndices = s.LeftKeyIndices
		joinedRecordChannel = leftRecordChannel
		tableIsLeft = false
	}

	table := hashtable.BuildJoinTable(tableRecords, tableKeyIndices, joinedKeyIndices, tableIsLeft)

	outputRecordChannelChan := make(chan (<-chan execution.Record), runtime.GOMAXPROCS(0))
	go func() {
		defer close(outputRecordChannelChan)
		processJoinedRecord := func(joinedRecord execution.Record) {
			outputRecordChannel := make(chan execution.Record, 8)
			outputRecordChannelChan <- outputRecordChannel

			go func() {
				defer close(outputRecordChannel)
				table.JoinWithRecord(joinedRecord, func(record execution.Record) {
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
