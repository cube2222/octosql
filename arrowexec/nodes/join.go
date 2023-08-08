package nodes

import (
	"github.com/cube2222/octosql/arrowexec/execution"
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
		if err := s.Left.Node.Run(ctx, func(produceCtx execution.ProduceContext, record execution.Record) error {
			leftRecordChannel <- record
			return nil
		}); err != nil {
			panic("implement me")
		}
	}()

	go func() {
		if err := s.Right.Node.Run(ctx, func(produceCtx execution.ProduceContext, record execution.Record) error {
			rightRecordChannel <- record
			return nil
		}); err != nil {
			panic("implement me")
		}
	}()

	var leftRecords, rightRecords []execution.Record
receiveLoop:
	for {
		select {
		case record, ok := <-leftRecordChannel:
			if !ok {
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

	return nil
}
