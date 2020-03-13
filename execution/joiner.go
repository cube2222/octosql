package execution

import (
	"github.com/cube2222/octosql"

	"context"

	"github.com/pkg/errors"
)

// Joiner is used to join one source stream with another datasource.
type Joiner struct {
	prefetchCount int

	variables octosql.Variables
	source    RecordStream
	joined    Node

	ringBegin, elements int
	pendingRecords      []*Record
	pendingJoins        []<-chan RecordStream

	reachedEndOfStream bool
	errors             chan error
}

func NewJoiner(prefetchCount int, variables octosql.Variables, sourceStream RecordStream, joined Node) *Joiner {
	return &Joiner{
		prefetchCount:  prefetchCount,
		variables:      variables,
		source:         sourceStream,
		joined:         joined,
		pendingRecords: make([]*Record, prefetchCount),
		pendingJoins:   make([]<-chan RecordStream, prefetchCount),
		errors:         make(chan error),
	}
}

func (joiner *Joiner) fillPending(ctx context.Context) error {
	for joiner.elements < joiner.prefetchCount && !joiner.reachedEndOfStream {
		srcRecord, err := joiner.source.Next(ctx)
		if err != nil {
			if err == ErrEndOfStream {
				joiner.reachedEndOfStream = true
				return nil
			}
			return errors.Wrap(err, "couldn't get source record")
		}

		variables, err := joiner.variables.MergeWith(srcRecord.AsVariables())
		if err != nil {
			return errors.Wrap(err, "couldn't merge given variables with source record variables")
		}

		joinedStreamChan := make(chan RecordStream)
		newPos := (joiner.ringBegin + joiner.elements) % joiner.prefetchCount
		joiner.pendingRecords[newPos] = srcRecord
		joiner.pendingJoins[newPos] = joinedStreamChan
		joiner.elements++

		go func() {
			joinedStream, _, err := joiner.joined.Get(ctx, variables, GetRawStreamID()) // TODO: This needs to be changed in the new joiner implementation.
			if err != nil {
				joiner.errors <- errors.Wrap(err, "couldn't get joined stream")
			}

			joinedStreamChan <- joinedStream
		}()
	}

	return nil
}

func (joiner *Joiner) GetNextRecord(ctx context.Context) (*Record, RecordStream, error) {
	err := joiner.fillPending(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could't fill pending record queue")
	}

	if joiner.elements == 0 {
		return nil, nil, ErrEndOfStream
	}

	outRecord := joiner.pendingRecords[joiner.ringBegin]
	var outStream RecordStream
	select {
	case err := <-joiner.errors:
		return nil, nil, errors.Wrap(err, "couldn't get joined stream from worker")
	case outStream = <-joiner.pendingJoins[joiner.ringBegin]:
	}
	joiner.ringBegin = (joiner.ringBegin + 1) % joiner.prefetchCount
	joiner.elements--

	return outRecord, outStream, nil
}

func (joiner *Joiner) Close() error {
	err := joiner.source.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close source stream")
	}

	return nil
}
