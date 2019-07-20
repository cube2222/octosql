package execution

import (
	"github.com/cube2222/octosql"
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

func (joiner *Joiner) fillPending() error {
	for joiner.elements < joiner.prefetchCount && !joiner.reachedEndOfStream {
		srcRecord, err := joiner.source.Next()
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
			joinedStream, err := joiner.joined.Get(variables)
			if err != nil {
				joiner.errors <- errors.Wrap(err, "couldn't get joined stream")
			}

			joinedStreamChan <- joinedStream
		}()
	}

	return nil
}

var i = 0

func (joiner *Joiner) GetNextRecord() (*Record, RecordStream, error) {
	i = (i + 1) % 13
	err := joiner.fillPending()
	if err != nil {
		return nil, nil, errors.Wrap(err, "could't fill pending record queue")
	}

	if len(joiner.pendingRecords) == 0 {
		return nil, nil, ErrEndOfStream
	}

	outRecord := joiner.pendingRecords[0]
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
