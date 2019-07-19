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

	pendingRecords     []*Record
	pendingJoins       []<-chan RecordStream
	reachedEndOfStream bool
	errors             chan error
}

func NewJoiner(prefetchCount int, variables octosql.Variables, sourceStream RecordStream, joined Node) *Joiner {
	return &Joiner{
		prefetchCount: prefetchCount,
		variables:     variables,
		source:        sourceStream,
		joined:        joined,
		errors:        make(chan error),
	}
}

func (joiner *Joiner) fillPending() error {
	for len(joiner.pendingRecords) < joiner.prefetchCount && !joiner.reachedEndOfStream {
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
		joiner.pendingRecords = append(joiner.pendingRecords, srcRecord)
		joiner.pendingJoins = append(joiner.pendingJoins, joinedStreamChan)

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

func (joiner *Joiner) GetNextRecord() (*Record, RecordStream, error) {
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
	case outStream = <-joiner.pendingJoins[0]:
	}
	joiner.pendingRecords = joiner.pendingRecords[1:]
	joiner.pendingJoins = joiner.pendingJoins[1:]

	return outRecord, outStream, nil
}

func (joiner *Joiner) Close() error {
	err := joiner.source.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close source stream")
	}

	return nil
}
