package execution

import (
	"context"
	"runtime"

	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

// LeftJoin currently only supports lookup joins.
type LeftJoin struct {
	source Node
	joined Node
}

func NewLeftJoin(source Node, joined Node) *LeftJoin {
	return &LeftJoin{source: source, joined: joined}
}

func (node *LeftJoin) Get(variables octosql.Variables) (RecordStream, error) {
	recordStream, err := node.source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream")
	}

	ctx, cancel := context.WithCancel(context.Background())

	stream := &LeftJoinedStream{
		variables:       variables,
		source:          recordStream,
		joined:          node.joined,
		curRecord:       nil,
		output:          make(chan (<-chan *Record), 32),
		workerCtx:       ctx,
		workerCtxCancel: cancel,
		workToDo:        make(chan joinTask, 32),
	}

	go stream.inputWorker()
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go stream.joinWorker()
	}

	return stream, nil
}

type LeftJoinedStream struct {
	variables octosql.Variables
	source    RecordStream
	joined    Node

	curRecord <-chan *Record

	output          chan (<-chan *Record)
	workerCtx       context.Context
	workerCtxCancel context.CancelFunc
	workToDo        chan joinTask
}

type joinTask struct {
	srcRecord *Record
	output    chan<- *Record
	//TODO: Error channel
}

func (stream *LeftJoinedStream) inputWorker() error {
	for {
		select {
		case <-stream.workerCtx.Done():
			close(stream.output)
			return stream.workerCtx.Err()
		default:
		}
		srcRecord, err := stream.source.Next()
		if err != nil {
			if err == ErrEndOfStream {
				close(stream.output)
				return nil
			}
			return errors.Wrap(err, "couldn't get source record")
		}

		rowOutput := make(chan *Record, 16)
		task := joinTask{
			srcRecord: srcRecord,
			output:    rowOutput,
		}

		stream.workToDo <- task
		stream.output <- rowOutput
	}
}

func (stream *LeftJoinedStream) joinWorker() error {
	for {
		var task joinTask
		select {
		case task = <-stream.workToDo:
		case <-stream.workerCtx.Done():
			return stream.workerCtx.Err()
		}
		srcRecord := task.srcRecord
		joinedAnyRecord := false

		variables, err := stream.variables.MergeWith(srcRecord.AsVariables())
		if err != nil {
			return errors.Wrap(err, "couldn't merge given variables with source record variables")
		}

		joinedStream, err := stream.joined.Get(variables)
		if err != nil {
			return errors.Wrap(err, "couldn't get joined stream")
		}

		for {
			joinedRecord, err := joinedStream.Next()
			if err != nil {
				if err == ErrEndOfStream {
					joinedStream.Close()
					if !joinedAnyRecord {
						select {
						case task.output <- srcRecord:
						case <-stream.workerCtx.Done():
							close(task.output)
							return stream.workerCtx.Err()
						}
					}
					close(task.output)
					break
				}
				return errors.Wrap(err, "couldn't get joined record")
			}
			joinedAnyRecord = true

			fields := srcRecord.fieldNames
			for _, field := range joinedRecord.Fields() {
				fields = append(fields, field.Name)
			}

			allVariableValues, err := srcRecord.AsVariables().MergeWith(joinedRecord.AsVariables())
			if err != nil {
				return errors.Wrap(err, "couldn't merge current record variables with joined record variables")
			}

			select {
			case task.output <- NewRecord(fields, allVariableValues):
			case <-stream.workerCtx.Done():
				joinedStream.Close()
				close(task.output)
				return stream.workerCtx.Err()
			}
		}
	}
}

func (stream *LeftJoinedStream) Close() error {
	err := stream.source.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close source stream")
	}

	stream.workerCtxCancel()

	return nil
}

func (stream *LeftJoinedStream) Next() (*Record, error) {
	for {
		if stream.curRecord == nil {
			var ok bool
			stream.curRecord, ok = <-stream.output
			if !ok {
				return nil, ErrEndOfStream
			}
		}

		out, ok := <-stream.curRecord
		if !ok {
			stream.curRecord = nil
			continue
		}

		return out, nil
	}
}
