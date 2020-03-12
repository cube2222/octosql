package execution

import (
	"context"
	"log"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type LookupJoin struct {
	source, joined Node
	stateStorage   storage.Storage
}

func NewLookupJoin(source Node, joined Node) *LookupJoin {
	return &LookupJoin{
		source: source,
		joined: joined,
	}
}

func (l *LookupJoin) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	panic("implement me")
}

type LookupJoinStream struct {
	stateStorage storage.Storage
	variables    octosql.Variables
	streamID     *StreamID

	joinedNode Node
}

var toBeJoinedQueuePrefix = []byte("$to_be_joined$")
var jobsPrefix = []byte("$jobs$")
var sourceRecordPrefix = []byte("$source_record$")
var controlMessagesQueuePrefix = []byte("$control_messages$")

// The scheduler takes records from the toBeJoined queue, and starts jobs to do joins.
// Control messages (records too, to satisfy the initial ordering of messages) are put on a controlMessages queue,
// where they will be handled by the receiver.
func (lsm *LookupJoinStream) RunScheduler(ctx context.Context) {
	for {
		err := lsm.loopScheduler(ctx)
		if err != nil {
			log.Fatal(err) // TODO: Handle
		}
	}
}

func (lsm *LookupJoinStream) loopScheduler(ctx context.Context) error {
	tx := lsm.stateStorage.BeginTransaction().WithPrefix(lsm.streamID.AsPrefix())

	var toBeJoinedQueue = NewOutputQueue(tx.WithPrefix(toBeJoinedQueuePrefix))

	var element QueueElement
	err := toBeJoinedQueue.Pop(ctx, &element)
	if err != nil {
		// TODO: Handle various error instances
	}

	switch typedElement := element.Type.(type) {
	case *QueueElement_Record:
		jobs := storage.NewMap(tx.WithPrefix(jobsPrefix))

		recordID := typedElement.Record.ID()

		// Create the job entry
		phantom := octosql.MakePhantom()
		if err := jobs.Set(recordID, &phantom); err != nil {
			return errors.Wrap(err, "couldn't add job to job set")
		}

		// Save the source record for this join.
		sourceRecordState := storage.NewValueState(tx.WithPrefix(recordID.AsPrefix()).WithPrefix(sourceRecordPrefix))
		if err = sourceRecordState.Set(typedElement.Record); err != nil {
			return errors.Wrap(err, "couldn't save source record for job")
		}

		// Save the message to the control messages queue
		var controlMessagesQueue = NewOutputQueue(tx.WithPrefix(controlMessagesQueuePrefix))
		if err := controlMessagesQueue.Push(ctx, &element); err != nil {
			return errors.Wrap(err, "couldn't add element to control messages queue")
		}

		if err := tx.Commit(); err != nil {
			return errors.Wrap(err, "couldn't commit job creation")
		}

		// Run the worker.
		// If we crash after committing, but before running the worker,
		// then the worker will be started on stream creation.
		go lsm.RunWorker(ctx, recordID)

		return nil

	case *QueueElement_Watermark, *QueueElement_EndOfStream, *QueueElement_Error:
		var controlMessagesQueue = NewOutputQueue(tx.WithPrefix(controlMessagesQueuePrefix))

		// Save the message to the control messages queue
		if err := controlMessagesQueue.Push(ctx, &element); err != nil {
			return errors.Wrap(err, "couldn't add element to control messages queue")
		}

		if err := tx.Commit(); err != nil {
			return errors.Wrap(err, "couldn't commit control messages receive")
		}

		return nil

	default:
		panic("invalid queue element type")
	}
}

// The worker drives streams to completion, puts received records to output queues scoped by record id.
// In the end, it puts an EndOfStream message on the queue.
func (lsm *LookupJoinStream) RunWorker(ctx context.Context, id *ID) error {
	tx := lsm.stateStorage.BeginTransaction()
	prefixedTx := tx.WithPrefix(lsm.streamID.AsPrefix())
	recordPrefixedTx := prefixedTx.WithPrefix(id.AsPrefix())

	// Get the stream ID we will use for the source stream for this job.
	sourceStreamID, err := GetSourceStreamID(recordPrefixedTx, octosql.MakePhantom())
	if err != nil {
		return errors.Wrapf(err, "couldn't get source stream id for record with id %s", id.Show())
	}

	var sourceRecord Record
	// Get the source record for this join.
	sourceRecordState := storage.NewValueState(recordPrefixedTx.WithPrefix(sourceRecordPrefix))
	if err = sourceRecordState.Get(&sourceRecord); err != nil {
		return errors.Wrap(err, "couldn't get source record for job")
	}

	jobVariables, err := sourceRecord.AsVariables().MergeWith(lsm.variables)
	if err != nil {
		return errors.Wrapf(err, "couldn't merge node variables with current record variables: %s and %s", lsm.variables, sourceRecord.Show())
	}

	rs, _, err := lsm.joinedNode.Get(storage.InjectStateTransaction(ctx, tx), jobVariables, sourceStreamID)
	if err != nil {
		return errors.Wrapf(err, "couldn't get record stream from joined node")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrapf(err, "couldn't commit transaction setting up the job")
	}

	engine := NewPullEngine(
		&JobOutputQueueIntermediateRecordStore{recordID: id},
		lsm.stateStorage,
		rs,
		lsm.streamID,
		&ZeroWatermarkGenerator{},
	)

	engine.Run(ctx)

	// We're done, the receiver of all those records will clean up everything when he's done reading.

	return nil
}

func (lsm *LookupJoinStream) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *Record) error {
	var toBeJoinedQueue = NewOutputQueue(tx.WithPrefix(toBeJoinedQueuePrefix))

	err := toBeJoinedQueue.Push(ctx, &QueueElement{
		Type: &QueueElement_Record{
			Record: record,
		},
	})
	if err != nil {
		return errors.Wrap(err, "couldn't push record to queue of records to be joined")
	}

	return nil
}

func (lsm *LookupJoinStream) Next(ctx context.Context, tx storage.StateTransaction) (*Record, error) {
	var outputQueue = NewOutputQueue(tx.WithPrefix(outputQueuePrefix))

	endOfStreamState := storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))
	var eos octosql.Value
	err := endOfStreamState.Get(&eos)
	if err == storage.ErrNotFound {
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get end of stream value")
	} else {
		return nil, ErrEndOfStream
	}

	var element QueueElement
	for err = outputQueue.Pop(ctx, &element); err == nil; err = outputQueue.Pop(ctx, &element) {
		switch payload := element.Type.(type) {
		case *QueueElement_Record:
			return payload.Record, nil
		case *QueueElement_Watermark:
			outputWatermarkState := storage.NewValueState(tx.WithPrefix(outputWatermarkPrefix))
			watermark, err := ptypes.Timestamp(payload.Watermark)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't parse watermark timestamp")
			}
			octoWatermark := octosql.MakeTime(watermark)
			err = outputWatermarkState.Set(&octoWatermark)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't update output watermark")
			}
		case *QueueElement_EndOfStream:
			octoEndOfStream := octosql.MakeBool(true)
			err := endOfStreamState.Set(&octoEndOfStream)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't update end of stream state")
			}
			return nil, ErrEndOfStream
		case *QueueElement_Error:
			return nil, errors.New(payload.Error)
		default:
			panic("invalid queue element type")
		}
	}

	return nil, errors.Wrap(err, "couldn't pop element from output queue")
}

func (lsm *LookupJoinStream) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	var toBeJoinedQueue = NewOutputQueue(tx.WithPrefix(toBeJoinedQueuePrefix))

	t, err := ptypes.TimestampProto(maxWatermark)
	if err != nil {
		return errors.Wrapf(err, "couldn't convert Time %s to proto.Timestamp", watermark)
	}

	err = toBeJoinedQueue.Push(ctx, &QueueElement{
		Type: &QueueElement_Watermark{
			Watermark: t,
		},
	})
	if err != nil {
		return errors.Wrap(err, "couldn't push record to queue of records to be joined")
	}

	return nil
}

func (lsm *LookupJoinStream) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	outputWatermarkState := storage.NewValueState(tx.WithPrefix(outputWatermarkPrefix))
	var octoWatermark octosql.Value
	err := outputWatermarkState.Get(&octoWatermark)
	if err == storage.ErrNotFound {
		return time.Time{}, nil
	} else if err != nil {
		return time.Time{}, errors.Wrap(err, "couldn't get output watermark")
	}

	return octoWatermark.AsTime(), nil
}

func (lsm *LookupJoinStream) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	var toBeJoinedQueue = NewOutputQueue(tx.WithPrefix(toBeJoinedQueuePrefix))

	err := toBeJoinedQueue.Push(ctx, &QueueElement{
		Type: &QueueElement_EndOfStream{
			EndOfStream: true,
		},
	})
	if err != nil {
		return errors.Wrap(err, "couldn't push end of stream to queue of records to be joined")
	}

	return nil
}

func (lsm *LookupJoinStream) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	var toBeJoinedQueue = NewOutputQueue(tx.WithPrefix(toBeJoinedQueuePrefix))

	err = toBeJoinedQueue.Push(ctx, &QueueElement{
		Type: &QueueElement_Error{
			Error: err.Error(),
		},
	})
	if err != nil {
		return errors.Wrap(err, "couldn't push error to queue of records to be joined")
	}

	return nil
}

func (lsm *LookupJoinStream) Close() error {
	return nil // TODO: Close this, remove state
}

type JobOutputQueueIntermediateRecordStore struct {
	recordID *ID
}

func (j *JobOutputQueueIntermediateRecordStore) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *Record) error {
	outputQueue := NewOutputQueue(tx.WithPrefix(j.recordID.AsPrefix()))

	element := QueueElement{
		Type: &QueueElement_Record{
			Record: record,
		},
	}
	err := outputQueue.Push(ctx, &element)
	if err != nil {
		return errors.Wrapf(err, "couldn't push record output queue for source record id %s", j.recordID.Show())
	}

	return nil
}

func (j *JobOutputQueueIntermediateRecordStore) Next(ctx context.Context, tx storage.StateTransaction) (*Record, error) {
	return nil, nil
}

func (j *JobOutputQueueIntermediateRecordStore) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	return nil
}

func (j *JobOutputQueueIntermediateRecordStore) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	return time.Time{}, nil
}

func (j *JobOutputQueueIntermediateRecordStore) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	outputQueue := NewOutputQueue(tx.WithPrefix(j.recordID.AsPrefix()))

	element := QueueElement{
		Type: &QueueElement_EndOfStream{
			EndOfStream: true,
		},
	}
	err := outputQueue.Push(ctx, &element)
	if err != nil {
		return errors.Wrapf(err, "couldn't mark end of stream on output queue for source record id %s", j.recordID.Show())
	}

	return nil
}

func (j *JobOutputQueueIntermediateRecordStore) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	outputQueue := NewOutputQueue(tx.WithPrefix(j.recordID.AsPrefix()))

	element := QueueElement{
		Type: &QueueElement_Error{
			Error: err.Error(),
		},
	}
	err = outputQueue.Push(ctx, &element)
	if err != nil {
		return errors.Wrapf(err, "couldn't mark error on output queue for source record id %s", j.recordID.Show())
	}

	return nil
}

func (j *JobOutputQueueIntermediateRecordStore) Close() error {
	return nil
}
