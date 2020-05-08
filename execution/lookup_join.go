package execution

import (
	"context"
	"log"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

var isInitializedPrefix = []byte("$initialized$")
var toBeJoinedQueuePrefix = []byte("$to_be_joined$")
var controlMessagesQueuePrefix = []byte("$control_messages$")
var jobsPrefix = []byte("$jobs$")
var sourceRecordPrefix = []byte("$source_record$")
var alreadyJoinedForRecordPrefix = []byte("$already_joined_for_record$")
var outputPrefix = []byte("$output$")
var jobTokenQueuePrefix = []byte("$job_tokens$")

type LookupJoin struct {
	maxJobsCount   int
	source, joined Node
	stateStorage   storage.Storage

	isLeftJoin bool
}

func NewLookupJoin(maxJobsCount int, stateStorage storage.Storage, source Node, joined Node, isLeftJoin bool) *LookupJoin {
	return &LookupJoin{
		maxJobsCount: maxJobsCount,
		source:       source,
		joined:       joined,
		stateStorage: stateStorage,
		isLeftJoin:   isLeftJoin,
	}
}

func (node *LookupJoin) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	sourceStream, execOutput, err := node.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source record stream")
	}

	rs := &LookupJoinStream{
		stateStorage: node.stateStorage,
		variables:    variables,
		streamID:     streamID,

		isLeftJoin: node.isLeftJoin,

		joinedNode: node.joined,
	}

	schedCtx, cancel := context.WithCancel(ctx)
	rs.schedulerCtxCancel = cancel
	rs.schedulerCloseErrChan = make(chan error)

	// We only want to do the initialization routine once, even in case of restarts.
	isInitialized := storage.NewValueState(tx.WithPrefix(streamID.AsPrefix()).WithPrefix(isInitializedPrefix))
	var phantom octosql.Value

	err = isInitialized.Get(&phantom)
	if err == storage.ErrNotFound {
		// Not initialized, initialize here.

		// Fill the token queue with initial tokens.
		tokenQueue := NewOutputQueue(tx.WithPrefix(streamID.AsPrefix()).WithPrefix(jobTokenQueuePrefix))
		token := octosql.MakePhantom()
		for i := 0; i < node.maxJobsCount; i++ {
			if err := tokenQueue.Push(ctx, &token); err != nil {
				return nil, nil, errors.Wrap(err, "couldn't push job token to token queue")
			}
		}

		// Save that we've done the initialization routine.
		err := isInitialized.Set(&phantom)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't save initialization status of lookup join")
		}
	} else if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get initialization status of lookup join")
	} else if err == nil {
		// Run workers for existing jobs.
		var jobs = storage.NewMap(tx.WithPrefix(streamID.AsPrefix()).WithPrefix(jobsPrefix))

		iter := jobs.GetIterator()
		var err error
		var jobID RecordID
		var phantom octosql.Value
		for err = iter.Next(&jobID, &phantom); err == nil; err = iter.Next(&jobID, &phantom) {
			go func() {
				err = rs.RunWorker(schedCtx, &jobID)
				for err != nil {
					log.Printf("couldn't run worker for job ID %s: %s, retrying", jobID.Show(), err.Error())
					err = rs.RunWorker(schedCtx, &jobID)
				}
			}()
		}
		if err != storage.ErrEndOfIterator {
			return nil, nil, errors.Wrap(err, "couldn't iterate over existing join jobs")
		}
		if err := iter.Close(); err != nil {
			return nil, nil, errors.Wrap(err, "couldn't close iterator over existing join jobs")
		}
	}

	// Run the scheduler which will start workers for new join jobs.
	go rs.RunScheduler(schedCtx)

	// Run the pull engine which supplies this lookup join with records which are in need of joining.
	engine := NewPullEngine(rs, node.stateStorage, []RecordStream{sourceStream}, streamID, execOutput.WatermarkSource, true, ctx)

	return engine,
		NewExecutionOutput(
			engine,
			execOutput.NextShuffles,
			append(execOutput.TasksToRun, func() error { engine.Run(); return nil }),
		),
		nil
}

type LookupJoinStream struct {
	stateStorage storage.Storage
	variables    octosql.Variables
	streamID     *StreamID

	// isLeftJoin dictates if we send the source record alone if there are no records to join with it.
	isLeftJoin bool

	joinedNode Node

	schedulerCtxCancel    func()
	schedulerCloseErrChan chan error
}

// The scheduler takes records from the toBeJoined queue, and starts jobs to do joins.
// Control messages (records too, to satisfy the initial ordering of messages) are put on a controlMessages queue,
// where they will be handled by the receiver.
func (rs *LookupJoinStream) RunScheduler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			rs.schedulerCloseErrChan <- ctx.Err()
			return
		default:
		}

		err := rs.loopScheduler(ctx)
		if errors.Cause(err) == ErrNewTransactionRequired {
			continue
		} else if errWait := GetErrWaitForChanges(err); errWait != nil {
			if err := errWait.ListenForChanges(ctx); err != nil {
				log.Println("couldn't listen for changes: ", err)
			}
			if err := errWait.Close(); err != nil {
				log.Println("couldn't close changes listener: ", err)
			}
		} else if err != nil {
			log.Printf("couldn't run lookup join scheduler loop: %s, retrying", err.Error())
		}
	}
}

func (rs *LookupJoinStream) loopScheduler(ctx context.Context) error {
	tx := rs.stateStorage.BeginTransaction().WithPrefix(rs.streamID.AsPrefix())

	var toBeJoinedQueue = NewOutputQueue(tx.WithPrefix(toBeJoinedQueuePrefix))

	var element QueueElement
	err := toBeJoinedQueue.Pop(ctx, &element)
	if err != nil {
		return errors.Wrap(err, "couldn't pop element from to be joined queue")
	}

	switch typedElement := element.Type.(type) {
	case *QueueElement_Record:
		// In case it's a record, we need to start a join job for it.
		var jobs = storage.NewMap(tx.WithPrefix(jobsPrefix))

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
		var controlMessagesQueue = NewOutputQueue(tx.WithPrefix(outputPrefix).WithPrefix(controlMessagesQueuePrefix))
		if err := controlMessagesQueue.Push(ctx, &element); err != nil {
			return errors.Wrap(err, "couldn't add element to control messages queue")
		}

		if err := tx.Commit(); err != nil {
			return errors.Wrap(err, "couldn't commit job creation")
		}

		// Run the worker.
		// If we crash after committing, but before running the worker,
		// then the worker will be started on stream creation.
		go func() {
			err = rs.RunWorker(ctx, recordID)
			for err != nil {
				log.Printf("couldn't run worker for job ID %s: %s, retrying", recordID.Show(), err.Error())
				err = rs.RunWorker(ctx, recordID)
			}
		}()

		return nil

	case *QueueElement_Watermark, *QueueElement_EndOfStream, *QueueElement_Error:
		// If it's a control message, pass it to the control messages queue.
		var controlMessagesQueue = NewOutputQueue(tx.WithPrefix(outputPrefix).WithPrefix(controlMessagesQueuePrefix))

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
func (rs *LookupJoinStream) RunWorker(ctx context.Context, id *RecordID) error {
	tx := rs.stateStorage.BeginTransaction()
	prefixedTx := tx.WithPrefix(rs.streamID.AsPrefix())
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

	if err := tx.Commit(); err != nil {
		return errors.Wrapf(err, "couldn't commit transaction setting up the job")
	}

	jobVariables, err := sourceRecord.AsVariables().MergeWith(rs.variables)
	if err != nil {
		return errors.Wrapf(err, "couldn't merge node variables with current record variables: %v and %s", rs.variables, sourceRecord.Show())
	}

	joinedStream, _, err := GetAndStartAllShuffles(ctx, rs.stateStorage, sourceStreamID, []Node{rs.joinedNode}, jobVariables)
	if err != nil {
		return errors.Wrapf(err, "couldn't get record stream from joined node")
	}

	engine := NewPullEngine(
		&JobOutputQueueIntermediateRecordStore{recordID: id},
		rs.stateStorage,
		[]RecordStream{joinedStream[0]}, // We put one stream in, so only one stream will come out.
		rs.streamID,
		&ZeroWatermarkGenerator{},
		true,
		ctx,
	)

	engine.Run()

	// We're done, the receiver of all those records will clean up everything when he's done reading.

	return nil
}

func (rs *LookupJoinStream) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *Record) error {
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

func (rs *LookupJoinStream) Next(ctx context.Context, tx storage.StateTransaction) (*Record, error) {
	// Check if this stream isn't done already.
	var endOfStreamState = storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))
	var eos octosql.Value
	err := endOfStreamState.Get(&eos)
	if err == storage.ErrNotFound {
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get end of stream value")
	} else {
		return nil, ErrEndOfStream
	}

	// Handle any possible output control messages.
	err = rs.HandleControlMessages(ctx, tx)
	if err == ErrEndOfStream {
		return nil, ErrEndOfStream
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't handle control messages")
	}

	// Get the next output record from any of the active join jobs.
	record, err := rs.GetNextRecord(ctx, tx)
	if err == nil {
		return record, nil
	} else if err == ErrEndOfStream {
		return nil, ErrEndOfStream
	} else if err != storage.ErrNotFound {
		return nil, errors.Wrap(err, "couldn't get next record")
	}

	// We didn't get anything to return
	// Now we check if a new transaction would return something
	// If yes, we tell the caller to create a new transaction
	// Otherwise, we return a storage subscription to wait on

	outputStorage := rs.stateStorage.WithPrefix(rs.streamID.AsPrefix()).WithPrefix(outputPrefix)
	sub := outputStorage.Subscribe(ctx)

	tx = rs.stateStorage.WithPrefix(rs.streamID.AsPrefix()).BeginTransaction()
	defer tx.Abort()

	err = rs.HandleControlMessages(ctx, tx)
	if err == ErrEndOfStream {
		if err := sub.Close(); err != nil {
			return nil, errors.Wrap(err, "couldn't change subscription")
		}
		return nil, ErrNewTransactionRequired
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't handle control messages")
	}

	record, err = rs.GetNextRecord(ctx, tx)
	if err == nil || err == ErrEndOfStream {
		if err := sub.Close(); err != nil {
			return nil, errors.Wrap(err, "couldn't change subscription")
		}
		return nil, ErrNewTransactionRequired
	} else if err != storage.ErrNotFound {
		return nil, errors.Wrap(err, "couldn't get next record")
	}

	return nil, NewErrWaitForChanges(sub)
}

func (rs *LookupJoinStream) HandleControlMessages(ctx context.Context, tx storage.StateTransaction) error {
	var controlMessagesQueue = NewOutputQueue(tx.WithPrefix(outputPrefix).WithPrefix(controlMessagesQueuePrefix))
	var endOfStreamState = storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))

	for {
		var msg QueueElement
		err := controlMessagesQueue.Peek(ctx, &msg)
		if err == storage.ErrNotFound {
			return nil
		} else if err != nil {
			return errors.Wrap(err, "couldn't check control messages queue")
		}

		switch typedMsg := msg.Type.(type) {
		case *QueueElement_Record:
			var jobs = storage.NewMap(tx.WithPrefix(jobsPrefix))
			var phantom octosql.Value

			err := jobs.Get(typedMsg.Record.ID(), &phantom)
			if err == nil {
				return nil // The job exists, we don't want to pop this control message yet
			} else if err != storage.ErrNotFound {
				return errors.Wrapf(err, "couldn't check if job %s exists", typedMsg.Record.ID().Show())
			}
			// We only handle record id control messages if the corresponding job doesn't exist anymore

		default:
			// Other messages always get handled
		}

		err = controlMessagesQueue.Pop(ctx, &msg)
		if err != nil {
			return errors.Wrap(err, "couldn't pop from control messages queue")
		}

		switch payload := msg.Type.(type) {
		case *QueueElement_Record:
			// This message can safely be discarded as the corresponding job doesn't exist.
			// As these jobs are done now, we can respond with tokens.
			tokenQueue := NewOutputQueue(tx.WithPrefix(jobTokenQueuePrefix))

			token := octosql.MakePhantom()
			if err := tokenQueue.Push(ctx, &token); err != nil {
				return errors.Wrap(err, "couldn't push job token to token queue")
			}

		case *QueueElement_Watermark:
			// Update the current output watermark state
			outputWatermarkState := storage.NewValueState(tx.WithPrefix(outputWatermarkPrefix))

			watermark, err := ptypes.Timestamp(payload.Watermark)
			if err != nil {
				return errors.Wrap(err, "couldn't parse watermark timestamp")
			}
			octoWatermark := octosql.MakeTime(watermark)
			if err := outputWatermarkState.Set(&octoWatermark); err != nil {
				return errors.Wrap(err, "couldn't update output watermark")
			}

		case *QueueElement_EndOfStream:
			// Update the current end of stream state
			octoEndOfStream := octosql.MakeBool(true)
			if err := endOfStreamState.Set(&octoEndOfStream); err != nil {
				return errors.Wrap(err, "couldn't update end of stream state")
			}
			return ErrEndOfStream

		case *QueueElement_Error:
			return errors.New(payload.Error)
		default:
			panic("invalid queue element type")
		}
	}
}

func (rs *LookupJoinStream) ReadyForMore(ctx context.Context, tx storage.StateTransaction) error {
	// We limit the amount of records received using an internal token queue.
	tokenQueue := NewOutputQueue(tx.WithPrefix(jobTokenQueuePrefix))

	var token octosql.Value
	if err := tokenQueue.Pop(ctx, &token); err != nil {
		return errors.Wrap(err, "couldn't get job token from token queue")
	}

	return nil
}

func (rs *LookupJoinStream) GetNextRecord(ctx context.Context, tx storage.StateTransaction) (*Record, error) {
	var jobs = storage.NewMap(tx.WithPrefix(jobsPrefix))

	iter := jobs.GetIterator()

	var jobRecordID RecordID
	var phantom octosql.Value
	var err error
	// Go over all jobs in order and try to get records to return from any of them.
	for err = iter.Next(&jobRecordID, &phantom); err == nil; err = iter.Next(&jobRecordID, &phantom) {
		sourceRecordState := storage.NewValueState(tx.WithPrefix(jobRecordID.AsPrefix()).WithPrefix(sourceRecordPrefix))
		recordsQueue := storage.NewDeque(tx.WithPrefix(outputPrefix).WithPrefix(jobRecordID.AsPrefix()))

		var outputElement QueueElement

		if rs.isLeftJoin {
			err := recordsQueue.PeekFront(&outputElement)
			if err == storage.ErrNotFound {
				continue
			} else if err != nil {
				return nil, errors.Wrapf(err, "couldn't peek element from output queue for job with recordID %s", jobRecordID.Show())
			}

			switch outputElement.Type.(type) {
			case *QueueElement_Record:
				// If this is a left join, save that we've now joined a record for this source record if we haven't already.
				alreadyJoinedSomethingForRecordState := storage.NewValueState(tx.WithPrefix(jobRecordID.AsPrefix()).WithPrefix(alreadyJoinedForRecordPrefix))
				var phantom octosql.Value

				err := alreadyJoinedSomethingForRecordState.Get(&phantom)
				if err == storage.ErrNotFound {
					phantom = octosql.MakePhantom()
					if err := alreadyJoinedSomethingForRecordState.Set(&phantom); err != nil {
						return nil, errors.Wrapf(err, "couldn't save that a record was now joined for job with recordID %s", jobRecordID.Show())
					}
				} else if err != nil {
					return nil, errors.Wrapf(err, "couldn't check if any record was already joined for job with recordID %s", jobRecordID.Show())
				}

			case *QueueElement_EndOfStream:
				// If this is a left join and we haven't joined anything for this source record,
				// then send the record without anything attached.
				alreadyJoinedSomethingForRecordState := storage.NewValueState(tx.WithPrefix(jobRecordID.AsPrefix()).WithPrefix(alreadyJoinedForRecordPrefix))
				var phantom octosql.Value

				err := alreadyJoinedSomethingForRecordState.Get(&phantom)
				if err == storage.ErrNotFound {
					phantom = octosql.MakePhantom()
					if err := alreadyJoinedSomethingForRecordState.Set(&phantom); err != nil {
						return nil, errors.Wrapf(err, "couldn't save that a record was now joined for job with recordID %s", jobRecordID.Show())
					}

					var sourceRecord Record
					if err = sourceRecordState.Get(&sourceRecord); err != nil {
						return nil, errors.Wrapf(err, "couldn't get source record for job with recordID %s", jobRecordID.Show())
					}

					if err := iter.Close(); err != nil {
						return nil, errors.Wrap(err, "couldn't close jobs iterator")
					}

					return &sourceRecord, nil
				} else if err != nil {
					return nil, errors.Wrapf(err, "couldn't check if any record was already joined for job with recordID %s", jobRecordID.Show())
				} else if err == nil {
					// We've already sent an output record for this source record, so we can clear the state.
					if err := alreadyJoinedSomethingForRecordState.Clear(); err != nil {
						return nil, errors.Wrapf(err, "couldn't clear information about already having sent a record for job with recordID %s", jobRecordID.Show())
					}
				}

			default:
			}
		}

		err = recordsQueue.PopFront(&outputElement)
		if err == storage.ErrNotFound {
			continue
		} else if err != nil {
			return nil, errors.Wrapf(err, "couldn't get element from output queue for job with recordID %s", jobRecordID.Show())
		}

		switch typedElement := outputElement.Type.(type) {
		case *QueueElement_Record:
			// If it's a record, just return it, with the source record for this job joined with it.
			joinedRecord := typedElement.Record

			var sourceRecord Record
			if err = sourceRecordState.Get(&sourceRecord); err != nil {
				return nil, errors.Wrapf(err, "couldn't get source record for job with recordID %s", jobRecordID.Show())
			}

			fields := sourceRecord.GetVariableNames()
			for _, field := range joinedRecord.Fields() {
				fields = append(fields, field.Name)
			}

			allVariableValues, err := sourceRecord.AsVariables().MergeWith(joinedRecord.AsVariables())
			if err != nil {
				return nil, errors.Wrap(err, "couldn't merge source record variables with joined record variables")
			}

			if err := iter.Close(); err != nil {
				return nil, errors.Wrap(err, "couldn't close jobs iterator")
			}

			return NewRecord(fields, allVariableValues, WithMetadataFrom(&sourceRecord), WithID(joinedRecord.ID())), nil

		case *QueueElement_EndOfStream:
			// If it's an end of stream, then we can delete this job.
			if err := sourceRecordState.Clear(); err != nil {
				return nil, errors.Wrapf(err, "couldn't clear source record for job with recordID %s", jobRecordID.Show())
			}

			if err := jobs.Delete(&jobRecordID); err != nil {
				return nil, errors.Wrapf(err, "couldn't delete job with recordID %s from jobs list", jobRecordID.Show())
			}

			if err := recordsQueue.Clear(); err != nil {
				return nil, errors.Wrapf(err, "couldn't clear joined record queue for job with recordID %s", jobRecordID.Show())
			}

			err := rs.HandleControlMessages(ctx, tx)
			if err == ErrEndOfStream {
				if err := iter.Close(); err != nil {
					return nil, errors.Wrap(err, "couldn't close jobs iterator")
				}

				return nil, ErrEndOfStream
			} else if err != nil {
				return nil, errors.Wrapf(err, "couldn't handle control messages after deleting job with recordID %s", jobRecordID.Show())
			}

		default:
			panic("invalid queue element type")
		}
	}
	if err == storage.ErrEndOfIterator {
		if err := iter.Close(); err != nil {
			return nil, errors.Wrap(err, "couldn't close jobs iterator")
		}

		return nil, storage.ErrNotFound
	} else {
		return nil, errors.Wrap(err, "couldn't get next job id from iterator")
	}
}

func (rs *LookupJoinStream) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	var toBeJoinedQueue = NewOutputQueue(tx.WithPrefix(toBeJoinedQueuePrefix))

	t, err := ptypes.TimestampProto(watermark)
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

func (j *LookupJoinStream) TriggerKeys(ctx context.Context, tx storage.StateTransaction, batchSize int) (int, error) {
	return 0, nil
}

func (rs *LookupJoinStream) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
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

func (rs *LookupJoinStream) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
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

func (rs *LookupJoinStream) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
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

func (rs *LookupJoinStream) Close(ctx context.Context, storage storage.Storage) error {
	rs.schedulerCtxCancel()
	err := <-rs.schedulerCloseErrChan
	if err == context.Canceled || err == context.DeadlineExceeded {
	} else if err != nil {
		return errors.Wrap(err, "couldn't stop lookup join scheduler")
	}

	if err := storage.DropAll(rs.streamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	if err := storage.DropAll(outputWatermarkPrefix); err != nil {
		return errors.Wrap(err, "couldn't clear storage with output watermark prefix")
	}

	return nil
}

type JobOutputQueueIntermediateRecordStore struct {
	recordID *RecordID
}

func (j *JobOutputQueueIntermediateRecordStore) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *Record) error {
	outputQueue := storage.NewDeque(tx.WithPrefix(outputPrefix).WithPrefix(j.recordID.AsPrefix()))

	element := QueueElement{
		Type: &QueueElement_Record{
			Record: record,
		},
	}
	err := outputQueue.PushBack(&element)
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

func (j *JobOutputQueueIntermediateRecordStore) TriggerKeys(ctx context.Context, tx storage.StateTransaction, batchSize int) (int, error) {
	return 0, nil
}

func (j *JobOutputQueueIntermediateRecordStore) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	return time.Time{}, nil
}

func (j *JobOutputQueueIntermediateRecordStore) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	outputQueue := storage.NewDeque(tx.WithPrefix(outputPrefix).WithPrefix(j.recordID.AsPrefix()))

	element := QueueElement{
		Type: &QueueElement_EndOfStream{
			EndOfStream: true,
		},
	}
	err := outputQueue.PushBack(&element)
	if err != nil {
		return errors.Wrapf(err, "couldn't mark end of stream on output queue for source record id %s", j.recordID.Show())
	}

	return nil
}

func (j *JobOutputQueueIntermediateRecordStore) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	outputQueue := storage.NewDeque(tx.WithPrefix(outputPrefix).WithPrefix(j.recordID.AsPrefix()))

	element := QueueElement{
		Type: &QueueElement_Error{
			Error: err.Error(),
		},
	}
	err = outputQueue.PushBack(&element)
	if err != nil {
		return errors.Wrapf(err, "couldn't mark error on output queue for source record id %s", j.recordID.Show())
	}

	return nil
}

func (j *JobOutputQueueIntermediateRecordStore) ReadyForMore(ctx context.Context, tx storage.StateTransaction) error {
	return nil
}

func (j *JobOutputQueueIntermediateRecordStore) Close(ctx context.Context, storage storage.Storage) error {
	return nil // All storages used by this irs are prefixed by pull engine's streamID and therefore will be closed by him
}
