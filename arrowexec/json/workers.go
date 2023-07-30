package json

import (
	"context"
	"fmt"
	"runtime"

	"github.com/valyala/fastjson"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

// jobIn is a single job for the parser worker pool.
// It contains a list of lines to parse into a list of records.
type jobIn struct {
	schema *arrow.Schema
	ctx    context.Context

	sequenceNumber int
	lines          [][]byte // data for each line

	// The channel to send job outputs to.
	outChan chan<- jobOutRecord
}

// jobOutRecord is the result of parsing a single batch.
type jobOutRecord struct {
	sequenceNumber int
	record         arrow.Record
	err            error
}

// The function below creates the workers and stores the job queue in a global variable.
var parserWorkReceiveChannel = func() chan<- jobIn {
	// We should be able to scale to all cores.
	workerCount := runtime.GOMAXPROCS(0)

	inChan := make(chan jobIn, 16)

	for i := 0; i < workerCount; i++ {
		go func() {
			var p fastjson.Parser

		getWorkLoop:
			for job := range inChan {
				// TODO: Fix allocator.
				recordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), job.schema)
				recordBuilder.Reserve(len(job.lines))
				err := func() error {
					readRecord, err := recordReader(job.schema, recordBuilder)
					if err != nil {
						return err
					}

					for i := range job.lines {
						v, err := p.ParseBytes(job.lines[i])
						if err != nil {
							return fmt.Errorf("couldn't parse json: %w", err)
						}

						if err := readRecord(v); err != nil {
							return err
						}
					}
					return nil
				}()
				outJob := jobOutRecord{
					sequenceNumber: job.sequenceNumber,
				}
				if err == nil {
					outJob.record = recordBuilder.NewRecord()
				} else {
					outJob.err = err
				}

				select {
				case job.outChan <- outJob:
				case <-job.ctx.Done():
					continue getWorkLoop
				}
			}
		}()
	}

	return inChan
}()
