package json

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/valyala/fastjson"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

// jobIn is a single job for the parser worker pool.
// It contains a list of lines to parse into a list of records.
type jobIn struct {
	fields []physical.SchemaField
	ctx    context.Context

	batchIndex int
	lines      []int    // line numbers
	data       [][]byte // data for each line

	// The channel to send job outputs to.
	outChan chan<- jobOutRecord
}

// jobOutRecord is the result of parsing a single line.
type jobOutRecord struct {
	batchIndex  int
	recordBatch RecordBatch
	err         error
}

// The function below creates the workers and stores the job queue in a global variable.
var parserWorkReceiveChannel = func() chan<- jobIn {
	// We should be able to scale to all cores.
	workerCount := runtime.GOMAXPROCS(0)

	inChan := make(chan jobIn, 128)

	for i := 0; i < workerCount; i++ {
		go func() {
			var p fastjson.Parser

		getWorkLoop:
			for job := range inChan {
				values, err := func() ([][]octosql.Value, error) {
					outValues := make([][]octosql.Value, len(job.fields))
					for i := range outValues {
						outValues[i] = make([]octosql.Value, len(job.data))
					}

					for i := range job.data {
						v, err := p.ParseBytes(job.data[i])
						if err != nil {
							return nil, fmt.Errorf("couldn't parse line %d: couldn't parse json: %w", job.lines[i], err)
						}

						o, err := v.Object()
						if err != nil {
							return nil, fmt.Errorf("couldn't parse line %d: expected JSON object, got '%s'", job.lines[i], string(job.data[i]))
						}

						for colIndex := range job.fields {
							outValues[colIndex][i], _ = getOctoSQLValue(job.fields[colIndex].Type, o.Get(job.fields[colIndex].Name))
						}
					}
					return outValues, nil
				}()
				outJob := jobOutRecord{
					batchIndex: job.batchIndex,
				}
				if err == nil {
					outJob.recordBatch = NewRecordBatch(values, make([]bool, len(values)), make([]time.Time, len(values)))
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
