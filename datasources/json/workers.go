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

	lines []int    // line numbers
	data  [][]byte // data for each line

	// The channel to send job outputs to.
	outChan chan<- []jobOutRecord
}

// jobOutRecord is the result of parsing a single line.
type jobOutRecord struct {
	line   int
	record Record
	err    error
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
				outJobs := make([]jobOutRecord, len(job.lines))
				for i := range outJobs {
					out := &outJobs[i]
					out.line = job.lines[i]

					v, err := p.ParseBytes(job.data[i])
					if err != nil {
						out.err = fmt.Errorf("couldn't parse json: %w", err)
						continue
					}

					o, err := v.Object()
					if err != nil {
						out.err = fmt.Errorf("expected JSON object, got '%s'", string(job.data[i]))
						continue
					}

					values := make([]octosql.Value, len(job.fields))
					for i := range values {
						values[i], _ = getOctoSQLValue(job.fields[i].Type, o.Get(job.fields[i].Name))
					}

					out.record = NewRecord(values, false, time.Time{})
				}
				select {
				case job.outChan <- outJobs:
				case <-job.ctx.Done():
					continue getWorkLoop
				}
			}
		}()
	}

	return inChan
}()
