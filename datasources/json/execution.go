package json

import (
	"bufio"
	"context"
	"fmt"
	"time"

	"github.com/valyala/fastjson"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/files"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type DatasourceExecuting struct {
	path   string
	tail   bool
	fields []physical.SchemaField
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	f, err := files.OpenLocalFile(ctx, d.path, files.WithTail(d.tail))
	if err != nil {
		return fmt.Errorf("couldn't open local file: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1024*1024)

	// All async processing in this function and all jobs created by it use this context.
	// This means that returning from this function will properly clean up all async processors.
	localCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	outChan := make(chan []jobOutRecord, 128)

	// This is necessary so that the global worker pool doesn't lock up.
	// We only allow the line reader to create parsing jobs when we know that the output channel has space left.
	// The consumer will always take out a token after reading from the output channel,
	// so filling up the token channel is equivalent to filling up the output channel.
	// But since the line reader goes through the worker pool, it can't block on the output channel.
	// That's why we use a token channel.
	outChanAvailableTokens := make(chan struct{}, 128)

	done := make(chan error, 1)

	// linesRead is first incremented by the line reader.
	// Then, after the line reader is done, it's read by the consumer,
	// so it knows when it's done reading all the lines.
	// Access is synchronized through the `done` channel.
	linesRead := 0

	// This routine reads lines and sends them in batches to the parser worker pool.
	go func() {
		line := 0
		batchSize := 64
		if d.tail {
			batchSize = 1
		}
		job := jobIn{
			ctx:     localCtx,
			fields:  d.fields,
			lines:   make([]int, 0, batchSize),
			data:    make([][]byte, 0, batchSize),
			outChan: outChan,
		}
		for sc.Scan() {
			data := make([]byte, len(sc.Bytes()))
			copy(data, sc.Bytes())
			job.lines = append(job.lines, line)
			job.data = append(job.data, data)

			if len(job.lines) == batchSize {
				select {
				case outChanAvailableTokens <- struct{}{}:
					parserWorkReceiveChannel <- job
					linesRead += len(job.lines)
				case <-localCtx.Done():
					return
				}
				job = jobIn{
					ctx:     localCtx,
					fields:  d.fields,
					lines:   make([]int, 0, batchSize),
					data:    make([][]byte, 0, batchSize),
					outChan: outChan,
				}
			}

			line++
		}
		if len(job.lines) > 0 {
			select {
			case outChanAvailableTokens <- struct{}{}:
				parserWorkReceiveChannel <- job
				linesRead += len(job.lines)
			case <-localCtx.Done():
				return
			}
		}
		done <- sc.Err()
	}()

	var queue []*Record
	var startIndex int
	var fileReaderIsDone bool
produceLoop:
	for {
		select {
		case outJobs := <-outChan:
			<-outChanAvailableTokens
			for i := range outJobs {
				out := outJobs[i]
				if err := out.err; err != nil {
					return err
				}
				for len(queue) <= out.line-startIndex {
					queue = append(queue, nil)
				}
				queue[out.line-startIndex] = &out.record
				for len(queue) > 0 && queue[0] != nil {
					record := queue[0]
					if err := produce(ProduceFromExecutionContext(ctx), *record); err != nil {
						return fmt.Errorf("couldn't produce: %w", err)
					}
					queue = queue[1:]
					startIndex++
				}
			}
			if fileReaderIsDone && startIndex == linesRead {
				break produceLoop
			}
		case readerErr := <-done:
			if readerErr != nil {
				return readerErr
			}
			fileReaderIsDone = true
			done = nil // will block this select branch from now on
			if fileReaderIsDone && startIndex == linesRead {
				break produceLoop
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func getOctoSQLValue(t octosql.Type, value *fastjson.Value) (out octosql.Value, ok bool) {
	if value == nil {
		return octosql.NewNull(), t.TypeID == octosql.TypeIDNull
	}

	switch t.TypeID {
	case octosql.TypeIDFloat:
		if value.Type() == fastjson.TypeNumber {
			v, _ := value.Float64()
			return octosql.NewFloat(v), true
		}
	case octosql.TypeIDBoolean:
		if value.Type() == fastjson.TypeTrue {
			return octosql.NewBoolean(true), true
		} else if value.Type() == fastjson.TypeFalse {
			return octosql.NewBoolean(false), true
		}
	case octosql.TypeIDString:
		if value.Type() == fastjson.TypeString {
			v, _ := value.StringBytes()
			return octosql.NewString(string(v)), true
		}
	case octosql.TypeIDTime:
		if value.Type() == fastjson.TypeString {
			v, _ := value.StringBytes()
			if parsed, err := time.Parse(time.RFC3339Nano, string(v)); err == nil {
				return octosql.NewTime(parsed), true
			}
		}
	case octosql.TypeIDDuration:
		if value.Type() == fastjson.TypeString {
			v, _ := value.StringBytes()
			if parsed, err := time.ParseDuration(string(v)); err == nil {
				return octosql.NewDuration(parsed), true
			}
		}
	case octosql.TypeIDList:
		if value.Type() == fastjson.TypeArray {
			arr, _ := value.Array()
			values := make([]octosql.Value, len(arr))

			outOk := true
			for i := range arr {
				curValue, curOk := getOctoSQLValue(*t.List.Element, arr[i])
				values[i] = curValue
				outOk = outOk && curOk
			}
			return octosql.NewList(values), outOk
		}
	case octosql.TypeIDStruct:
		if value.Type() == fastjson.TypeObject {
			obj, _ := value.Object()
			values := make([]octosql.Value, len(t.Struct.Fields))

			outOk := true
			for i, field := range t.Struct.Fields {
				curValue, curOk := getOctoSQLValue(field.Type, obj.Get(field.Name))
				values[i] = curValue
				outOk = outOk && curOk
			}
			return octosql.NewStruct(values), outOk
		}
	case octosql.TypeIDUnion:
		for _, alternative := range t.Union.Alternatives {
			v, ok := getOctoSQLValue(alternative, value)
			if ok {
				return v, true
			}
		}
	}

	return octosql.ZeroValue, false
}
