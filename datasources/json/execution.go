package json

import (
	"bufio"
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/valyala/fastjson"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/files"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type jobIn struct {
	// jobCtx?
	line int
	data []byte
}

type jobOut struct {
	line   int
	record Record
	err    error
}

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
	localCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	inChan := make(chan []jobIn, 64)
	outChan := make(chan []jobOut, 64)

	workers := runtime.GOMAXPROCS(0)
	for i := 0; i < workers; i++ {
		go func() {
			var p fastjson.Parser

			for jobs := range inChan {
				outJobs := make([]jobOut, len(jobs))
				for i := range outJobs {
					job := jobs[i]
					out := &outJobs[i]
					out.line = job.line

					v, err := p.ParseBytes(job.data)
					if err != nil {
						out.err = fmt.Errorf("couldn't parse json: %w", err)
						continue
					}

					if v.Type() != fastjson.TypeObject {
						out.err = fmt.Errorf("expected JSON object, got '%s'", string(job.data))
						continue
					}
					o, err := v.Object()
					if err != nil {
						out.err = fmt.Errorf("expected JSON object, got '%s'", string(job.data))
						continue
					}

					values := make([]octosql.Value, len(d.fields))
					for i := range values {
						values[i], _ = getOctoSQLValue(d.fields[i].Type, o.Get(d.fields[i].Name))
					}

					out.record = NewRecord(values, false, time.Time{})
				}
				select {
				case outChan <- outJobs:
				case <-localCtx.Done():
					return
				}
			}
		}()
	}

	done := make(chan struct{})
	linesRead := 0
	go func() {
		line := 0
		batchSize := 32
		if d.tail {
			batchSize = 1
		}
		batch := make([]jobIn, batchSize)
		batchIndex := 0
		for sc.Scan() {
			data := make([]byte, len(sc.Bytes()))
			copy(data, sc.Bytes())
			batch[batchIndex] = jobIn{
				line: line,
				data: data,
			}
			batchIndex++
			if batchIndex == batchSize {
				batchIndex = 0
				select {
				case inChan <- batch:
					linesRead += len(batch)
				case <-localCtx.Done():
					close(done)
					return
				}
				batch = make([]jobIn, batchSize)
			}

			line++
		}
		if batchIndex > 0 {
			batch = batch[:batchIndex]
			select {
			case inChan <- batch:
				linesRead += len(batch)
			case <-localCtx.Done():
				close(done)
				return
			}
		}
		if sc.Err() != nil {
			panic(sc.Err())
			// TODO: Przekaż ten błąd na done channel.
			// TODO: Handle me.
		}
		close(done)
	}()

	var queue []*Record
	var startIndex int
	var fileReaderIsDone bool
produceLoop:
	for {
		select {
		case outJobs := <-outChan:
			for i := range outJobs {
				out := outJobs[i]
				if out.err != nil {
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
		// TODO: Back-pressure? Może jakieś tokeny?
		case <-done:
			fileReaderIsDone = true
			done = nil // will block from now on
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
