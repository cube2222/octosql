package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/btree"
	"github.com/gosuri/uilive"
	"github.com/olekukonko/tablewriter"

	"github.com/cube2222/octosql/aggregates"
	"github.com/cube2222/octosql/datasources/json"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/functions"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/table_valued_functions"
)

func main() {
	// runtime.GOMAXPROCS(1)
	// f, err := os.Create("profile.pprof")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer f.Close()
	// if err := pprof.StartCPUProfile(f); err != nil {
	// 	log.Fatal(err)
	// }
	// defer pprof.StopCPUProfile()

	statement, err := sqlparser.Parse(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	logicalPlan, err := parser.ParseNode(statement.(sqlparser.SelectStatement))
	if err != nil {
		log.Fatal(err)
	}
	env := physical.Environment{
		Aggregates: map[string][]physical.AggregateDescriptor{
			"count": aggregates.CountOverloads,
		},
		Functions: map[string][]physical.FunctionDescriptor{
			"=": functions.Equals,
		},
		Datasources: &physical.DatasourceRepository{
			Datasources: map[string]func(name string) (physical.DatasourceImplementation, error){
				"json": json.Creator,
			},
		},
		TableValuedFunctions: map[string][]physical.TableValuedFunctionDescriptor{
			"max_diff_watermark": table_valued_functions.MaxDiffWatermark,
			"tumble":             table_valued_functions.Tumble,
		},
		PhysicalConfig:  nil,
		VariableContext: nil,
	}
	// TODO: Wrap panics into errors in subfunction.
	physicalPlan := logicalPlan.Typecheck(
		context.Background(),
		env,
		logical.Environment{
			CommonTableExpressions: map[string]physical.Node{},
		},
	)
	spew.Dump(physicalPlan.Schema)
	executionPlan, err := physicalPlan.Materialize(
		context.Background(),
		env,
	)

	wg := sync.WaitGroup{}
	wg.Add(1)
	mutex := sync.Mutex{}
	outputRecords := btree.New(execution.BTreeDefaultDegree)
	watermark := time.Time{}
	done := false

	go func() {
		defer wg.Done()
		liveWriter := uilive.New()

		for range time.Tick(time.Second / 4) {
			var buf bytes.Buffer

			table := tablewriter.NewWriter(&buf)
			table.SetColWidth(64)
			table.SetRowLine(false)
			header := make([]string, len(physicalPlan.Schema.Fields))
			for i := range physicalPlan.Schema.Fields {
				header[i] = physicalPlan.Schema.Fields[i].Name
			}
			table.SetHeader(header)
			table.SetAutoFormatHeaders(false)

			mutex.Lock()
			outputRecords.Ascend(func(item btree.Item) bool {
				itemTyped := item.(execution.GroupKeyIface)
				key := itemTyped.GetGroupKey()
				row := make([]string, len(key))
				for i := range key {
					row[i] = key[i].String()
				}
				table.Append(row)
				return true
			})
			watermark := watermark
			done := done
			mutex.Unlock()

			table.Render()

			fmt.Fprintf(&buf, "watermark: %s\n", watermark.Format(time.RFC3339Nano))

			buf.WriteTo(liveWriter)
			liveWriter.Flush()

			if done {
				return
			}
		}
	}()

	if err != nil {
		log.Fatal(err)
	}
	if err := executionPlan.Run(
		execution.ExecutionContext{
			Context:         context.Background(),
			VariableContext: nil,
		},
		func(ctx execution.ProduceContext, record execution.Record) error {
			mutex.Lock()
			if !record.Retraction {
				// This destroys duplicate records :)
				outputRecords.ReplaceOrInsert(execution.GroupKey(record.Values))
			} else {
				outputRecords.Delete(execution.GroupKey(record.Values))
			}
			mutex.Unlock()
			return nil
		},
		func(ctx execution.ProduceContext, msg execution.MetadataMessage) error {
			mutex.Lock()
			watermark = msg.Watermark
			mutex.Unlock()
			return nil
		},
	); err != nil {
		log.Fatal(err)
	}

	mutex.Lock()
	done = true
	mutex.Unlock()

	wg.Wait()
}
