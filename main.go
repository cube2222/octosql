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
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/datasources/csv"
	"github.com/cube2222/octosql/datasources/json"
	"github.com/cube2222/octosql/datasources/postgres"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/functions"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/optimizer"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/table_valued_functions"
)

func main() {
	databaseCreators := map[string]func(configUntyped config.DatabaseSpecificConfig) (physical.Database, error){
		"postgres": postgres.Creator,
	}

	config.RegisterDatabaseType("postgres", func() config.DatabaseSpecificConfig { return &postgres.Config{} })
	cfg, err := config.Read()
	if err != nil {
		log.Fatal(err)
	}

	databases := make(map[string]physical.Database)
	for _, dbConfig := range cfg.Databases {
		db, err := databaseCreators[dbConfig.Type](dbConfig.Config)
		if err != nil {
			log.Fatal(err)
		}
		databases[dbConfig.Name] = db
	}

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
			"=":              functions.Equals,
			"int":            functions.Int,
			"time_from_unix": functions.TimeFromUnix,
		},
		Datasources: &physical.DatasourceRepository{
			Databases: databases,
			FileHandlers: map[string]func(name string) (physical.DatasourceImplementation, error){
				"json": json.Creator,
				"csv":  csv.Creator,
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
	physicalPlan = optimizer.Optimize(physicalPlan)
	executionPlan, err := physicalPlan.Materialize(
		context.Background(),
		env,
	)

	if err != nil {
		log.Fatal(err)
	}
	if err := executionPlan.Run(
		execution.ExecutionContext{
			Context:         context.Background(),
			VariableContext: nil,
		},
		func(ctx execution.ProduceContext, record execution.Record) error {
			log.Println(record.String())
			return nil
		},
		func(ctx execution.ProduceContext, msg execution.MetadataMessage) error {
			// log.Println(msg.Watermark)
			return nil
		},
	); err != nil {
		log.Fatal(err)
	}
}

type TableOutput struct {
	wg            sync.WaitGroup
	mutex         sync.Mutex
	outputRecords *btree.BTree
	watermark     time.Time
	done          bool
}

func NewTableOutput(schema physical.Schema) *TableOutput {
	to := &TableOutput{
		outputRecords: btree.New(execution.BTreeDefaultDegree),
	}

	go func() {
		defer to.wg.Done()
		liveWriter := uilive.New()

		for range time.Tick(time.Second / 4) {
			var buf bytes.Buffer

			table := tablewriter.NewWriter(&buf)
			table.SetColWidth(64)
			table.SetRowLine(false)
			header := make([]string, len(schema.Fields))
			for i := range schema.Fields {
				header[i] = schema.Fields[i].Name
			}
			table.SetHeader(header)
			table.SetAutoFormatHeaders(false)

			to.mutex.Lock()
			to.outputRecords.Ascend(func(item btree.Item) bool {
				itemTyped := item.(execution.GroupKeyIface)
				key := itemTyped.GetGroupKey()
				row := make([]string, len(key))
				for i := range key {
					row[i] = key[i].String()
				}
				table.Append(row)
				return true
			})
			watermark := to.watermark
			done := to.done
			to.mutex.Unlock()

			table.Render()

			fmt.Fprintf(&buf, "watermark: %s\n", watermark.Format(time.RFC3339Nano))

			buf.WriteTo(liveWriter)
			liveWriter.Flush()

			if done {
				return
			}
		}
	}()

	return to
}

func (to *TableOutput) SendRecord(ctx execution.ProduceContext, record execution.Record) error {
	to.mutex.Lock()
	if !record.Retraction {
		// TODO: This destroys duplicate records :) Should be counter.
		to.outputRecords.ReplaceOrInsert(execution.GroupKey(record.Values))
	} else {
		to.outputRecords.Delete(execution.GroupKey(record.Values))
	}
	to.mutex.Unlock()
	return nil
}

func (to *TableOutput) SendMeta(ctx execution.ProduceContext, msg execution.MetadataMessage) error {
	to.mutex.Lock()
	to.watermark = msg.Watermark
	to.mutex.Unlock()
	return nil
}

func (to *TableOutput) Stop() {
	to.mutex.Lock()
	to.done = true
	to.mutex.Unlock()
	to.wg.Wait()
}
