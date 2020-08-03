package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/mitchellh/go-homedir"
	"github.com/oklog/ulid"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/physical"
)

func RunTelemetry(ctx context.Context, telemetryInfo TelemetryInfo, datasources []config.DataSourceConfig, plan physical.Node, outputOptions *physical.OutputOptions) {
	var telemetry Telemetry

	telemetry.FunctionsUsed = make(map[string]bool)
	telemetry.TableValuedFunctionsUsed = make(map[string]bool)
	telemetry.DatasourceTypesInConfig = make(map[string]bool)
	telemetry.DatasourceTypesUsed = make(map[string]bool)

	telemetry.DeviceID = GetDeviceID(ctx)
	telemetry.OS = runtime.GOOS
	telemetry.Arch = runtime.GOARCH
	telemetry.GoVersion = runtime.Version()
	telemetry.NumCPU = runtime.NumCPU()
	telemetry.GoMaxProcs = runtime.GOMAXPROCS(0)
	telemetry.OutputFormat = telemetryInfo.OutputFormat
	telemetry.Version = telemetryInfo.Version

	for _, datasourceConfig := range datasources {
		telemetry.DatasourceTypesInConfig[datasourceConfig.Type] = true
	}
	plan.Transform(ctx, TelemetryTransformer(&telemetry, datasources))
	telemetry.NodesUsed.Limit = outputOptions.Limit != nil
	telemetry.NodesUsed.Offset = outputOptions.Offset != nil
	telemetry.NodesUsed.OrderBy = telemetry.NodesUsed.OrderBy || len(outputOptions.OrderByExpressions) > 0
	log.Printf("Sending telemetry: %+v", telemetry)
	SendTelemetry(ctx, &telemetry)
}

type TelemetryInfo struct {
	OutputFormat string
	Version      string
}

type Telemetry struct {
	Version         string `json:"version"`
	DeviceID        string `json:"device_id"`
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	GoVersion       string `json:"go_version"`
	NumCPU          int    `json:"num_cpu"`
	GoMaxProcs      int    `json:"go_max_procs"`
	ExpressionCount int    `json:"expression_count"`
	ExpressionsUsed struct {
		Function       bool `json:"function"`
		NodeExpression bool `json:"node_expression"`
		StarExpression bool `json:"star_expression"`
		Tuple          bool `json:"tuple"`
	} `json:"expressions_used"`
	FormulaCount int `json:"formula_count"`
	FormulasUsed struct {
		In     bool `json:"in"`
		Like   bool `json:"like"`
		Regexp bool `json:"regexp"`
	} `json:"formulas_used"`
	NodeCount int `json:"node_count"`
	NodesUsed struct {
		Distinct            bool `json:"distinct"`
		GroupBy             bool `json:"group_by"`
		Limit               bool `json:"limit"`
		LookupJoin          bool `json:"lookup_join"`
		Offset              bool `json:"offset"`
		OrderBy             bool `json:"order_by"`
		StreamJoin          bool `json:"stream_join"`
		TableValuedFunction bool `json:"table_valued_function"`
	} `json:"nodes_used"`
	TriggersUsed struct {
		Counting  bool `json:"counting"`
		Delay     bool `json:"delay"`
		Watermark bool `json:"watermark"`
	} `json:"triggers_used"`
	FunctionsUsed            map[string]bool `json:"functions_used"`
	TableValuedFunctionsUsed map[string]bool `json:"table_valued_functions_used"`
	DatasourceTypesInConfig  map[string]bool `json:"datasource_types_in_config"`
	DatasourceTypesUsed      map[string]bool `json:"datasource_types_used"`
	OutputFormat             string          `json:"output_format"`
}

func TelemetryTransformer(telemetry *Telemetry, datasources []config.DataSourceConfig) *physical.Transformers {
	return &physical.Transformers{
		ExprT: func(expr physical.Expression) physical.Expression {
			telemetry.ExpressionCount++

			switch expr := expr.(type) {
			case *physical.FunctionExpression:
				telemetry.ExpressionsUsed.Function = true
				telemetry.FunctionsUsed[expr.Name] = true
			case *physical.NodeExpression:
				telemetry.ExpressionsUsed.NodeExpression = true
			case *physical.StarExpression:
				telemetry.ExpressionsUsed.StarExpression = true
			case *physical.Tuple:
				telemetry.ExpressionsUsed.Tuple = true
			}

			return expr
		},
		FormulaT: func(formula physical.Formula) physical.Formula {
			telemetry.FormulaCount++

			switch formula := formula.(type) {
			case *physical.Predicate:
				switch formula.Relation {
				case physical.In:
					telemetry.FormulasUsed.In = true
				case physical.Like:
					telemetry.FormulasUsed.Like = true
				case physical.Regexp:
					telemetry.FormulasUsed.Regexp = true
				}
			}

			return formula
		},
		NodeT: func(node physical.Node) physical.Node {
			telemetry.NodeCount++

			switch node := node.(type) {
			case *physical.DataSourceBuilder:
				for i := range datasources {
					if datasources[i].Name == node.Name {
						telemetry.DatasourceTypesUsed[datasources[i].Type] = true
						break
					}
				}
			case *physical.Distinct:
				telemetry.NodesUsed.Distinct = true
			case *physical.GroupBy:
				telemetry.NodesUsed.GroupBy = true
			case *physical.LookupJoin:
				telemetry.NodesUsed.LookupJoin = true
			case *physical.OrderBy:
				telemetry.NodesUsed.OrderBy = true
			case *physical.StreamJoin:
				telemetry.NodesUsed.StreamJoin = true
			case *physical.TableValuedFunction:
				telemetry.NodesUsed.TableValuedFunction = true
				telemetry.TableValuedFunctionsUsed[node.Name] = true
			}

			return node
		},
		TriggerT: func(trigger physical.Trigger) physical.Trigger {
			switch trigger.(type) {
			case *physical.CountingTrigger:
				telemetry.TriggersUsed.Counting = true
			case *physical.DelayTrigger:
				telemetry.TriggersUsed.Delay = true
			case *physical.WatermarkTrigger:
				telemetry.TriggersUsed.Watermark = true
			}

			return trigger
		},
	}
}

func SendTelemetry(ctx context.Context, telemetry *Telemetry) {
	data, err := json.Marshal(telemetry)
	if err != nil {
		return
	}

	req, err := http.NewRequest(http.MethodPost, "https://telemetry.octosql.dev/telemetry", bytes.NewReader(data))
	if err != nil {
		return
	}

	start := time.Now()
	go func() {
		http.DefaultClient.Do(req.WithContext(ctx))
		log.Println("Telemetry took: ", time.Since(start))
	}()
}

func GetDeviceID(ctx context.Context) string {
	dir, err := homedir.Dir()
	if err != nil {
		return "error1"
	}
	deviceIDFilePath := path.Join(dir, ".octosql", "deviceid")
	storedID, err := ioutil.ReadFile(deviceIDFilePath)
	if os.IsNotExist(err) {
		newID := ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
		err := os.Mkdir(path.Join(dir, ".octosql"), os.ModePerm)
		if err != nil {
			return "error2"
		}
		err = ioutil.WriteFile(deviceIDFilePath, []byte(newID), os.ModePerm)
		if err != nil {
			return "error3"
		}
		return newID
	} else if err != nil {
		return "error4"
	}
	return string(storedID)
}
