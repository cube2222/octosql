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

func RunTelemetry(ctx context.Context, telemetryInfo TelemetryInfo, datasources []config.DataSourceConfig, plan physical.Node) {
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
	SendTelemetry(ctx, &telemetry)
}

type TelemetryInfo struct {
	OutputFormat string
	Version      string
}

type Telemetry struct {
	Version         string
	DeviceID        string
	OS              string
	Arch            string
	GoVersion       string
	NumCPU          int
	GoMaxProcs      int
	ExpressionCount int
	ExpressionsUsed struct {
		Function       bool
		NodeExpression bool
		StarExpression bool
		Tuple          bool
	}
	FormulaCount int
	FormulasUsed struct {
		In     bool
		Like   bool
		Regexp bool
	}
	NodeCount int
	NodesUsed struct {
		GroupBy             bool
		Limit               bool
		Offset              bool
		LookupJoin          bool
		StreamJoin          bool
		Distinct            bool
		UnionAll            bool
		Interval            bool
		OrderBy             bool
		TableValuedFunction bool
	}
	TriggersUsed struct {
		Counting  bool
		Delay     bool
		Watermark bool
	}
	FunctionsUsed            map[string]bool
	TableValuedFunctionsUsed map[string]bool
	DatasourceTypesInConfig  map[string]bool
	DatasourceTypesUsed      map[string]bool
	OutputFormat             string
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
			case *physical.Distinct:
				telemetry.NodesUsed.Distinct = true
			case *physical.GroupBy:
				telemetry.NodesUsed.GroupBy = true
			case *physical.Limit:
				telemetry.NodesUsed.Limit = true
			case *physical.LookupJoin:
				telemetry.NodesUsed.LookupJoin = true
			case *physical.Offset:
				telemetry.NodesUsed.Offset = true
			case *physical.OrderBy:
				telemetry.NodesUsed.OrderBy = true
			case *physical.StreamJoin:
				telemetry.NodesUsed.StreamJoin = true
			case *physical.TableValuedFunction:
				telemetry.NodesUsed.TableValuedFunction = true
				telemetry.TableValuedFunctionsUsed[node.Name] = true
			case *physical.DataSourceBuilder:
				for i := range datasources {
					if datasources[i].Name == node.Name {
						telemetry.DatasourceTypesUsed[datasources[i].Type] = true
						break
					}
				}
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
		log.Println("telemetry took: ", time.Since(start))
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
