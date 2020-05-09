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

	"github.com/cube2222/octosql/physical"
)

type Telemetry struct {
	Version   string
	DeviceID  string
	OS        string
	Arch      string
	GoVersion string
	NumCPU    int
	Features  struct {
		GroupBy             bool
		Limit               bool
		Offset              bool
		LeftJoin            bool
		InnerJoin           bool
		Distinct            bool
		UnionAll            bool
		Interval            bool
		OrderBy             bool
		Like                bool
		Regexp              bool
		Function            bool
		TableValuedFunction bool
	}
	FunctionsUsed            map[string]bool
	TableValuedFunctionsUsed map[string]bool
	// TODO: Add datasource types of config.
	// TODO: Add datasource types of query.
}

func TelemetryTransformer(telemetry *Telemetry) *physical.Transformers {
	return &physical.Transformers{
		ExprT: func(expr physical.Expression) physical.Expression {
			switch expr := expr.(type) {
			case *physical.FunctionExpression:
				telemetry.Features.Function = true
				telemetry.FunctionsUsed[expr.Name] = true
			}

			return expr
		},
		NodeT: func(node physical.Node) physical.Node {
			switch node := node.(type) {
			case *physical.GroupBy:
				telemetry.Features.GroupBy = true
			case *physical.Limit:
				telemetry.Features.Limit = true
			case *physical.Offset:
				telemetry.Features.Offset = true
			case *physical.Distinct:
				telemetry.Features.Distinct = true
			case *physical.OrderBy:
				telemetry.Features.OrderBy = true
			case *physical.TableValuedFunction:
				telemetry.Features.TableValuedFunction = true
				telemetry.TableValuedFunctionsUsed[node.Name] = true
			}

			return node
		},
		FormulaT: func(formula physical.Formula) physical.Formula {
			switch formula := formula.(type) {
			case *physical.Predicate:
				switch formula.Relation {
				case physical.Like:
					telemetry.Features.Like = true
				case physical.Regexp:
					telemetry.Features.Regexp = true
				}
			}

			return formula
		},
	}
}

func SendTelemetry(ctx context.Context, telemetry *Telemetry) {
	telemetry.DeviceID = GetDeviceID(ctx)
	telemetry.OS = runtime.GOOS
	telemetry.Arch = runtime.GOARCH
	telemetry.GoVersion = runtime.Version()
	telemetry.NumCPU = runtime.NumCPU()

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
		err := ioutil.WriteFile(deviceIDFilePath, []byte(newID), os.ModePerm)
		if err != nil {
			return "error2"
		}
		return newID
	} else if err != nil {
		return "error3"
	}
	return string(storedID)
}
