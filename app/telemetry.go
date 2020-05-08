package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/cube2222/octosql/physical"
)

type Telemetry struct {
	UserID   string
	Features struct {
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
	data, err := json.Marshal(telemetry)
	if err != nil {
		return
	}

	req, err := http.NewRequest(http.MethodPost, "http://localhost:8080/telemetry", bytes.NewReader(data))
	if err != nil {
		return
	}

	go http.DefaultClient.Do(req)
}
