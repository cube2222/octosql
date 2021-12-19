package telemetry

import (
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/manager"
)

type QueryTelemetryData struct {
	NodeTypesUsed       []string `json:"node_types_used"`
	ExpressionTypesUsed []string `json:"expression_types_used"`
	TriggerTypesUsed    []string `json:"trigger_types_used"`
	FunctionsUsed       []string `json:"functions_used"`
	AggregatesUsed      []string `json:"aggregates_used"`
	PluginsInstalled    []string `json:"plugins_installed"`
}

func GetQueryTelemetryData(node physical.Node, pluginsInstalled []manager.PluginMetadata) QueryTelemetryData {
	nodeTypesUsed := make(map[string]bool)
	expressionTypesUsed := make(map[string]bool)
	triggerTypesUsed := make(map[string]bool)
	functionsUsed := make(map[string]bool)
	aggregatesUsed := make(map[string]bool)
	(&physical.Transformers{
		NodeTransformer: func(node physical.Node) physical.Node {
			nodeTypesUsed[node.NodeType.String()] = true
			if node.NodeType == physical.NodeTypeGroupBy {
				getTriggerTypes(triggerTypesUsed, node.GroupBy.Trigger)
				for _, agg := range node.GroupBy.Aggregates {
					aggregatesUsed[agg.Name] = true
				}
			}
			return node
		},
		ExpressionTransformer: func(expr physical.Expression) physical.Expression {
			expressionTypesUsed[expr.ExpressionType.String()] = true
			if expr.ExpressionType == physical.ExpressionTypeFunctionCall {
				functionsUsed[expr.FunctionCall.Name] = true
			}
			return expr
		},
	}).TransformNode(node)
	out := QueryTelemetryData{}
	for k := range nodeTypesUsed {
		out.NodeTypesUsed = append(out.NodeTypesUsed, k)
	}
	for k := range expressionTypesUsed {
		out.ExpressionTypesUsed = append(out.ExpressionTypesUsed, k)
	}
	for k := range triggerTypesUsed {
		out.TriggerTypesUsed = append(out.TriggerTypesUsed, k)
	}
	for k := range functionsUsed {
		out.FunctionsUsed = append(out.FunctionsUsed, k)
	}
	for k := range aggregatesUsed {
		out.AggregatesUsed = append(out.AggregatesUsed, k)
	}
	for _, plugin := range pluginsInstalled {
		out.PluginsInstalled = append(out.PluginsInstalled, plugin.Reference.String())
	}
	return out
}

func getTriggerTypes(triggerTypesUsed map[string]bool, trigger physical.Trigger) {
	triggerTypesUsed[trigger.TriggerType.String()] = true
	if trigger.TriggerType == physical.TriggerTypeMulti {
		for _, t := range trigger.MultiTrigger.Triggers {
			getTriggerTypes(triggerTypesUsed, t)
		}
	}
}
