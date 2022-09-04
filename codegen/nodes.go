package codegen

import (
	"fmt"
	"strings"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func (g *CodeGenerator) Node(ctx Context, node physical.Node, produce func(reference Record)) {
	switch node.NodeType {
	case physical.NodeTypeFilter:
		g.Node(ctx, node.Filter.Source, func(record Record) {
			predicateValue := g.Expression(ctx.WithRecord(record), node.Filter.Predicate)
			g.Printfln("if %s {", predicateValue.Reference)
			produce(record)
			g.Printfln("}")
		})
	case physical.NodeTypeMap:
		g.Node(ctx, node.Map.Source, func(record Record) {
			fields := make([]RecordField, len(node.Map.Expressions))
			for i := range node.Map.Expressions {
				fields[i] = RecordField{
					Name:  node.Schema.Fields[i].Name,
					Value: g.Expression(ctx.WithRecord(record), node.Map.Expressions[i]),
				}
			}
			produce(Record{
				Fields: fields,
			})
		})
	case physical.NodeTypeGroupBy:
		keyType := g.Unique("keyType")
		g.Printfln("type %s struct {", keyType)
		for i := range node.GroupBy.Key {
			g.Printfln("key%d %s", i, GoType(node.GroupBy.Key[i].Type))
		}
		g.Printfln("}")

		aggregateType := g.Unique("aggregateType")
		g.Printfln("type %s struct {", aggregateType)
		for i := range node.GroupBy.Aggregates {
			var curAggregateType string
			switch node.GroupBy.Aggregates[i].Name {
			// TODO: Move to descriptor.
			case "count":
				curAggregateType = "lib.AggregateCount"
			case "sum":
				switch node.GroupBy.AggregateExpressions[i].Type.TypeID {
				case octosql.TypeIDInt:
					curAggregateType = "lib.AggregateSumInt"
				default:
					panic("implement me")
				}
			default:
				panic("implement me")
			}
			g.Printfln("aggregate%d %s", i, curAggregateType)
		}
		g.Printfln("}")

		aggregates := g.Unique("aggregates")
		g.Printfln("%s := make(map[%s]%s)", aggregates, keyType, aggregateType)
		g.Node(ctx, node.GroupBy.Source, func(record Record) {
			keyValues := make([]string, len(node.GroupBy.Key))
			for i := range node.GroupBy.Key {
				keyValues[i] = g.Expression(ctx.WithRecord(record), node.GroupBy.Key[i]).Reference
			}
			key := fmt.Sprintf("%s{%s}", keyType, strings.Join(keyValues, ", "))
			aggregateValues := g.Unique("aggregateValues")
			ok := g.Unique("ok")
			g.Printfln("%s, %s := %s[%s]", aggregateValues, ok, aggregates, key)
			g.Printfln("if !%s {", ok)
			g.Printfln("%s = %s{}", aggregateValues, aggregateType)
			g.Printfln("}")

			for i := range node.GroupBy.Aggregates {
				aggregateArgument := g.Expression(ctx.WithRecord(record), node.GroupBy.AggregateExpressions[i])
				switch node.GroupBy.Aggregates[i].Name {
				// TODO: Move to descriptor.
				case "count":
					g.Printfln("%s.aggregate%d.Count++", aggregateValues, i)
				case "sum":
					switch node.GroupBy.AggregateExpressions[i].Type.TypeID {
					case octosql.TypeIDInt:
						g.Printfln("%s.aggregate%d.Sum += %s", aggregateValues, i, aggregateArgument.Reference)
					default:
						panic("implement me")
					}
				default:
					panic("implement me")
				}
			}

			g.Printfln("%s[%s] = %s", aggregates, key, aggregateValues)
		})
		key := g.Unique("key")
		aggregate := g.Unique("aggregate")
		g.Printfln("for %s, %s := range %s {", key, aggregate, aggregates)
		fields := make([]RecordField, len(node.GroupBy.Key)+len(node.GroupBy.Aggregates))
		for i := range node.GroupBy.Key {
			fields[i] = RecordField{
				Name: node.Schema.Fields[i].Name,
				Value: Value{
					Type:      node.Schema.Fields[i].Type,
					Reference: fmt.Sprintf("%s.key%d", key, i),
				},
			}
		}
		for i := range node.GroupBy.Aggregates {
			aggregateValue := g.Unique(fmt.Sprintf("aggregate%dValue", i))
			g.Printfln("%s := %s.aggregate%d.Value()", aggregateValue, aggregate, i)

			fields[len(node.GroupBy.Key)+i] = RecordField{
				Name: node.Schema.Fields[len(node.GroupBy.Key)+i].Name,
				Value: Value{
					Type:      node.Schema.Fields[len(node.GroupBy.Key)+i].Type,
					Reference: aggregateValue,
				},
			}
		}
		produce(Record{Fields: fields})
		g.Printfln("}")

	case physical.NodeTypeTableValuedFunction:
		switch node.TableValuedFunction.Name {
		case "range":
			index := g.Unique("index")
			start := g.Expression(ctx, node.TableValuedFunction.Arguments["start"].Expression.Expression)
			end := g.Expression(ctx, node.TableValuedFunction.Arguments["end"].Expression.Expression)
			g.Printfln("for %s := %s; %s < %s; %s++ {", index, start.Reference, index, end.Reference, index)
			produce(Record{
				Fields: []RecordField{
					{
						Name:  node.Schema.Fields[0].Name,
						Value: Value{Type: octosql.Int, Reference: index},
					},
				},
			})
			g.Printfln("}")
		default:
			panic("implement me")
		}
	default:
		panic("implement me")
	}
}
