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
			g.Printfln("if %s {", predicateValue.AsType(octosql.TypeIDBoolean))
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
					panic(fmt.Sprintf("implement me: %s", node.GroupBy.AggregateExpressions[i].Type.TypeID))
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
				keyValues[i] = g.Expression(ctx.WithRecord(record), node.GroupBy.Key[i]).DebugRawReference()
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
						g.Printfln("%s.aggregate%d.Sum += %s", aggregateValues, i, aggregateArgument.AsType(octosql.TypeIDInt))
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
		g.Printfln("_ = %s", key)
		g.Printfln("_ = %s", aggregate)
		fields := make([]RecordField, len(node.GroupBy.Key)+len(node.GroupBy.Aggregates))
		for i := range node.GroupBy.Key {
			keyValue := g.DeclareVariable(fmt.Sprintf("key%dValue", i), node.GroupBy.Key[i].Type)
			fields[i] = RecordField{
				Name:  node.Schema.Fields[i].Name,
				Value: keyValue,
			}
		}
		for i := range node.GroupBy.Aggregates {
			aggregateValue := g.DeclareVariable(fmt.Sprintf("aggregate%dValue", i), node.GroupBy.Aggregates[i].OutputType)
			g.Printfln("%s = %s.aggregate%d.Value()", aggregateValue.AsType(node.GroupBy.Aggregates[i].OutputType.TypeID), aggregate, i)

			fields[len(node.GroupBy.Key)+i] = RecordField{
				Name:  node.Schema.Fields[len(node.GroupBy.Key)+i].Name,
				Value: aggregateValue,
			}
		}
		produce(Record{Fields: fields})
		g.Printfln("}")

	case physical.NodeTypeDatasource:
		// TODO: Handle pushed-down predicates.
		uniqueToColname := make(map[string]string)
		colnameToUnique := make(map[string]string)
		for k, v := range node.Datasource.VariableMapping {
			trimmed := strings.TrimPrefix(k, node.Datasource.Alias+".")
			uniqueToColname[v] = trimmed
			colnameToUnique[trimmed] = v
		}

		fieldsOriginalNames := make([]physical.SchemaField, len(node.Schema.Fields))
		for i := range node.Schema.Fields {
			fieldsOriginalNames[i] = physical.SchemaField{
				Name: uniqueToColname[node.Schema.Fields[i].Name],
				Type: node.Schema.Fields[i].Type,
			}
		}
		schemaOriginalNames := physical.NewSchema(fieldsOriginalNames, node.Schema.TimeField)

		node.Datasource.DatasourceImplementation.(GenerateableDataSource).Generate(g, ctx, schemaOriginalNames, func(record Record) {
			recordFields := make([]RecordField, len(record.Fields))
			for i := range record.Fields {
				recordFields[i] = RecordField{
					Name:  colnameToUnique[record.Fields[i].Name],
					Value: record.Fields[i].Value,
				}
			}
			produce(Record{Fields: recordFields})
		})

	case physical.NodeTypeTableValuedFunction:
		switch node.TableValuedFunction.Name {
		case "range":
			index := g.Unique("index")
			start := g.Expression(ctx, node.TableValuedFunction.Arguments["start"].Expression.Expression)
			end := g.Expression(ctx, node.TableValuedFunction.Arguments["end"].Expression.Expression)
			g.Printfln("for %s := %s; %s < %s; %s++ {", index, start.AsType(octosql.TypeIDInt), index, end.AsType(octosql.TypeIDInt), index)
			indexValue := g.DeclareVariable("indexValue", octosql.Int)
			g.Printfln("%s = %s", indexValue.AsType(octosql.TypeIDInt), index)
			produce(Record{
				Fields: []RecordField{
					{
						Name:  node.Schema.Fields[0].Name,
						Value: indexValue,
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
