package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/physical"
)

type Map struct {
	expressions   []Expression
	aliases       []string
	starQualifier []string
	isStar        []bool
	source        Node
}

func NewMap(expressions []Expression, aliases []string, starQualifiers []string, isStar []bool, child Node) *Map {
	return &Map{expressions: expressions, aliases: aliases, starQualifier: starQualifiers, isStar: isStar, source: child}
}

func (node *Map) Typecheck(ctx context.Context, env physical.Environment, state physical.State) physical.Node {
	source := node.source.Typecheck(ctx, env, state)

	var expressions []physical.Expression
	var aliases []*string
	for i := range node.isStar {
		if !node.isStar[i] {
			expressions = append(expressions, node.expressions[i].Typecheck(ctx, env.WithRecordSchema(source.Schema), state))
			if node.aliases[i] != "" {
				aliases = append(aliases, &node.aliases[i])
			} else {
				aliases = append(aliases, nil)
			}
		} else {
			for _, field := range source.Schema.Fields {
				if qualifier := node.starQualifier[i]; qualifier != "" {
					if !strings.HasPrefix(field.Name, qualifier+".") { // TODO: Is the additional dot required?
						continue
					}
				}
				expressions = append(expressions, physical.Expression{
					Type:           field.Type,
					ExpressionType: physical.ExpressionTypeVariable,
					Variable: &physical.Variable{
						Name:     field.Name,
						IsLevel0: true,
					},
				})
				aliases = append(aliases, nil)
			}
		}
	}

	existingFields := make(map[string]int)
	outFields := make([]physical.SchemaField, len(expressions))
	for i := range expressions {
		var name string
		if aliases[i] != nil {
			name = *aliases[i]
		} else if expressions[i].ExpressionType == physical.ExpressionTypeVariable {
			name = expressions[i].Variable.Name
		} else {
			name = fmt.Sprintf("col_%d", i)
		}
		existingCount := existingFields[name]
		if existingCount > 0 {
			// We don't want duplicate field names.
			name = fmt.Sprintf("%s_%d", name, existingCount)
		}
		existingFields[name] = existingCount + 1
		outFields[i] = physical.SchemaField{
			Name: name,
			Type: expressions[i].Type,
		}
	}

	outTimeFieldIndex := -1
	if source.Schema.TimeField != -1 {
		sourceTimeFieldName := source.Schema.Fields[source.Schema.TimeField].Name
		for i := range expressions {
			if expressions[i].ExpressionType == physical.ExpressionTypeVariable {
				if expressions[i].Variable.Name == sourceTimeFieldName {
					outTimeFieldIndex = i
					break
				}
			}
		}
	}

	return physical.Node{
		Schema:   physical.NewSchema(outFields, outTimeFieldIndex),
		NodeType: physical.NodeTypeMap,
		Map: &physical.Map{
			Source:      source,
			Expressions: expressions,
			Aliases:     aliases,
		},
	}
}
