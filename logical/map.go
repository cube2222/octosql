package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Map struct {
	expressions       []Expression
	aliases           []string
	starQualifier     []string
	isStar            []bool
	objectExplosions  []Expression
	isObjectExplosion []bool
	source            Node
}

func NewMap(expressions []Expression, aliases []string, starQualifiers []string, isStar []bool, objectExplosions []Expression, isObjectExplosion []bool, child Node) *Map {
	return &Map{
		expressions:       expressions,
		aliases:           aliases,
		starQualifier:     starQualifiers,
		isStar:            isStar,
		objectExplosions:  objectExplosions,
		isObjectExplosion: isObjectExplosion,
		source:            child,
	}
}

func (node *Map) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	source, mapping := node.source.Typecheck(ctx, env, logicalEnv)
	reverseMapping := ReverseMapping(mapping)

	var expressions []physical.Expression
	var aliases []*string
	var unnests []int
	for i := range node.expressions {
		if node.isStar[i] {
			for _, field := range source.Schema.Fields {
				if qualifier := node.starQualifier[i]; qualifier != "" {
					if !strings.HasPrefix(reverseMapping[field.Name], qualifier+".") {
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
				unnests = append(unnests, 0)
			}
		} else if node.isObjectExplosion[i] {
			objectExpr := node.objectExplosions[i].Typecheck(ctx, env.WithRecordSchema(source.Schema), logicalEnv.WithRecordUniqueVariableNames(mapping))
			if objectExpr.Type.TypeID != octosql.TypeIDStruct {
				panic(fmt.Errorf("object explosion can only be invoked on an object, got %v", objectExpr.Type))
			}

			fieds := objectExpr.Type.Struct.Fields
			for i := range fieds {
				expressions = append(expressions, physical.Expression{
					Type:           fieds[i].Type,
					ExpressionType: physical.ExpressionTypeObjectFieldAccess,
					ObjectFieldAccess: &physical.ObjectFieldAccess{
						Object: objectExpr,
						Field:  fieds[i].Name,
					},
				})
				aliases = append(aliases, &fieds[i].Name)
				unnests = append(unnests, 0)
			}
		} else {
			unnestLevel := 0
			curExpression := node.expressions[i]
			for fe, ok := curExpression.(*FunctionExpression); ok && fe.Name == "unnest"; fe, ok = curExpression.(*FunctionExpression) {
				if len(fe.Arguments) != 1 {
					panic("unnest takes exactly 1 argument")
				}
				unnestLevel++
				curExpression = fe.Arguments[0]
			}
			unnests = append(unnests, unnestLevel)

			expressions = append(expressions, curExpression.Typecheck(ctx, env.WithRecordSchema(source.Schema), logicalEnv.WithRecordUniqueVariableNames(mapping)))
			if node.aliases[i] != "" {
				aliases = append(aliases, &node.aliases[i])
			} else {
				aliases = append(aliases, nil)
			}
		}
	}

	existingFields := make(map[string]int)
	outFields := make([]physical.SchemaField, len(expressions))
	outMapping := make(map[string]string)
	for i := range expressions {
		var name string
		if aliases[i] != nil {
			name = *aliases[i]
		} else if expressions[i].ExpressionType == physical.ExpressionTypeVariable {
			name = reverseMapping[expressions[i].Variable.Name]
		} else {
			name = fmt.Sprintf("col_%d", i)
		}
		existingCount := existingFields[name]
		if existingCount > 0 {
			// We don't want duplicate field names.
			name = fmt.Sprintf("%s_%d", name, existingCount)
		}
		existingFields[name] = existingCount + 1

		unique := logicalEnv.GetUnique(name)
		outMapping[name] = unique
		outFields[i] = physical.SchemaField{
			Name: unique,
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

	outputNode := physical.Node{
		Schema:   physical.NewSchema(outFields, outTimeFieldIndex, physical.WithNoRetractions(source.Schema.NoRetractions)),
		NodeType: physical.NodeTypeMap,
		Map: &physical.Map{
			Source:      source,
			Expressions: expressions,
		},
	}

	for i, level := range unnests {
		for j := 0; j < level; j++ {
			predecessorFields := outputNode.Schema.Fields

			if predecessorFields[i].Type.TypeID != octosql.TypeIDList {
				panic(fmt.Sprintf("unnest argument must be list, is %s", predecessorFields[i].Type))
			}

			unnestedFields := make([]physical.SchemaField, len(predecessorFields))
			copy(unnestedFields, predecessorFields[:i])
			unnestedFields[i] = physical.SchemaField{
				Name: predecessorFields[i].Name,
				Type: *predecessorFields[i].Type.List.Element,
			}
			if i < len(predecessorFields)-1 {
				copy(unnestedFields[i+1:], predecessorFields[i+1:])
			}

			outputNode = physical.Node{
				Schema:   physical.NewSchema(unnestedFields, outputNode.Schema.TimeField),
				NodeType: physical.NodeTypeUnnest,
				Unnest: &physical.Unnest{
					Source: outputNode,
					Field:  predecessorFields[i].Name,
				},
			}
		}
	}

	return outputNode, outMapping
}
