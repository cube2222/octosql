package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/optimizer"
	"github.com/cube2222/octosql/physical"
)

type impl struct {
	config *Config
	schema physical.Schema
	table  string
}

func (i *impl) Schema() (physical.Schema, error) {
	return i.schema, nil
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment) (execution.Node, error) {
	// Prepare statement
	db, err := connect(i.config)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to database: %w", err)
	}
	fields := make([]string, len(i.schema.Fields))
	for index := range i.schema.Fields {
		fields[index] = i.schema.Fields[index].Name
	}
	stmt, err := db.PrepareEx(ctx, uuid.Must(uuid.NewV4()).String(), fmt.Sprintf("SELECT %s FROM %s", strings.Join(fields, ", "), i.table), nil)
	if err != nil {
		return nil, fmt.Errorf("couldn't prepare statement: %w", err)
	}
	return &DatasourceExecuting{
		fields: i.schema.Fields,
		table:  i.table,
		db:     db,
		stmt:   stmt,
	}, nil
}

func (i *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected []physical.Expression, newPushedDown []physical.Expression, changed bool) {
	newPushedDown = make([]physical.Expression, len(pushedDownPredicates))
	copy(newPushedDown, pushedDownPredicates)
	for _, pred := range newPredicates {
		isOk := true
		predicateChecker := optimizer.Transformers{
			ExpressionTransformer: func(expr physical.Expression) physical.Expression {
				switch expr.ExpressionType {
				case physical.ExpressionTypeVariable:
				case physical.ExpressionTypeConstant:
				case physical.ExpressionTypeFunctionCall:
					switch expr.FunctionCall.Name {
					case ">", ">=", "=", "<", "<=":
					default:
						isOk = false
					}
				case physical.ExpressionTypeAnd:
				case physical.ExpressionTypeOr:
				default:
					isOk = false
				}
				return expr
			},
		}
		predicateChecker.TransformExpr(pred)
		if isOk {
			newPushedDown = append(newPushedDown, pred)
		} else {
			rejected = append(rejected, pred)
		}
	}
	changed = len(newPushedDown) > len(pushedDownPredicates)
	panic("implement me")
	return
}
