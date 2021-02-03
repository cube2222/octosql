package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
)

type TableValuedFunctionArgumentValue interface {
	iTableValuedFunctionArgumentValue()
	Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.TableValuedFunctionArgumentValue, octosql.Variables, error)
}

func (*TableValuedFunctionArgumentValueExpression) iTableValuedFunctionArgumentValue() {}
func (*TableValuedFunctionArgumentValueTable) iTableValuedFunctionArgumentValue()      {}
func (*TableValuedFunctionArgumentValueDescriptor) iTableValuedFunctionArgumentValue() {}

type TableValuedFunctionArgumentValueExpression struct {
	expression Expression
}

func NewTableValuedFunctionArgumentValueExpression(expression Expression) *TableValuedFunctionArgumentValueExpression {
	return &TableValuedFunctionArgumentValueExpression{expression: expression}
}

func (arg *TableValuedFunctionArgumentValueExpression) Typecheck(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.TableValuedFunctionArgumentValue, octosql.Variables, error) {
	panic("implement me")
}

type TableValuedFunctionArgumentValueTable struct {
	source Node
}

func NewTableValuedFunctionArgumentValueTable(source Node) *TableValuedFunctionArgumentValueTable {
	return &TableValuedFunctionArgumentValueTable{source: source}
}

func (arg *TableValuedFunctionArgumentValueTable) Typecheck(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.TableValuedFunctionArgumentValue, octosql.Variables, error) {
	panic("implement me")
	// sourceNodes, variables, err := arg.source.Physical(ctx, physicalCreator)
	// if err != nil {
	// 	return nil, nil, errors.Wrap(err, "couldn't get physical node")
	// }
	//
	// outputArguments := make([]physical.TableValuedFunctionArgumentValue, len(sourceNodes))
	// for i := range sourceNodes {
	// 	outputArguments[i] = physical.NewTableValuedFunctionArgumentValueTable(sourceNodes[i])
	// }
	//
	// return outputArguments, variables, nil
}

type TableValuedFunctionArgumentValueDescriptor struct {
	descriptor octosql.VariableName
}

func NewTableValuedFunctionArgumentValueDescriptor(descriptor octosql.VariableName) *TableValuedFunctionArgumentValueDescriptor {
	return &TableValuedFunctionArgumentValueDescriptor{descriptor: descriptor}
}

func (arg *TableValuedFunctionArgumentValueDescriptor) Typecheck(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.TableValuedFunctionArgumentValue, octosql.Variables, error) {
	panic("implement me")
}

type TableValuedFunction struct {
	name      string
	arguments map[string]TableValuedFunctionArgumentValue
}

func NewTableValuedFunction(name string, arguments map[octosql.VariableName]TableValuedFunctionArgumentValue) *TableValuedFunction {
	return &TableValuedFunction{name: name, arguments: arguments}
}

func (node *TableValuedFunction) Typecheck(ctx context.Context, env physical.Environment, state physical.State) ([]physical.Node, error) {
	panic("implement me")
	// variables := octosql.NoVariables()
	//
	// physArguments := make(map[octosql.VariableName][]physical.TableValuedFunctionArgumentValue)
	// for k, v := range node.arguments {
	// 	physArg, argVariables, err := v.Physical(ctx, physicalCreator)
	// 	if err != nil {
	// 		return nil, nil, errors.Wrapf(
	// 			err,
	// 			"couldn't get physical plan for table valued function argument \"%s\"", k,
	// 		)
	// 	}
	//
	// 	variables, err = variables.MergeWith(argVariables)
	// 	if err != nil {
	// 		return nil, nil, errors.Wrapf(
	// 			err,
	// 			"couldn't merge variables with those of table valued function argument \"%s\"", k,
	// 		)
	// 	}
	//
	// 	physArguments[k] = physArg
	// }
	//
	// // We only want one source node with multiple partitions for a table valued function, otherwise partitioning gets nasty.
	// // So we find it here if it exists.
	// multipartitionCount := 0
	// multipartitionArgumentName := octosql.NewVariableName("")
	// for arg, argValue := range physArguments {
	// 	if len(argValue) > 1 {
	// 		multipartitionArgumentName = arg
	// 		multipartitionCount++
	// 	}
	// }
	//
	// // If there is more that one multipartition stream input, we error.
	// if multipartitionCount > 1 {
	// 	return nil, octosql.NoVariables(), errors.Errorf("only one source node with multiple partitions allowed for table valued function, got %d", multipartitionCount)
	// }
	//
	// // If there are no multipartition input streams, we return a single partition.
	// if multipartitionCount == 0 {
	// 	singleArguments := make(map[octosql.VariableName]physical.TableValuedFunctionArgumentValue)
	// 	for k, v := range physArguments {
	// 		singleArguments[k] = v[0]
	// 	}
	//
	// 	return []physical.Node{physical.NewTableValuedFunction(
	// 		node.name,
	// 		singleArguments,
	// 	)}, variables, nil
	// }
	//
	// // Otherwise we add one instance of this table valued function to each partition, and return the resulting partitions.
	// multipartitionSourceNodes := physArguments[multipartitionArgumentName]
	//
	// outNodes := make([]physical.Node, len(multipartitionSourceNodes))
	// for i, sourceNode := range physArguments[multipartitionArgumentName] {
	// 	singleArguments := make(map[octosql.VariableName]physical.TableValuedFunctionArgumentValue)
	// 	for k, v := range physArguments {
	// 		if k == multipartitionArgumentName {
	// 			continue
	// 		}
	// 		singleArguments[k] = v[0]
	// 	}
	// 	singleArguments[multipartitionArgumentName] = sourceNode
	//
	// 	outNodes[i] = physical.NewTableValuedFunction(
	// 		node.name,
	// 		singleArguments,
	// 	)
	// }
	//
	// return outNodes, variables, nil
}
