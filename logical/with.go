package logical

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

type With struct {
	cteNames []string
	cteNodes []Node
	source   Node
}

func NewWith(cteNames []string, cteNodes []Node, source Node) *With {
	return &With{
		cteNames: cteNames,
		cteNodes: cteNodes,
		source:   source,
	}
}

func (node *With) Typecheck(ctx context.Context, env physical.Environment, state physical.State) physical.Node {
	panic("implement me")
	// variables := octosql.NoVariables()
	// for i := range node.cteNodes {
	// 	physicalNode, nodeVariables, err := node.cteNodes[i].Physical(ctx, physicalCreator)
	// 	if err != nil {
	// 		return nil, nil, errors.Wrapf(
	// 			err,
	// 			"couldn't get physical plan for common table expression %s with index %d", node.cteNames[i], i,
	// 		)
	// 	}
	// 	variables, err = variables.MergeWith(nodeVariables)
	// 	if err != nil {
	// 		return nil, nil, errors.Wrapf(
	// 			err,
	// 			"couldn't merge variables with those of common table expression %s with index %d", node.cteNames[i], i,
	// 		)
	// 	}
	//
	// 	// Add this common table expression to the available datasources so further CTE's can access this one.
	// 	*physicalCreator = *physicalCreator.WithCommonTableExpression(node.cteNames[i], physicalNode)
	// }
	//
	// left, childVariables, err := node.left.Physical(ctx, physicalCreator)
	// if err != nil {
	// 	return nil, nil, errors.Wrap(err, "couldn't get physical plan for with left node")
	// }
	//
	// variables, err = childVariables.MergeWith(variables)
	// if err != nil {
	// 	return nil, nil, errors.Wrap(err, "couldn't merge variables for with left")
	// }
	//
	// return left, variables, nil
}
