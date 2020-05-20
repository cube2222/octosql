package logical

import (
	"context"
	"fmt"
	"runtime"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
)

var ErrFallbackToLookupJoin = errors.New("fallback to lookup join")

type Join struct {
	source   Node
	joined   Node
	joinType execution.JoinType
	triggers []Trigger
}

func NewJoin(source, joined Node, joinType execution.JoinType) *Join {
	return &Join{
		source:   source,
		joined:   joined,
		joinType: joinType,
	}
}

func (node *Join) Visualize() *graph.Node {
	n := graph.NewNode("Join")

	n.AddChild("source", node.source.Visualize())
	n.AddChild("joined", node.joined.Visualize())

	for i, trigger := range node.triggers {
		n.AddChild(fmt.Sprintf("trigger_%d", i), trigger.Visualize())
	}

	n.AddChild("join_type", graph.NewNode(node.joinType.String()))
	return n
}

func (node *Join) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	sourceNodes, sourceVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for join source nodes")
	}

	joinedNodes, joinedVariables, err := node.joined.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for left join joined nodes")
	}

	variables, err := sourceVariables.MergeWith(joinedVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables for source and joined nodes")
	}

	// Based on the cardinality of sources we decide whether we will create a stream_join or a lookup_join
	// Stream joins support only equity conjunctions (i.e a.x = b.y AND a.v + 17 = b.something * 2)
	sourceCardinality := sourceNodes[0].Metadata().Cardinality()
	joinedCardinality := joinedNodes[0].Metadata().Cardinality()

	isStreamJoin := false
	canBeLookupJoin := true

	if sourceCardinality == metadata.Unbounded && joinedCardinality == metadata.Unbounded {
		isStreamJoin = true
		canBeLookupJoin = false // can't do a lookup join between two unbounded sources
	} else if joinedCardinality == metadata.BoundedFitsInLocalStorage || sourceCardinality == metadata.BoundedFitsInLocalStorage {
		isStreamJoin = true
	}

	// Lookup join doesn't support outer join, so it must be a stream join
	if node.joinType == execution.OUTER_JOIN {
		isStreamJoin = true
		canBeLookupJoin = false
	}

	if isStreamJoin {
		outNodes, variables, err := node.physicalStreamJoin(ctx, physicalCreator, variables, sourceNodes, joinedNodes)

		// If the ON part of join isn't supported by stream join we can fallback to lookup join if we can (so no OUTER join and no two unbounded sources)
		if err == ErrFallbackToLookupJoin {
			if !canBeLookupJoin {
				return nil, nil, errors.New("provided join statement can't be made into a stream join nor a lookup join")
			}
		} else if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't create physical.StreamJoin")
		} else {
			return outNodes, variables, err
		}
	}

	// If the conditions for a stream join aren't met we create a lookup join
	isLeftJoin := node.joinType == execution.LEFT_JOIN

	sourceShuffled := physical.NewShuffle(1, physical.NewConstantStrategy(0), sourceNodes)
	joinedShuffled := physical.NewShuffle(1, physical.NewConstantStrategy(0), joinedNodes)
	outNodes := make([]physical.Node, len(sourceShuffled))

	for i := range outNodes {
		outNodes[i] = physical.NewLookupJoin(sourceShuffled[i], joinedShuffled[i], isLeftJoin)
	}

	return outNodes, variables, nil
}

func (node *Join) physicalStreamJoin(ctx context.Context, physicalCreator *PhysicalPlanCreator, variables octosql.Variables, sourceNodes, joinedNodes []physical.Node) ([]physical.Node, octosql.Variables, error) {
	var formula physical.Formula
	// If the joined node is a formula, it means there are some conditions in the ON part of the join
	// (otherwise it's just two nodes). We take the formula from the filter to create the key, and set the joined node
	// as the source of the filter (basically we get rid of the formula node-wise, create key expressions for it,
	// and the sources are the source and the source of the filter)
	if filter, ok := joinedNodes[0].(*physical.Filter); ok {
		formula = filter.Formula
		for i := range joinedNodes {
			joinedNodes[i] = joinedNodes[i].(*physical.Filter).Source
		}
	} else {
		formula = physical.NewConstant(true)
	}

	// We check if the ON part of the join is legal for a stream join

	if !isConjunctionOfEqualities(formula) {
		return nil, nil, ErrFallbackToLookupJoin
	}

	// Create necessary namespaces of source and joined
	sourceNamespace := sourceNodes[0].Metadata().Namespace() // TODO: should these be merged with variables
	joinedNamespace := joinedNodes[0].Metadata().Namespace()
	eventTimeField := sourceNodes[0].Metadata().EventTimeField()

	// Create the appropriate keys from the formula. Basically how it works is it goes through predicates i.e a.x = b.y
	// and then decides whether a.x forms part of source or joined and then adds it to the appropriate key.
	sourceKey, joinedKey, err := getKeysFromFormula(formula, sourceNamespace, joinedNamespace)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create keys from join formula")
	}

	// Get the number of partitions into which the stream join will be split
	streamJoinParallelism, err := config.GetInt(
		physicalCreator.physicalConfig,
		"streamJoinParallelism",
		config.WithDefault(runtime.GOMAXPROCS(0)),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get streamJoinParallelism configuration")

	}

	// Create physical.Triggers from logical.Triggers
	triggers := make([]physical.Trigger, len(node.triggers))
	for i := range node.triggers {
		out, triggerVariables, err := node.triggers[i].Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get physical plan for trigger with index %d", i)
		}
		variables, err = variables.MergeWith(triggerVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't merge variables with those of trigger with index %d", i)
		}

		triggers[i] = out
	}

	sourceShuffled := physical.NewShuffle(streamJoinParallelism, physical.NewKeyHashingStrategy(sourceKey), sourceNodes)
	joinedShuffled := physical.NewShuffle(streamJoinParallelism, physical.NewKeyHashingStrategy(joinedKey), joinedNodes)

	for i := range sourceShuffled {
		sourceShuffled[i] = physical.NewNextShuffleMetadataChange("_left", i, sourceShuffled[i])
	}

	for i := range joinedShuffled {
		joinedShuffled[i] = physical.NewNextShuffleMetadataChange("_right", i, joinedShuffled[i])
	}

	outNodes := make([]physical.Node, len(sourceShuffled))

	for i := range outNodes {
		outNodes[i] = physical.NewStreamJoin(sourceShuffled[i], joinedShuffled[i], sourceKey, joinedKey, eventTimeField, node.joinType, triggers)
	}

	return outNodes, variables, nil
}

func isConjunctionOfEqualities(f physical.Formula) bool {
	switch f := f.(type) {
	case *physical.And:
		return isConjunctionOfEqualities(f.Left) && isConjunctionOfEqualities(f.Right)
	case *physical.Predicate:
		return f.Relation == physical.Equal
	case *physical.Constant:
		return f.Value
	default:
		return false
	}
}

func getKeysFromFormula(formula physical.Formula, sourceNamespace, joinedNamespace *metadata.Namespace) ([]physical.Expression, []physical.Expression, error) {
	sourceKey := make([]physical.Expression, 0)
	joinedKey := make([]physical.Expression, 0)

	for _, element := range formula.SplitByAnd() {
		if _, ok := element.(*physical.Constant); ok {
			continue
		}

		predicate := element.(*physical.Predicate) // this won't error since we only have predicates and constants

		leftExpression := predicate.Left
		rightExpression := predicate.Right

		doesLeftMatchSource := leftExpression.DoesMatchNamespace(sourceNamespace)
		doesLeftMatchJoined := leftExpression.DoesMatchNamespace(joinedNamespace)
		doesRightMatchSource := rightExpression.DoesMatchNamespace(sourceNamespace)
		doesRightMatchJoined := rightExpression.DoesMatchNamespace(joinedNamespace)

		// We check for errors. For example a predicate a.x - b.y = 0 won't have any match on the left formula
		if !doesLeftMatchSource && !doesLeftMatchJoined {
			return nil, nil, errors.New("left expression of predicate doesn't match either sources namespace")
		} else if !doesRightMatchSource && !doesRightMatchJoined {
			return nil, nil, errors.New("right expression of predicate doesn't match either sources namespace")
		} else if !doesLeftMatchSource && !doesRightMatchSource {
			return nil, nil, errors.New("neither side of predicate matches sources namespace")
		} else if !doesLeftMatchJoined && !doesRightMatchJoined {
			return nil, nil, errors.New("neither side of predicate matches joined sources namespace")
		}

		// Now we want to decide which part of the predicate belong to which key
		if !doesLeftMatchSource {
			// If left doesn't match source, then right surely does and also left must match joined
			sourceKey = append(sourceKey, rightExpression)
			joinedKey = append(joinedKey, leftExpression)
		} else if !doesLeftMatchJoined {
			// Likewise if left doesn't match joined, then right surely does and also left must match source
			sourceKey = append(sourceKey, leftExpression)
			joinedKey = append(joinedKey, rightExpression)
		} else if !doesRightMatchSource {
			// Left matches both, but right doesn't match source, so right is joined, left is source
			sourceKey = append(sourceKey, leftExpression)
			joinedKey = append(joinedKey, rightExpression)
		} else {
			// Left matches both, and right matches source, so right is source, left is joined
			sourceKey = append(sourceKey, rightExpression)
			joinedKey = append(joinedKey, leftExpression)
		}
	}

	return sourceKey, joinedKey, nil
}

func (node *Join) WithTriggers(triggers []Trigger) *Join {
	return &Join{
		source:   node.source,
		joined:   node.joined,
		joinType: node.joinType,
		triggers: triggers,
	}
}
