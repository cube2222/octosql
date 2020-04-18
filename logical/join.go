package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
)

type Join struct {
	source     Node
	joined     Node
	isLeftJoin bool
}

func NewJoin(source, joined Node, isLeftJoin bool) *Join {
	return &Join{
		source:     source,
		joined:     joined,
		isLeftJoin: isLeftJoin,
	}
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

	sourceShuffled := physical.NewShuffle(1, sourceNodes, physical.DefaultShuffleStrategy)
	joinedShuffled := physical.NewShuffle(1, joinedNodes, physical.DefaultShuffleStrategy)
	outNodes := make([]physical.Node, len(sourceShuffled))

	// Based on the cardinality of sources we decide whether we will create a stream_join or a lookup_join
	// Stream joins support only equity conjunctions (i.e a.x = b.y AND a.v + 17 = b.something * 2)
	sourceCardinality := sourceNodes[0].Metadata().Cardinality()
	joinedCardinality := joinedNodes[0].Metadata().Cardinality()

	isStreamJoin := false

	if sourceCardinality == metadata.Unbounded && joinedCardinality == metadata.Unbounded {
		isStreamJoin = true
	} else if sourceCardinality == metadata.Unbounded && joinedCardinality == metadata.BoundedFitsInLocalStorage {
		isStreamJoin = true
	} else if sourceCardinality == metadata.BoundedFitsInLocalStorage && joinedCardinality == metadata.Unbounded {
		isStreamJoin = true
	}

	if isStreamJoin {
		var formula physical.Formula
		if filter, ok := joinedNodes[0].(*physical.Filter); ok {
			formula = filter.Formula
		} else {
			formula = physical.NewConstant(true)
		}

		// We check if the ON part of the join is legal for a stream join
		if !isConjunctionOfEqualities(formula) {
			return nil, nil, errors.New("The ON part of join isn't a conjunction of equalities")
		}

		// Create necessary namespaces of source and joined
		sourceNamespace := sourceNodes[0].Metadata().Namespace()
		sourceNamespace.MergeWithVariables(sourceVariables)

		joinedNamespace := joinedNodes[0].Metadata().Namespace()
		joinedNamespace.MergeWithVariables(joinedVariables)

		sourceEventTimeField := sourceNodes[0].Metadata().EventTimeField()
		joinedEventTimeField := joinedNodes[0].Metadata().EventTimeField()

		// Create the appropriate keys from the formula. Basically how it works is it goes through predicates i.e a.x = b.y
		// and then decides whether a.x forms part of source or joined and then adds it to the appropriate key.
		sourceKey, joinedKey, eventTimeField, err := getKeysAndEventTimeFromFormula(formula, sourceNamespace, joinedNamespace, sourceEventTimeField, joinedEventTimeField)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't create keys from join formula")
		}

		for i := range outNodes {
			outNodes[i] = physical.NewStreamJoin(sourceShuffled[i], joinedShuffled[i], sourceKey, joinedKey, eventTimeField, node.isLeftJoin)
		}
	} else { // if the conditions for a stream join aren't met we create a lookup join
		for i := range outNodes {
			outNodes[i] = physical.NewLookupJoin(sourceShuffled[i], joinedShuffled[i], node.isLeftJoin)
		}
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

func getKeysAndEventTimeFromFormula(formula physical.Formula, sourceNamespace, joinedNamespace *metadata.Namespace, sourceEventTimeField, joinedEventTimeField octosql.VariableName) ([]physical.Expression, []physical.Expression, octosql.VariableName, error) {
	var sourceKey, joinedKey []physical.Expression
	eventTimeField := octosql.NewVariableName("") // will stay empty if we don't join by event time

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
			return nil, nil, "", errors.New("Left expression of predicate doesn't match either sources namespace")
		} else if !doesRightMatchSource && !doesRightMatchJoined {
			return nil, nil, "", errors.New("Right expression of predicate doesn't match either sources namespace")
		} else if !doesLeftMatchSource && !doesRightMatchSource {
			return nil, nil, "", errors.New("Neither side of predicate matches sources namespace")
		} else if !doesLeftMatchJoined && !doesRightMatchJoined {
			return nil, nil, "", errors.New("Neither side of predicate matches joined sources namespace")
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

		// Now we have added a new expression to the appropriate keys
		// we want to check whether we are joining by event time

		lastSourceExpression := sourceKey[len(sourceKey)-1] // slice won't be empty, since we just appended to it
		lastJoinedExpression := joinedKey[len(joinedKey)-1]

		if sourceVariable, ok := lastSourceExpression.(*physical.Variable); ok {
			if joinedVariable, ok := lastJoinedExpression.(*physical.Variable); ok {
				if sourceVariable.Name == sourceEventTimeField &&
					joinedVariable.Name == joinedEventTimeField {
					// We are joining by event time field
					eventTimeField = octosql.NewVariableName("join_event_time") // TODO: what about this?
				}
			}
		}
	}

	return sourceKey, joinedKey, eventTimeField, nil
}
