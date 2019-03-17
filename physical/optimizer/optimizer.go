package optimizer

import (
	"context"

	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/matcher"
)

// CandidateApprover is used to approve a candidate match for optimizing.
// If it returns true, the optimizer *must* have something to do.
type CandidateApprover func(match *matcher.Match) bool

// Scenario is a single optimization definition.
type Scenario struct {
	Name        string
	Description string

	// CandidateMatcher matches nodes which may be targets for this optimization.
	CandidateMatcher matcher.NodeMatcher

	// CandidateApprover approves matched nodes, meaning this optimization will happen.
	// May be nil, if a match is enough.
	CandidateApprover CandidateApprover

	// Reassembler uses the variables in the match to create a new node, replacing the matched one.
	Reassembler func(match *matcher.Match) physical.Node
}

// Optimize runs an optimization loop.
// Terminates when there is nothing left to do.
func Optimize(ctx context.Context, scenarios []Scenario, plan physical.Node) physical.Node {
	changed := true
	for changed {
		changed = false
		nodeTransformer := func(node physical.Node) physical.Node {
			for i := range scenarios {
				match := matcher.NewMatch()
				matched := scenarios[i].CandidateMatcher.Match(match, node)
				if !matched {
					continue
				}
				if scenarios[i].CandidateApprover != nil {
					matched = scenarios[i].CandidateApprover(match)
					if !matched {
						continue
					}
				}
				changed = true
				return scenarios[i].Reassembler(match)
			}
			return node
		}
		plan = plan.Transform(ctx, &physical.Transformers{
			NodeT: nodeTransformer,
		})
	}
	return plan
}
