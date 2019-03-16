package optimizer

import (
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/matcher"
)

type CandidateApprover func(match *matcher.Match) bool

type Scenario struct {
	Name              string
	Description       string
	CandidateMatcher  matcher.NodeMatcher
	CandidateApprover CandidateApprover
	Reassembler       func(match *matcher.Match) physical.Node
}
