package physical

import (
	"context"
	"log"

	"github.com/cube2222/octosql/execution"
)

// Relation describes a comparison operator.
type Relation string

const (
	Equal        Relation = "equal"
	NotEqual     Relation = "not_equal"
	MoreThan     Relation = "more_than"
	LessThan     Relation = "less_than"
	Like         Relation = "like"
	In           Relation = "in"
	NotIn        Relation = "not_in"
	GreaterEqual Relation = "greater_equal"
	LessEqual    Relation = "less_equal"
	Regexp       Relation = "regexp"
)

func NewRelation(relation string) Relation {
	return Relation(relation)
}

func (rel Relation) Materialize(ctx context.Context, matCtx *MaterializationContext) execution.Relation {
	switch rel {
	case Equal:
		return execution.NewEqual()
	case NotEqual:
		return execution.NewNotEqual()
	case MoreThan:
		return execution.NewMoreThan()
	case LessThan:
		return execution.NewLessThan()
	case Like:
		return execution.NewLike()
	case In:
		return execution.NewIn()
	case NotIn:
		return execution.NewNotIn()
	case GreaterEqual:
		return execution.NewGreaterEqual()
	case LessEqual:
		return execution.NewLessEqual()
	case Regexp:
		return execution.NewRegexp()
	default:
		log.Fatalf("Invalid relation: %+v", rel) // This should be filtered at the logical plan level
		return nil
	}
}
