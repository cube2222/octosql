package logical

import (
	"context"
	"strings"

	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Relation string

const (
	Equal    Relation = "="
	NotEqual Relation = "<>"
	MoreThan Relation = ">"
	LessThan Relation = "<"
	Like     Relation = "like"
	In       Relation = "in"
)

func NewRelation(relation string) Relation {
	return Relation(relation)
}

func (rel Relation) Physical(ctx context.Context) (physical.Relation, error) {
	switch Relation(strings.ToLower(string(rel))) {
	case Equal:
		return physical.Equal, nil
	case NotEqual:
		return physical.NotEqual, nil
	case MoreThan:
		return physical.MoreThan, nil
	case LessThan:
		return physical.LessThan, nil
	case Like:
		return physical.Like, nil
	case In:
		return physical.In, nil
	default:
		return "", errors.Errorf("invalid relation %s", rel)
	}
}
