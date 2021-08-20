package logical

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/cube2222/octosql/physical"
)

type Requalifier struct {
	qualifier string
	source    Node
}

func NewRequalifier(qualifier string, child Node) *Requalifier {
	return &Requalifier{qualifier: qualifier, source: child}
}

var qualifiedNameRegexp = regexp.MustCompile(`^[^.]+\..+$`)

func (node *Requalifier) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	source, mapping := node.source.Typecheck(ctx, env, logicalEnv)

	outMapping := make(map[string]string)
	for name, unique := range mapping {
		if qualifiedNameRegexp.MatchString(name) {
			dotIndex := strings.Index(name, ".")
			name = fmt.Sprintf("%s.%s", node.qualifier, name[dotIndex+1:])
		} else {
			name = fmt.Sprintf("%s.%s", node.qualifier, name)
		}
		outMapping[name] = unique
	}

	return source, outMapping
}
