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

func (node *Requalifier) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Node {
	source := node.source.Typecheck(ctx, env, logicalEnv)
	outFields := make([]physical.SchemaField, len(source.Schema.Fields))
	for i, field := range source.Schema.Fields {
		name := field.Name
		if qualifiedNameRegexp.MatchString(name) {
			dotIndex := strings.Index(name, ".")
			name = fmt.Sprintf("%s.%s", node.qualifier, name[dotIndex+1:])
		} else {
			name = fmt.Sprintf("%s.%s", node.qualifier, name)
		}
		outFields[i] = physical.SchemaField{
			Name: name,
			Type: field.Type,
		}
	}

	return physical.Node{
		Schema:   physical.NewSchema(outFields, source.Schema.TimeField),
		NodeType: physical.NodeTypeRequalifier,
		Requalifier: &physical.Requalifier{
			Source:    source,
			Qualifier: node.qualifier,
		},
	}
}
