package formats

import (
	"strings"

	"github.com/cube2222/octosql/physical"
)

func withoutQualifiers(fields []physical.SchemaField) []physical.SchemaField {
	outFields := make([]physical.SchemaField, len(fields))
	for i := range fields {
		name := fields[i].Name
		if strings.Contains(name, ".") {
			name = strings.SplitN(name, ".", 2)[1]
		}
		outFields[i] = physical.SchemaField{
			Name: name,
			Type: fields[i].Type,
		}
	}
	return outFields
}
