package formats

import (
	"strings"

	"github.com/cube2222/octosql/physical"
)

func withoutQualifiers(fields []physical.SchemaField) []physical.SchemaField {
	shortName := func(name string) string {
		if strings.Contains(name, ".") {
			name = strings.SplitN(name, ".", 2)[1]
		}
		return name
	}

	nameCount := map[string]int{}
	for i := range fields {
		nameCount[shortName(fields[i].Name)]++
	}

	outFields := make([]physical.SchemaField, len(fields))
	for i := range fields {
		name := fields[i].Name
		if short := shortName(fields[i].Name); nameCount[short] == 1 {
			name = short
		}
		outFields[i] = physical.SchemaField{
			Name: name,
			Type: fields[i].Type,
		}
	}
	return outFields
}
