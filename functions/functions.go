package functions

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

// TODO: Change this to the final map in place.
func FunctionMap() map[string][]physical.FunctionDescriptor {
	return map[string][]physical.FunctionDescriptor{
		// Comparisons
		"<": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
				OutputType:    octosql.Boolean,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					// TODO: Null should probably not equal Null.
					return octosql.NewBoolean(values[0].Compare(values[1]) < 0), nil
				},
			},
		},
		"<=": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
				OutputType:    octosql.Boolean,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					// TODO: Null should probably not equal Null.
					return octosql.NewBoolean(values[0].Compare(values[1]) <= 0), nil
				},
			},
		},
		// TODO: Maybe equals shouldn't be a function? It has very specific type checking needs.
		// TODO: Adding function strictness fixes the problem of NULL equality.
		"=": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
				OutputType:    octosql.Boolean,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					// TODO: Null should probably not equal Null.
					return octosql.NewBoolean(values[0].Compare(values[1]) == 0), nil
				},
			},
		},
		">=": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
				OutputType:    octosql.Boolean,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					// TODO: Null should probably not equal Null.
					return octosql.NewBoolean(values[0].Compare(values[1]) >= 0), nil
				},
			},
		},
		">": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
				OutputType:    octosql.Boolean,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					// TODO: Null should probably not equal Null.
					return octosql.NewBoolean(values[0].Compare(values[1]) > 0), nil
				},
			},
		},
		// logic
		"not": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Boolean},
				OutputType:    octosql.Boolean,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewBoolean(!values[0].Boolean), nil
				},
			},
		},
		// strings
		"like": {
			{
				ArgumentTypes: []octosql.Type{octosql.String, octosql.String},
				OutputType:    octosql.Boolean,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					const likeEscape = '\\'
					const likeAny = '_'
					const likeAll = '%'

					needsEscaping := func(r rune) bool {
						return r == '+' ||
							r == '?' ||
							r == '(' ||
							r == ')' ||
							r == '{' ||
							r == '}' ||
							r == '[' ||
							r == ']' ||
							r == '^' ||
							r == '$' ||
							r == '.'
					}

					// we assume that the escape character is '\'
					likePatternToRegexp := func(pattern string) (string, error) {
						var sb strings.Builder
						sb.WriteRune('^') // match start

						escaping := false // was the character previously seen an escaping \

						for _, r := range pattern {
							if escaping { // escaping \, _ and % is legal (we just write . or .*), otherwise an error occurs
								if r != likeAny && r != likeAll && r != likeEscape {
									return "", fmt.Errorf("escaping invalid character in LIKE pattern: %v", r)
								}

								escaping = false
								sb.WriteRune(r)

								if r == likeEscape {
									// since _ and % don't need to be escaped in regexp we just replace \_ with _
									// but \ needs to be replaced in both, so we need to write an additional \
									sb.WriteRune(likeEscape)
								}
							} else {
								if r == likeEscape { // if we find an escape sequence we just handle it in the next step
									escaping = true
								} else if r == likeAny { // _ transforms to . (any character)
									sb.WriteRune('.')
								} else if r == likeAll { // % transforms to .* (any string)
									sb.WriteString(".*")
								} else if needsEscaping(r) { // escape characters that might break the regexp
									sb.WriteRune('\\')
									sb.WriteRune(r)
								} else { // just write everything else
									sb.WriteRune(r)
								}
							}
						}

						sb.WriteRune('$') // match end

						if escaping {
							return "", fmt.Errorf("pattern ends with an escape character that doesn't escape anything")
						}

						return sb.String(), nil
					}

					patternString, err := likePatternToRegexp(values[1].Str)
					if err != nil {
						return octosql.Value{}, fmt.Errorf("couldn't transform LIKE pattern %s to regexp: %w", values[1].Str, err)
					}

					reg, err := regexp.Compile(patternString)
					if err != nil {
						return octosql.Value{}, fmt.Errorf("couldn't compile LIKE pattern regexp expression: '%s' => '%s': %w", values[1].Str, patternString, err)
					}

					return octosql.NewBoolean(reg.MatchString(values[0].Str)), nil
				},
			},
		},
		"~": {
			{
				ArgumentTypes: []octosql.Type{octosql.String, octosql.String},
				OutputType:    octosql.Boolean,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					reg, err := regexp.Compile(values[1].Str)
					if err != nil {
						return octosql.Value{}, fmt.Errorf("couldn't compile ~ pattern regexp expression: '%s': %w", values[1].Str, err)
					}

					return octosql.NewBoolean(reg.MatchString(values[0].Str)), nil
				},
			},
		},
		"~*": {
			{
				ArgumentTypes: []octosql.Type{octosql.String, octosql.String},
				OutputType:    octosql.Boolean,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					reg, err := regexp.Compile(strings.ToLower(values[1].Str))
					if err != nil {
						return octosql.Value{}, fmt.Errorf("couldn't compile ~* pattern regexp expression: '%s': %w", values[1].Str, err)
					}

					return octosql.NewBoolean(reg.MatchString(strings.ToLower(values[0].Str))), nil
				},
			},
		},
		"int": {
			{
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Int,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewInt(int(values[0].Float)), nil
				},
			},
		},
		// time
		"now": {
			{
				ArgumentTypes: []octosql.Type{},
				OutputType:    octosql.Time,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewTime(time.Now()), nil
				},
			},
		},
		"time_from_unix": {
			{
				ArgumentTypes: []octosql.Type{octosql.Int},
				OutputType:    octosql.Time,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewTime(time.Unix(int64(values[0].Int), 0)), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Time,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewTime(time.Unix(int64(values[0].Float), 0)), nil
				},
			},
		},
	}
}
