package functions

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
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
		"!=": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
				OutputType:    octosql.Boolean,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					// TODO: Null should probably not equal Null.
					return octosql.NewBoolean(values[0].Compare(values[1]) != 0), nil
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
		// arithmetic operators
		"+": {
			{
				ArgumentTypes: []octosql.Type{octosql.Int, octosql.Int},
				OutputType:    octosql.Int,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewInt(values[0].Int + values[1].Int), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Float, octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(values[0].Float + values[1].Float), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Duration},
				OutputType:    octosql.Duration,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewDuration(values[0].Duration + values[1].Duration), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Time, octosql.Duration},
				OutputType:    octosql.Time,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewTime(values[0].Time.Add(values[1].Duration)), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Time},
				OutputType:    octosql.Time,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewTime(values[1].Time.Add(values[0].Duration)), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.String, octosql.String},
				OutputType:    octosql.String,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewString(values[0].Str + values[1].Str), nil
				},
			},
		},
		"-": {
			{
				ArgumentTypes: []octosql.Type{octosql.Int, octosql.Int},
				OutputType:    octosql.Int,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewInt(values[0].Int - values[1].Int), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Float, octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(values[0].Float - values[1].Float), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Duration},
				OutputType:    octosql.Duration,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewDuration(values[0].Duration - values[1].Duration), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Time, octosql.Duration},
				OutputType:    octosql.Time,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewTime(values[0].Time.Add(-values[1].Duration)), nil
				},
			},
		},
		"*": {
			{
				ArgumentTypes: []octosql.Type{octosql.Int, octosql.Int},
				OutputType:    octosql.Int,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewInt(values[0].Int * values[1].Int), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Float, octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(values[0].Float * values[1].Float), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Int},
				OutputType:    octosql.Duration,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewDuration(values[0].Duration * time.Duration(values[1].Int)), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Int, octosql.Duration},
				OutputType:    octosql.Duration,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewDuration(values[1].Duration * time.Duration(values[0].Int)), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.String, octosql.Int},
				OutputType:    octosql.String,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewString(strings.Repeat(values[0].Str, values[1].Int)), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Int, octosql.String},
				OutputType:    octosql.String,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewString(strings.Repeat(values[1].Str, values[0].Int)), nil
				},
			},
		},
		"/": {
			{
				ArgumentTypes: []octosql.Type{octosql.Int, octosql.Int},
				OutputType:    octosql.Int,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewInt(values[0].Int / values[1].Int), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Float, octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(values[0].Float / values[1].Float), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Int},
				OutputType:    octosql.Duration,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewDuration(values[0].Duration / time.Duration(values[1].Int)), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Duration},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(float64(values[0].Duration) / float64(values[1].Duration)), nil
				},
			},
		},
		// math
		"abs": {
			{
				ArgumentTypes: []octosql.Type{octosql.Int},
				OutputType:    octosql.Int,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					if values[0].Int > 0 {
						return values[0], nil
					}
					return octosql.NewInt(values[0].Int * -1), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(math.Abs(values[0].Float)), nil
				},
			},
		},
		"sqrt": {
			{
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(math.Sqrt(values[0].Float)), nil
				},
			},
		},
		"ceil": {
			{
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(math.Ceil(values[0].Float)), nil
				},
			},
		},
		"floor": {
			{
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(math.Floor(values[0].Float)), nil
				},
			},
		},
		"log2": {
			{
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(math.Log2(values[0].Float)), nil
				},
			},
		},
		"ln": {
			{
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(math.Log(values[0].Float)), nil
				},
			},
		},
		"log10": {
			{
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(math.Log10(values[0].Float)), nil
				},
			},
		},
		"pow": {
			{
				ArgumentTypes: []octosql.Type{octosql.Float, octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(math.Pow(values[0].Float, values[1].Float)), nil
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
		"upper": {
			{
				ArgumentTypes: []octosql.Type{octosql.String},
				OutputType:    octosql.String,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewString(strings.ToUpper(values[0].Str)), nil
				},
			},
		},
		"lower": {
			{
				ArgumentTypes: []octosql.Type{octosql.String},
				OutputType:    octosql.String,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewString(strings.ToLower(values[0].Str)), nil
				},
			},
		},
		"reverse": {
			{
				ArgumentTypes: []octosql.Type{octosql.String},
				OutputType:    octosql.String,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					out := make([]rune, len(values[0].Str))
					for i, ch := range values[0].Str {
						out[len(out)-i-1] = ch
					}
					return octosql.NewString(string(out)), nil
				},
			},
		},
		"substr": {
			{
				ArgumentTypes: []octosql.Type{octosql.String, octosql.Int},
				OutputType:    octosql.String,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					if len(values[0].Str) <= values[1].Int {
						return octosql.NewString(""), nil
					}
					return octosql.NewString(values[0].Str[values[1].Int:]), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.String, octosql.Int, octosql.Int},
				OutputType:    octosql.String,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					if len(values[0].Str) <= values[1].Int {
						return octosql.NewString(""), nil
					}
					end := values[1].Int + values[2].Int
					if end > len(values[0].Str) {
						end = len(values[0].Str)
					}
					return octosql.NewString(values[0].Str[values[1].Int:end]), nil
				},
			},
		},
		"replace": {
			{
				ArgumentTypes: []octosql.Type{octosql.String, octosql.String, octosql.String},
				OutputType:    octosql.String,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewString(strings.Replace(values[0].Str, values[1].Str, values[2].Str, -1)), nil
				},
			},
		},
		"len": {
			{
				ArgumentTypes: []octosql.Type{octosql.String},
				OutputType:    octosql.Int,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewInt(len(values[0].Str)), nil
				},
			},
		},
		// TODO: Regexp Match with capture of first group.
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
		// Conversions
		"int": {
			{
				// This case will catch any types which may be int at the start of non-exact matching.
				// So the int function can be used as a type cast.
				ArgumentTypes: []octosql.Type{octosql.Int},
				OutputType:    octosql.Int,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return values[0], nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Int,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewInt(int(values[0].Float)), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.String},
				OutputType:    octosql.Int,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					n, err := strconv.Atoi(values[0].Str)
					if err != nil {
						return octosql.Value{}, fmt.Errorf("couldn't parse string '%s' as int: %s", values[0].Str, err)
					}
					return octosql.NewInt(n), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Duration},
				OutputType:    octosql.Int,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewInt(int(values[0].Duration)), nil
				},
			},
		},
		"float": {
			{
				// This case will catch any types which may be float at the start of non-exact matching.
				// So the int function can be used as a type cast.
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return values[0], nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Int},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(float64(values[0].Int)), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.String},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					n, err := strconv.ParseFloat(values[0].Str, 64)
					if err != nil {
						return octosql.Value{}, fmt.Errorf("couldn't parse string '%s' as float: %s", values[0].Str, err)
					}
					return octosql.NewFloat(n), nil
				},
			},
			{
				ArgumentTypes: []octosql.Type{octosql.Duration},
				OutputType:    octosql.Float,
				Strict:        true,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewFloat(float64(values[0].Duration)), nil
				},
			},
		},
	}
}
