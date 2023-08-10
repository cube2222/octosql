package functions

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/compute"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func FunctionMap() map[string]physical.FunctionDetails {
	return map[string]physical.FunctionDetails{
		// // Comparisons
		// "<": {
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			TypeFn: func(types []octosql.Type) (octosql.Type, bool) {
		// 				if len(types) != 2 {
		// 					return octosql.Type{}, false
		// 				}
		// 				if !types[0].Equals(types[1]) {
		// 					return octosql.Type{}, false
		// 				}
		// 				return octosql.Boolean, true
		// 			},
		// 			Strict: true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewBoolean(values[0].Compare(values[1]) < 0), nil
		// 			},
		// 		},
		// 	},
		// },
		// "<=": {
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			TypeFn: func(types []octosql.Type) (octosql.Type, bool) {
		// 				if len(types) != 2 {
		// 					return octosql.Type{}, false
		// 				}
		// 				if !types[0].Equals(types[1]) {
		// 					return octosql.Type{}, false
		// 				}
		// 				return octosql.Boolean, true
		// 			},
		// 			Strict: true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewBoolean(values[0].Compare(values[1]) <= 0), nil
		// 			},
		// 		},
		// 	},
		// },
		"=": {
			Descriptors: []physical.FunctionDescriptor{
				// TODO: Fix this for Arrow and unions.
				{
					ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
					OutputType:    octosql.Boolean,
					Strict:        true,
					Function: func(arguments []arrow.Array) (arrow.Array, error) {
						args := make([]compute.Datum, len(arguments))
						for i, arg := range arguments {
							args[i] = &compute.ArrayDatum{Value: arg.Data()}
						}

						fn, ok := compute.GetFunctionRegistry().GetFunction("equal")
						if !ok {
							panic("Bug: equal function not found")
						}
						out, err := fn.Execute(context.Background(), nil, args...)
						if err != nil {
							return nil, fmt.Errorf("couldn't execute function: %w", err)
						}
						return array.MakeFromData(out.(*compute.ArrayDatum).Value), nil
					},
				},
			},
		},
		// "!=": {
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		// TODO: Specializations for concrete primitive types.
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
		// 			OutputType:    octosql.Boolean,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewBoolean(!values[0].Equal(values[1])), nil
		// 			},
		// 		},
		// 	},
		// },
		// ">=": {
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			TypeFn: func(types []octosql.Type) (octosql.Type, bool) {
		// 				if len(types) != 2 {
		// 					return octosql.Type{}, false
		// 				}
		// 				if !types[0].Equals(types[1]) {
		// 					return octosql.Type{}, false
		// 				}
		// 				return octosql.Boolean, true
		// 			},
		// 			Strict: true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewBoolean(values[0].Compare(values[1]) >= 0), nil
		// 			},
		// 		},
		// 	},
		// },
		// ">": {
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			TypeFn: func(types []octosql.Type) (octosql.Type, bool) {
		// 				if len(types) != 2 {
		// 					return octosql.Type{}, false
		// 				}
		// 				if !types[0].Equals(types[1]) {
		// 					return octosql.Type{}, false
		// 				}
		// 				return octosql.Boolean, true
		// 			},
		// 			Strict: true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewBoolean(values[0].Compare(values[1]) > 0), nil
		// 			},
		// 		},
		// 	},
		// },
		// "is null": {
		// 	Description: "Returns true only if the argument is null.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Any},
		// 			OutputType:    octosql.Boolean,
		// 			Strict:        false,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				if values[0].TypeID == octosql.TypeIDNull {
		// 					return octosql.NewBoolean(true), nil
		// 				}
		// 				return octosql.NewBoolean(false), nil
		// 			},
		// 		},
		// 	},
		// },
		// "is not null": {
		// 	Description: "Returns true only if the argument is not null.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Any},
		// 			OutputType:    octosql.Boolean,
		// 			Strict:        false,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				if values[0].TypeID == octosql.TypeIDNull {
		// 					return octosql.NewBoolean(false), nil
		// 				}
		// 				return octosql.NewBoolean(true), nil
		// 			},
		// 		},
		// 	},
		// },
		// // arithmetic operators
		"+": {
			Descriptors: []physical.FunctionDescriptor{
				{
					ArgumentTypes: []octosql.Type{octosql.Int, octosql.Int},
					OutputType:    octosql.Int,
					Strict:        true,
					Function: func(values []arrow.Array) (arrow.Array, error) {
						out, err := compute.Add(context.Background(), compute.ArithmeticOptions{
							NoCheckOverflow: true,
						}, &compute.ArrayDatum{Value: values[0].Data()}, &compute.ArrayDatum{Value: values[1].Data()})
						if err != nil {
							return nil, fmt.Errorf("couldn't add arrays: %w", err)
						}
						return array.MakeFromData(out.(*compute.ArrayDatum).Value), nil
					},
				},
				// {
				// 	ArgumentTypes: []octosql.Type{octosql.Float, octosql.Float},
				// 	OutputType:    octosql.Float,
				// 	Strict:        true,
				// 	Function: func(values []octosql.Value) (octosql.Value, error) {
				// 		return octosql.NewFloat(values[0].Float + values[1].Float), nil
				// 	},
				// },
				// {
				// 	ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Duration},
				// 	OutputType:    octosql.Duration,
				// 	Strict:        true,
				// 	Function: func(values []octosql.Value) (octosql.Value, error) {
				// 		return octosql.NewDuration(values[0].Duration + values[1].Duration), nil
				// 	},
				// },
				// {
				// 	ArgumentTypes: []octosql.Type{octosql.Time, octosql.Duration},
				// 	OutputType:    octosql.Time,
				// 	Strict:        true,
				// 	Function: func(values []octosql.Value) (octosql.Value, error) {
				// 		return octosql.NewTime(values[0].Time.Add(values[1].Duration)), nil
				// 	},
				// },
				// {
				// 	ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Time},
				// 	OutputType:    octosql.Time,
				// 	Strict:        true,
				// 	Function: func(values []octosql.Value) (octosql.Value, error) {
				// 		return octosql.NewTime(values[1].Time.Add(values[0].Duration)), nil
				// 	},
				// },
				// {
				// 	ArgumentTypes: []octosql.Type{octosql.String, octosql.String},
				// 	OutputType:    octosql.String,
				// 	Strict:        true,
				// 	Function: func(values []octosql.Value) (octosql.Value, error) {
				// 		return octosql.NewString(values[0].Str + values[1].Str), nil
				// 	},
				// },
			},
		},
		// "-": {
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Int, octosql.Int},
		// 			OutputType:    octosql.Int,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewInt(values[0].Int - values[1].Int), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Int},
		// 			OutputType:    octosql.Int,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewInt(-values[0].Int), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float, octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(values[0].Float - values[1].Float), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(-values[0].Float), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Duration},
		// 			OutputType:    octosql.Duration,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewDuration(values[0].Duration - values[1].Duration), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Duration},
		// 			OutputType:    octosql.Duration,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewDuration(-values[0].Duration), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Time, octosql.Duration},
		// 			OutputType:    octosql.Time,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewTime(values[0].Time.Add(-values[1].Duration)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "*": {
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Int, octosql.Int},
		// 			OutputType:    octosql.Int,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewInt(values[0].Int * values[1].Int), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float, octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(values[0].Float * values[1].Float), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Int},
		// 			OutputType:    octosql.Duration,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewDuration(values[0].Duration * time.Duration(values[1].Int)), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Int, octosql.Duration},
		// 			OutputType:    octosql.Duration,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewDuration(values[1].Duration * time.Duration(values[0].Int)), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String, octosql.Int},
		// 			OutputType:    octosql.String,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewString(strings.Repeat(values[0].Str, values[1].Int)), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Int, octosql.String},
		// 			OutputType:    octosql.String,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewString(strings.Repeat(values[1].Str, values[0].Int)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "/": {
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Int, octosql.Int},
		// 			OutputType:    octosql.Int,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewInt(values[0].Int / values[1].Int), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float, octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(values[0].Float / values[1].Float), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Int},
		// 			OutputType:    octosql.Duration,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewDuration(values[0].Duration / time.Duration(values[1].Int)), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Duration, octosql.Duration},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(float64(values[0].Duration) / float64(values[1].Duration)), nil
		// 			},
		// 		},
		// 	},
		// },
		// // math
		// "abs": {
		// 	Description: "Returns absolute value of argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Int},
		// 			OutputType:    octosql.Int,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				if values[0].Int > 0 {
		// 					return values[0], nil
		// 				}
		// 				return octosql.NewInt(values[0].Int * -1), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(math.Abs(values[0].Float)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "sqrt": {
		// 	Description: "Returns square root of argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(math.Sqrt(values[0].Float)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "ceil": {
		// 	Description: "Returns ceiling of argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(math.Ceil(values[0].Float)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "floor": {
		// 	Description: "Returns floor of argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(math.Floor(values[0].Float)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "log2": {
		// 	Description: "Returns the logarithm base 2 of the argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(math.Log2(values[0].Float)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "log": {
		// 	Description: "Returns the natural logarithm of the argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(math.Log(values[0].Float)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "log10": {
		// 	Description: "Returns the logarithm base 10 of the argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(math.Log10(values[0].Float)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "pow": {
		// 	Description: "Returns the first argument to the power of the second.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float, octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(math.Pow(values[0].Float, values[1].Float)), nil
		// 			},
		// 		},
		// 	},
		// },
		// // logic
		// "not": {
		// 	Description: "Returns the negation of the argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Boolean},
		// 			OutputType:    octosql.Boolean,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewBoolean(!values[0].Boolean), nil
		// 			},
		// 		},
		// 	},
		// },
		// // strings
		// "like": {
		// 	Description: "Implements the LIKE operator. Returns whether the first argument matches the pattern in the seconds one. '_' can be used to match a single arbitrary character and '%' can be used to match any number (including 0) of characters.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String, octosql.String},
		// 			OutputType:    octosql.Boolean,
		// 			Strict:        true,
		// 			Function: func() func(values []octosql.Value) (octosql.Value, error) {
		// 				regexpCache, err := ristretto.NewCache(&ristretto.Config{
		// 					NumCounters: 128,     // number of keys to track frequency of (10M).
		// 					MaxCost:     1 << 26, // maximum cost of cache (64MB).
		// 					BufferItems: 64,      // number of keys per Get buffer.
		// 				})
		// 				if err != nil {
		// 					panic(fmt.Errorf("couldn't initialize regexp cache: %w", err))
		// 				}
		//
		// 				return func(values []octosql.Value) (octosql.Value, error) {
		// 					const likeEscape = '\\'
		// 					const likeAny = '_'
		// 					const likeAll = '%'
		//
		// 					needsEscaping := func(r rune) bool {
		// 						return r == '+' ||
		// 							r == '?' ||
		// 							r == '(' ||
		// 							r == ')' ||
		// 							r == '{' ||
		// 							r == '}' ||
		// 							r == '[' ||
		// 							r == ']' ||
		// 							r == '^' ||
		// 							r == '$' ||
		// 							r == '.'
		// 					}
		//
		// 					// we assume that the escape character is '\'
		// 					likePatternToRegexp := func(pattern string) (*regexp.Regexp, error) {
		// 						if out, ok := regexpCache.Get(pattern); ok {
		// 							return out.(*regexp.Regexp), nil
		// 						}
		//
		// 						var sb strings.Builder
		// 						sb.WriteRune('^') // match start
		//
		// 						escaping := false // was the character previously seen an escaping \
		//
		// 						for _, r := range pattern {
		// 							if escaping { // escaping \, _ and % is legal (we just write . or .*), otherwise an error occurs
		// 								if r != likeAny && r != likeAll && r != likeEscape {
		// 									return nil, fmt.Errorf("escaping invalid character in LIKE pattern: %v", r)
		// 								}
		//
		// 								escaping = false
		// 								sb.WriteRune(r)
		//
		// 								if r == likeEscape {
		// 									// since _ and % don't need to be escaped in regexp we just replace \_ with _
		// 									// but \ needs to be replaced in both, so we need to write an additional \
		// 									sb.WriteRune(likeEscape)
		// 								}
		// 							} else {
		// 								if r == likeEscape { // if we find an escape sequence we just handle it in the next step
		// 									escaping = true
		// 								} else if r == likeAny { // _ transforms to . (any character)
		// 									sb.WriteRune('.')
		// 								} else if r == likeAll { // % transforms to .* (any string)
		// 									sb.WriteString(".*")
		// 								} else if needsEscaping(r) { // escape characters that might break the regexp
		// 									sb.WriteRune('\\')
		// 									sb.WriteRune(r)
		// 								} else { // just write everything else
		// 									sb.WriteRune(r)
		// 								}
		// 							}
		// 						}
		//
		// 						sb.WriteRune('$') // match end
		//
		// 						if escaping {
		// 							return nil, fmt.Errorf("pattern ends with an escape character that doesn't escape anything")
		// 						}
		//
		// 						reg, err := regexp.Compile(sb.String())
		// 						if err != nil {
		// 							return nil, fmt.Errorf("couldn't compile LIKE pattern regexp expression: '%s' => '%s': %w", values[1].Str, sb.String(), err)
		// 						}
		//
		// 						regexpCache.Set(pattern, reg, 1)
		//
		// 						return reg, nil
		// 					}
		//
		// 					reg, err := likePatternToRegexp(values[1].Str)
		// 					if err != nil {
		// 						return octosql.Value{}, fmt.Errorf("couldn't transform LIKE pattern to regexp: %w", err)
		// 					}
		//
		// 					return octosql.NewBoolean(reg.MatchString(values[0].Str)), nil
		// 				}
		// 			}(),
		// 		},
		// 	},
		// },
		// "~": {
		// 	Description: "Implements the ~ operator. Returns whether the first argument matches the regex pattern in the second one.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String, octosql.String},
		// 			OutputType:    octosql.Boolean,
		// 			Strict:        true,
		// 			Function: func() func(values []octosql.Value) (octosql.Value, error) {
		// 				regexpCache, err := ristretto.NewCache(&ristretto.Config{
		// 					NumCounters: 128,     // number of keys to track frequency of (10M).
		// 					MaxCost:     1 << 26, // maximum cost of cache (64MB).
		// 					BufferItems: 64,      // number of keys per Get buffer.
		// 				})
		// 				if err != nil {
		// 					panic(fmt.Errorf("couldn't initialize regexp cache: %w", err))
		// 				}
		//
		// 				return func(values []octosql.Value) (octosql.Value, error) {
		// 					pattern := values[1].Str
		//
		// 					var reg *regexp.Regexp
		// 					if cached, ok := regexpCache.Get(pattern); ok {
		// 						reg = cached.(*regexp.Regexp)
		// 					} else {
		// 						compiled, err := regexp.Compile(pattern)
		// 						if err != nil {
		// 							return octosql.Value{}, fmt.Errorf("couldn't compile ~ pattern regexp expression: '%s': %w", pattern, err)
		// 						}
		// 						reg = compiled
		//
		// 						regexpCache.Set(pattern, compiled, 1)
		// 					}
		//
		// 					return octosql.NewBoolean(reg.MatchString(values[0].Str)), nil
		// 				}
		// 			}(),
		// 		},
		// 	},
		// },
		// "~*": {
		// 	Description: "Implements the ~* operator. Returns whether the first argument matches the regex pattern in the second one. Case insensitive.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String, octosql.String},
		// 			OutputType:    octosql.Boolean,
		// 			Strict:        true,
		// 			Function: func() func(values []octosql.Value) (octosql.Value, error) {
		// 				regexpCache, err := ristretto.NewCache(&ristretto.Config{
		// 					NumCounters: 128,     // number of keys to track frequency of (10M).
		// 					MaxCost:     1 << 26, // maximum cost of cache (64MB).
		// 					BufferItems: 64,      // number of keys per Get buffer.
		// 				})
		// 				if err != nil {
		// 					panic(fmt.Errorf("couldn't initialize regexp cache: %w", err))
		// 				}
		//
		// 				return func(values []octosql.Value) (octosql.Value, error) {
		// 					pattern := strings.ToLower(values[1].Str)
		//
		// 					var reg *regexp.Regexp
		// 					if cached, ok := regexpCache.Get(pattern); ok {
		// 						reg = cached.(*regexp.Regexp)
		// 					} else {
		// 						compiled, err := regexp.Compile(pattern)
		// 						if err != nil {
		// 							return octosql.Value{}, fmt.Errorf("couldn't compile ~ pattern regexp expression: '%s': %w", pattern, err)
		// 						}
		// 						reg = compiled
		//
		// 						regexpCache.Set(pattern, compiled, 1)
		// 					}
		//
		// 					return octosql.NewBoolean(reg.MatchString(strings.ToLower(values[0].Str))), nil
		// 				}
		// 			}(),
		// 		},
		// 	},
		// },
		// // TODO: Regexp Match with capture of first group.
		// "upper": {
		// 	Description: "Returns the argument upper cased.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String},
		// 			OutputType:    octosql.String,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewString(strings.ToUpper(values[0].Str)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "lower": {
		// 	Description: "Returns the argument lower cased.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String},
		// 			OutputType:    octosql.String,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewString(strings.ToLower(values[0].Str)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "reverse": {
		// 	Description: "Returns the argument reversed.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String},
		// 			OutputType:    octosql.String,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				out := make([]rune, len(values[0].Str))
		// 				for i, ch := range values[0].Str {
		// 					out[len(out)-i-1] = ch
		// 				}
		// 				return octosql.NewString(string(out)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "substr": {
		// 	Description: "Returns a substring of the first argument beginning at the index provided in the second argument and optionally limiting the length using the third argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String, octosql.Int},
		// 			OutputType:    octosql.String,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				if len(values[0].Str) <= values[1].Int {
		// 					return octosql.NewString(""), nil
		// 				}
		// 				return octosql.NewString(values[0].Str[values[1].Int:]), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String, octosql.Int, octosql.Int},
		// 			OutputType:    octosql.String,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				if len(values[0].Str) <= values[1].Int {
		// 					return octosql.NewString(""), nil
		// 				}
		// 				end := values[1].Int + values[2].Int
		// 				if end > len(values[0].Str) {
		// 					end = len(values[0].Str)
		// 				}
		// 				return octosql.NewString(values[0].Str[values[1].Int:end]), nil
		// 			},
		// 		},
		// 	},
		// },
		// "replace": {
		// 	Description: "Replaces all occurrences of the second argument in the first argument by the third argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String, octosql.String, octosql.String},
		// 			OutputType:    octosql.String,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewString(strings.Replace(values[0].Str, values[1].Str, values[2].Str, -1)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "position": {
		// 	Description: "Finds the first occurrence of the second argument in the first argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String, octosql.String},
		// 			OutputType:    octosql.TypeSum(octosql.Int, octosql.Null),
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				i := strings.Index(values[0].Str, values[1].Str)
		// 				if i == -1 {
		// 					return octosql.NewNull(), nil
		// 				}
		// 				return octosql.NewInt(i), nil
		// 			},
		// 		},
		// 	},
		// },
		"len": {
			Description: "Returns the length of the collection: string, list, object or tuple.",
			Descriptors: []physical.FunctionDescriptor{
				{
					ArgumentTypes: []octosql.Type{octosql.String},
					OutputType:    octosql.Int,
					Strict:        true, // TODO: Handle strictness for arrow.
					// TODO: Do we need FunctionScalar here?
					Function: func(values []arrow.Array) (arrow.Array, error) {
						stringArray := values[0].(*array.String)

						lengths := array.NewInt64Builder(memory.NewGoAllocator()) // TODO: Get allocator from context.
						lengths.Reserve(stringArray.Len())

						for i := 0; i < stringArray.Len(); i++ {
							lengths.UnsafeAppend(int64(stringArray.ValueOffset(i+1) - stringArray.ValueOffset(i)))
						}
						return lengths.NewArray(), nil
					},
				},
				// {
				// 	TypeFn: func(types []octosql.Type) (octosql.Type, bool) {
				// 		if len(types) != 1 {
				// 			return octosql.Type{}, false
				// 		}
				// 		if types[0].TypeID != octosql.TypeIDList {
				// 			return octosql.Type{}, false
				// 		}
				// 		return octosql.Int, true
				// 	},
				// 	Strict: true,
				// 	Function: func(values []octosql.Value) (octosql.Value, error) {
				// 		return octosql.NewInt(len(values[0].List)), nil
				// 	},
				// },
				// {
				// 	TypeFn: func(types []octosql.Type) (octosql.Type, bool) {
				// 		if len(types) != 1 {
				// 			return octosql.Type{}, false
				// 		}
				// 		if types[0].TypeID != octosql.TypeIDStruct {
				// 			return octosql.Type{}, false
				// 		}
				// 		return octosql.Int, true
				// 	},
				// 	Strict: true,
				// 	Function: func(values []octosql.Value) (octosql.Value, error) {
				// 		return octosql.NewInt(len(values[0].Struct)), nil
				// 	},
				// },
				// {
				// 	TypeFn: func(types []octosql.Type) (octosql.Type, bool) {
				// 		if len(types) != 1 {
				// 			return octosql.Type{}, false
				// 		}
				// 		if types[0].TypeID != octosql.TypeIDTuple {
				// 			return octosql.Type{}, false
				// 		}
				// 		return octosql.Int, true
				// 	},
				// 	Strict: true,
				// 	Function: func(values []octosql.Value) (octosql.Value, error) {
				// 		return octosql.NewInt(len(values[0].Tuple)), nil
				// 	},
				// },
			},
		},
		// // time
		// "now": {
		// 	Description: "Returns the current time.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{},
		// 			OutputType:    octosql.Time,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewTime(time.Now()), nil
		// 			},
		// 		},
		// 	},
		// },
		// "parse_time": {
		// 	Description: "Parses the time in the second argument using the pattern in the first argument. The pattern should be specified as in the Go standard library time.Parse function: https://pkg.go.dev/time#pkg-constants",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String, octosql.String},
		// 			OutputType:    octosql.TypeSum(octosql.Time, octosql.Null),
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				t, err := time.Parse(values[0].Str, values[1].Str)
		// 				if err != nil {
		// 					log.Printf("error parsing time: %s", err)
		// 					return octosql.NewNull(), nil
		// 				}
		// 				return octosql.NewTime(t), nil
		// 			},
		// 		},
		// 	},
		// },
		// "time_from_unix": {
		// 	Description: "Parses the unix timestamp as a time.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Int},
		// 			OutputType:    octosql.Time,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewTime(time.Unix(int64(values[0].Int), 0)), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float},
		// 			OutputType:    octosql.Time,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				i, f := math.Modf(values[0].Float)
		// 				return octosql.NewTime(time.Unix(int64(i), int64(float64(time.Second)*f))), nil
		// 			},
		// 		},
		// 	},
		// },
		// "time_to_unix": {
		// 	Description: "Converts time to unix timestamp.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Time},
		// 			OutputType:    octosql.Int,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewInt(int(values[0].Time.Unix())), nil
		// 			},
		// 		},
		// 	},
		// },
		// // Conversions
		// "int": {
		// 	Description: "Converts the argument to an int.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			// This case will catch any types which may be int at the start of non-exact matching.
		// 			// So the int function can be used as a type cast.
		// 			ArgumentTypes: []octosql.Type{octosql.Int},
		// 			OutputType:    octosql.Int,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return values[0], nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Boolean},
		// 			OutputType:    octosql.Int,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				if values[0].Boolean {
		// 					return octosql.NewInt(1), nil
		// 				} else {
		// 					return octosql.NewInt(0), nil
		// 				}
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Float},
		// 			OutputType:    octosql.Int,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewInt(int(values[0].Float)), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String},
		// 			OutputType:    octosql.Int,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				n, err := strconv.Atoi(values[0].Str)
		// 				if err != nil {
		// 					log.Printf("couldn't parse string '%s' as int: %s", values[0].Str, err)
		// 					return octosql.NewNull(), nil
		// 				}
		// 				return octosql.NewInt(n), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Duration},
		// 			OutputType:    octosql.Int,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewInt(int(values[0].Duration)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "float": {
		// 	Description: "Converts the argument to an float.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			// This case will catch any types which may be float at the start of non-exact matching.
		// 			// So the int function can be used as a type cast.
		// 			ArgumentTypes: []octosql.Type{octosql.Float},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return values[0], nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Int},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(float64(values[0].Int)), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.String},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				n, err := strconv.ParseFloat(values[0].Str, 64)
		// 				if err != nil {
		// 					log.Printf("couldn't parse string '%s' as float: %s", values[0].Str, err)
		// 					return octosql.NewNull(), nil
		// 				}
		// 				return octosql.NewFloat(n), nil
		// 			},
		// 		},
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Duration},
		// 			OutputType:    octosql.Float,
		// 			Strict:        true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewFloat(float64(values[0].Duration)), nil
		// 			},
		// 		},
		// 	},
		// },
		// "string": {
		// 	Description: "Converts the argument to a string.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Any},
		// 			OutputType:    octosql.String,
		// 			Strict:        false,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.NewString(values[0].String()), nil
		// 			},
		// 		},
		// 	},
		// },
		// // Array Functions
		// "[]": {
		// 	Description: "Implements the indexing operator: list[index]",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			TypeFn: func(ts []octosql.Type) (octosql.Type, bool) {
		// 				if len(ts) != 2 {
		// 					return octosql.Type{}, false
		// 				}
		// 				if ts[0].TypeID != octosql.TypeIDList {
		// 					return octosql.Type{}, false
		// 				}
		// 				if ts[1].TypeID != octosql.TypeIDInt {
		// 					return octosql.Type{}, false
		// 				}
		// 				outType := octosql.Null
		// 				if elType := ts[0].List.Element; elType != nil {
		// 					outType = octosql.TypeSum(outType, *elType)
		// 				}
		// 				return outType, true
		// 			},
		// 			Strict: true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				if values[1].Int >= len(values[0].List) {
		// 					return octosql.NewNull(), nil
		// 				}
		// 				return values[0].List[values[1].Int], nil
		// 			},
		// 		},
		// 	},
		// },
		// "in": {
		// 	Description: "",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			TypeFn: func(ts []octosql.Type) (octosql.Type, bool) {
		// 				if len(ts) != 2 {
		// 					return octosql.Type{}, false
		// 				}
		// 				if ts[1].TypeID != octosql.TypeIDList {
		// 					return octosql.Type{}, false
		// 				}
		// 				return octosql.Boolean, true
		// 			},
		// 			Strict: true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				for i := range values[1].List {
		// 					if values[0].Equal(values[1].List[i]) {
		// 						return octosql.NewBoolean(true), nil
		// 					}
		// 				}
		// 				return octosql.NewBoolean(false), nil
		// 			},
		// 		},
		// 		{
		// 			TypeFn: func(ts []octosql.Type) (octosql.Type, bool) {
		// 				if len(ts) != 2 {
		// 					return octosql.Type{}, false
		// 				}
		// 				if ts[1].TypeID != octosql.TypeIDTuple {
		// 					return octosql.Type{}, false
		// 				}
		// 				return octosql.Boolean, true
		// 			},
		// 			Strict: true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				for i := range values[1].Tuple {
		// 					if values[0].Equal(values[1].Tuple[i]) {
		// 						return octosql.NewBoolean(true), nil
		// 					}
		// 				}
		// 				return octosql.NewBoolean(false), nil
		// 			},
		// 		},
		// 	},
		// },
		// "not in": {
		// 	Description: "",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			TypeFn: func(ts []octosql.Type) (octosql.Type, bool) {
		// 				if len(ts) != 2 {
		// 					return octosql.Type{}, false
		// 				}
		// 				if ts[1].TypeID != octosql.TypeIDList {
		// 					return octosql.Type{}, false
		// 				}
		// 				return octosql.Boolean, true
		// 			},
		// 			Strict: true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				for i := range values[1].List {
		// 					if values[0].Equal(values[1].List[i]) {
		// 						return octosql.NewBoolean(false), nil
		// 					}
		// 				}
		// 				return octosql.NewBoolean(true), nil
		// 			},
		// 		},
		// 		{
		// 			TypeFn: func(ts []octosql.Type) (octosql.Type, bool) {
		// 				if len(ts) != 2 {
		// 					return octosql.Type{}, false
		// 				}
		// 				if ts[1].TypeID != octosql.TypeIDTuple {
		// 					return octosql.Type{}, false
		// 				}
		// 				return octosql.Boolean, true
		// 			},
		// 			Strict: true,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				for i := range values[1].Tuple {
		// 					if values[0].Equal(values[1].Tuple[i]) {
		// 						return octosql.NewBoolean(false), nil
		// 					}
		// 				}
		// 				return octosql.NewBoolean(true), nil
		// 			},
		// 		},
		// 	},
		// },
		// // Utility functions
		// "panic": {
		// 	Description: "Fails the execution of OctoSQL and prints the argument.",
		// 	Descriptors: []physical.FunctionDescriptor{
		// 		{
		// 			ArgumentTypes: []octosql.Type{octosql.Any},
		// 			OutputType:    octosql.Any,
		// 			Strict:        false,
		// 			Function: func(values []octosql.Value) (octosql.Value, error) {
		// 				return octosql.ZeroValue, fmt.Errorf("panic: %s", values[0].String())
		// 			},
		// 		},
		// 	},
		// },
	}
}
