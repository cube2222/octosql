package functions

import (
	"math"
	"reflect"
	"testing"
	"time"

	. "github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

func Test_parseInt(t *testing.T) {
	type args []Value
	tests := []struct {
		name    string
		args    args
		want    Value
		wantErr bool
	}{
		{
			name:    "FuncInt - parse float",
			args:    []Value{MakeFloat(7.0)},
			want:    MakeInt(7),
			wantErr: false,
		},
		{
			name:    "FuncInt - parse string",
			args:    []Value{MakeString("192")},
			want:    MakeInt(192),
			wantErr: false,
		},
		{
			name:    "FuncInt - parse bool",
			args:    []Value{MakeBool(true)},
			want:    MakeInt(1),
			wantErr: false,
		},
		{
			name:    "FuncInt - invalid string",
			args:    []Value{MakeString("17a")},
			want:    ZeroValue(),
			wantErr: true,
		},
		{
			name:    "FuncInt - no args",
			args:    []Value{},
			want:    ZeroValue(),
			wantErr: true,
		},
		{
			name:    "FuncInt - too many args",
			args:    []Value{MakeFloat(7.0), MakeString("1")},
			want:    ZeroValue(),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := execute(FuncInt, tt.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("FuncInt error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FuncInt = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_stringFunctions(t *testing.T) {
	type args struct {
		fun  execution.Function
		args []Value
	}
	tests := []struct {
		name    string
		args    args
		want    Value
		wantErr bool
	}{
		/* uppercase tests */
		{
			name: "uppercase - pass",
			args: args{
				fun:  FuncUpper,
				args: []Value{MakeString("aLa MA kotA i PSA")},
			},
			want:    MakeString("ALA MA KOTA I PSA"),
			wantErr: false,
		},
		{
			name: "uppercase too many args - fail",
			args: args{
				fun:  FuncUpper,
				args: []Value{MakeString("aLa MA kotA i PSA"), MakeString("a co to jest?")},
			},
			want:    ZeroValue(),
			wantErr: true,
		},
		{
			name: "uppercase int - fail",
			args: args{
				fun:  FuncUpper,
				args: []Value{MakeInt(17)},
			},
			want:    ZeroValue(),
			wantErr: true,
		},
		/* lowercase tests */
		{
			name: "lowercase - pass",
			args: args{
				fun:  FuncLower,
				args: []Value{MakeString("aLa MA kotA i PSA")},
			},
			want:    MakeString("ala ma kota i psa"),
			wantErr: false,
		},
		{
			name: "lowercase no args - fail",
			args: args{
				fun:  FuncLower,
				args: []Value{},
			},
			want:    ZeroValue(),
			wantErr: true,
		},
		{
			name: "lowercase float - fail",
			args: args{
				fun:  FuncLower,
				args: []Value{MakeFloat(17.16)},
			},
			want:    ZeroValue(),
			wantErr: true,
		},
		/* capitalize tests */
		{
			name: "capitalize - pass",
			args: args{
				fun:  FuncCapitalize,
				args: []Value{MakeString("pawełjumperLEGENDApolskiegoYT")},
			},
			want:    MakeString("Pawełjumperlegendapolskiegoyt"),
			wantErr: false,
		},
		{
			name: "capitalize - pass2",
			args: args{
				fun:  FuncCapitalize,
				args: []Value{MakeString("there Are Several Words here")},
			},
			want:    MakeString("There Are Several Words Here"),
			wantErr: false,
		},
		{
			name: "capitalize no args - fail",
			args: args{
				fun:  FuncCapitalize,
				args: []Value{},
			},
			want:    ZeroValue(),
			wantErr: true,
		},
		{
			name: "capitalize float - fail",
			args: args{
				fun:  FuncCapitalize,
				args: []Value{MakeFloat(17.16)},
			},
			want:    ZeroValue(),
			wantErr: true,
		},
		/* reverse tests */
		{
			name: "reverse - pass",
			args: args{
				fun:  FuncReverse,
				args: []Value{MakeString("ala ma kota")},
			},
			want:    MakeString("atok am ala"),
			wantErr: false,
		},
		{
			name: "reverse - pass2",
			args: args{
				fun:  FuncReverse,
				args: []Value{MakeString("aBcD123-")},
			},
			want:    MakeString("-321DcBa"),
			wantErr: false,
		},
		{
			name: "reverse no args - fail",
			args: args{
				fun:  FuncReverse,
				args: []Value{},
			},
			want:    ZeroValue(),
			wantErr: true,
		},
		{
			name: "reverse bool - fail",
			args: args{
				fun:  FuncReverse,
				args: []Value{MakeBool(true)},
			},
			want:    ZeroValue(),
			wantErr: true,
		},
		{
			name: "reverse too many args - fail",
			args: args{
				fun:  FuncReverse,
				args: []Value{MakeString("aa"), MakeString("bb")},
			},
			want:    ZeroValue(),
			wantErr: true,
		},

		/* regexp matching tests */
		{
			name: "simple find - 1",
			args: args{
				fun:  FuncRegexpFind,
				args: []Value{MakeString("t[a+b]e"), MakeString("tcetdetaetbe")},
			},
			want:    MakeString("tae"),
			wantErr: false,
		},
		{
			name: "simple find - 2",
			args: args{
				fun:  FuncRegexpFind,
				args: []Value{MakeString("a..d"), MakeString("axdaxxdaxdxa")},
			},
			want:    MakeString("axxd"),
			wantErr: false,
		},
		{
			name: "simple find - 3",
			args: args{
				fun:  FuncRegexpFind,
				args: []Value{MakeString(".[0-9]."), MakeString("this is a bit longer but 4 the matcher it's no problem")},
			},
			want:    MakeString(" 4 "), /* matches the spaces with . */
			wantErr: false,
		},
		{
			name: "simple find - 4",
			args: args{
				fun:  FuncRegexpFind,
				args: []Value{MakeString("[1-9][0-9]{3}"), MakeString("The year was 2312 and the aliens began their invasion")},
			},
			want:    MakeString("2312"),
			wantErr: false,
		},
		{
			name: "star regexp find - 1",
			args: args{
				fun:  FuncRegexpFind,
				args: []Value{MakeString("AB*A"), MakeString("My favourite band is not ABA, it's ABBA.")},
			},
			want:    MakeString("ABA"),
			wantErr: false,
		},
		{
			name: "star regexp find - 2",
			args: args{
				fun:  FuncRegexpFind,
				args: []Value{MakeString("a*ba*"), MakeString("What is a bbba?")},
			},
			want:    MakeString("b"), /* matches the shortest */
			wantErr: false,
		},
		{
			name: "complex regexp",
			args: args{
				fun:  FuncRegexpFind,
				args: []Value{MakeString(`[a + b]{2}c*d{3}`), MakeString("abcddaaacdddbacccdddabcda")},
			},
			want:    MakeString("aacddd"),
			wantErr: false,
		},
		{
			name: "regexp match true",
			args: args{
				fun:  FuncRegexpMatches,
				args: []Value{MakeString(`[a + b]{2}c*d{3}`), MakeString("abcddaaacdddbacccdddabcda")},
			},
			want:    MakeBool(true),
			wantErr: false,
		},
		{
			name: "regexp match false",
			args: args{
				fun:  FuncRegexpMatches,
				args: []Value{MakeString(`[a + b]{2}zc*d{3}`), MakeString("abcddaaacdddbacccdddabcda")},
			},
			want:    MakeBool(false),
			wantErr: false,
		},
		{
			name: "regexp match whole true",
			args: args{
				fun:  FuncRegexpMatches,
				args: []Value{MakeString(`^abcd$`), MakeString("abcd")},
			},
			want:    MakeBool(true),
			wantErr: false,
		},
		{
			name: "regexp match whole false",
			args: args{
				fun:  FuncRegexpMatches,
				args: []Value{MakeString(`^abcd$`), MakeString("abcde")},
			},
			want:    MakeBool(false),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := execute(tt.args.fun, tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_various(t *testing.T) {
	type args struct {
		fun  execution.Function
		args []Value
	}
	tests := []struct {
		name    string
		args    args
		want    Value
		wantErr bool
	}{
		/* sqrt() */
		{
			name: "sqrt(4)",
			args: args{
				args: []Value{MakeInt(4)},
				fun:  FuncSqrt,
			},
			want:    MakeFloat(2.0), /* type is important */
			wantErr: false,
		},
		{
			name: "sqrt(7)",
			args: args{
				args: []Value{MakeInt(7)},
				fun:  FuncSqrt,
			},
			want:    MakeFloat(math.Sqrt(7.0)), /* type is important */
			wantErr: false,
		},
		{
			name: "sqrt(-1)",
			args: args{
				args: []Value{MakeInt(-1)},
				fun:  FuncSqrt,
			},
			want:    ZeroValue(),
			wantErr: true,
		},

		/* log */
		{
			name: "log(8)",
			args: args{
				args: []Value{MakeInt(8)},
				fun:  FuncLog2,
			},
			want:    MakeFloat(3.0),
			wantErr: false,
		},
		{
			name: "log(15.5)",
			args: args{
				args: []Value{MakeFloat(15.5)},
				fun:  FuncLog2,
			},
			want:    MakeFloat(math.Log2(15.5)),
			wantErr: false,
		},
		{
			name: "log(0)",
			args: args{
				args: []Value{MakeInt(0)},
				fun:  FuncLog2,
			},
			want:    ZeroValue(),
			wantErr: true,
		},

		/* x^y */
		{
			name: "3^4",
			args: args{
				args: []Value{MakeFloat(3.0), MakeFloat(4.0)},
				fun:  FuncPower,
			},
			want:    MakeFloat(81.0),
			wantErr: false,
		},

		{
			name: "abs(-7)",
			args: args{
				args: []Value{MakeInt(-7)},
				fun:  FuncAbs,
			},
			want:    MakeInt(7),
			wantErr: false,
		},
		{
			name: "abs(7)",
			args: args{
				args: []Value{MakeInt(7)},
				fun:  FuncAbs,
			},
			want:    MakeInt(7),
			wantErr: false,
		},
		{
			name: "abs(-7.0)",
			args: args{
				args: []Value{MakeFloat(-7.0)},
				fun:  FuncAbs,
			},
			want:    MakeFloat(7.0),
			wantErr: false,
		},
		{
			name: "abs(7.0)",
			args: args{
				args: []Value{MakeFloat(7)},
				fun:  FuncAbs,
			},
			want:    MakeFloat(7.0),
			wantErr: false,
		},
		{
			name: "least(-1, 3, -3, 7, 2)",
			args: args{
				args: []Value{
					MakeInt(-1),
					MakeInt(3),
					MakeInt(-3),
					MakeInt(7),
					MakeInt(2),
				},
				fun: FuncLeast,
			},
			want:    MakeInt(-3),
			wantErr: false,
		},
		{
			name: "greatest(-1, 3, -3, 7, 2)",
			args: args{
				args: []Value{
					MakeInt(-1),
					MakeInt(3),
					MakeInt(-3),
					MakeInt(7),
					MakeInt(2),
				},
				fun: FuncGreatest,
			},
			want:    MakeInt(7),
			wantErr: false,
		},
		{
			name: "reverse(-1, 3, -3, 'test', 2)",
			args: args{
				args: []Value{
					MakeTuple(
						[]Value{
							MakeInt(-1),
							MakeInt(3),
							MakeInt(-3),
							MakeString("test"),
							MakeInt(2),
						},
					),
				},
				fun: FuncReverse,
			},
			want: MakeTuple(
				[]Value{
					MakeInt(2),
					MakeString("test"),
					MakeInt(-3),
					MakeInt(3),
					MakeInt(-1),
				},
			),
			wantErr: false,
		},
		{
			name: "reverse('hello hello')",
			args: args{
				args: []Value{MakeString("hello hello")},
				fun:  FuncReverse,
			},
			want:    MakeString("olleh olleh"),
			wantErr: false,
		},
		{
			name: "substring('hello hello', 3)",
			args: args{
				args: []Value{
					MakeString("hello hello"),
					MakeInt(3),
				},
				fun: FuncSubstring,
			},
			want:    MakeString("lo hello"),
			wantErr: false,
		},
		{
			name: "substring('hello hello', 3, 8)",
			args: args{
				args: []Value{
					MakeString("hello hello"),
					MakeInt(3),
					MakeInt(8),
				},
				fun: FuncSubstring,
			},
			want:    MakeString("lo he"),
			wantErr: false,
		},
		{
			name: "regexp('[l]*o', 'hello hello')",
			args: args{
				args: []Value{
					MakeString("[l]*o"),
					MakeString("hello hello"),
				},
				fun: FuncRegexpFind,
			},
			want:    MakeString("llo"),
			wantErr: false,
		},
		{
			name: "replace('llo', 'oll', 'hello hello')",
			args: args{
				args: []Value{
					MakeString("llo"),
					MakeString("oll"),
					MakeString("hello hello"),
				},
				fun: FuncReplace,
			},
			want:    MakeString("heoll heoll"),
			wantErr: false,
		},
		{
			name: "hasprefix('hel', 'hello hello')",
			args: args{
				args: []Value{
					MakeString("hel"),
					MakeString("hello hello"),
				},
				fun: FuncHasPrefix,
			},
			want:    MakeBool(true),
			wantErr: false,
		},
		{
			name: "hasprefix('hel', 'hello hello')",
			args: args{
				args: []Value{
					MakeString("hela"),
					MakeString("hello hello"),
				},
				fun: FuncHasPrefix,
			},
			want:    MakeBool(false),
			wantErr: false,
		},
		{
			name: "hassuffix('hel', 'hello hello')",
			args: args{
				args: []Value{
					MakeString("ello"),
					MakeString("hello hello"),
				},
				fun: FuncHasSuffix,
			},
			want:    MakeBool(true),
			wantErr: false,
		},
		{
			name: "hassuffix('hel', 'hello hello')",
			args: args{
				args: []Value{
					MakeString("allo"),
					MakeString("hello hello"),
				},
				fun: FuncHasSuffix,
			},
			want:    MakeBool(false),
			wantErr: false,
		},
		{
			name: "contains('o h', 'hello hello')",
			args: args{
				args: []Value{
					MakeString("o h"),
					MakeString("hello hello"),
				},
				fun: FuncContains,
			},
			want:    MakeBool(true),
			wantErr: false,
		},
		{
			name: "contains(5, (1, 3, 'test', 5, 4))",
			args: args{
				args: []Value{
					MakeInt(5),
					MakeTuple(
						[]Value{
							MakeInt(1),
							MakeInt(3),
							MakeString("test"),
							MakeInt(5),
							MakeInt(4),
						},
					),
				},
				fun: FuncContains,
			},
			want:    MakeBool(true),
			wantErr: false,
		},
		{
			name: "contains(2, (1, 3, 'test', 5, 4))",
			args: args{
				args: []Value{
					MakeInt(2),
					MakeTuple(
						[]Value{
							MakeInt(1),
							MakeInt(3),
							MakeString("test"),
							MakeInt(5),
							MakeInt(4),
						},
					),
				},
				fun: FuncContains,
			},
			want:    MakeBool(false),
			wantErr: false,
		},
		{
			name: "index('o h', 'hello hello')",
			args: args{
				args: []Value{
					MakeString("o h"),
					MakeString("hello hello"),
				},
				fun: FuncIndex,
			},
			want:    MakeInt(4),
			wantErr: false,
		},
		{
			name: "contains(5, (1, 3, 'test', 5, 4))",
			args: args{
				args: []Value{
					MakeInt(5),
					MakeTuple(
						[]Value{
							MakeInt(1),
							MakeInt(3),
							MakeString("test"),
							MakeInt(5),
							MakeInt(4),
						},
					),
				},
				fun: FuncIndex,
			},
			want:    MakeInt(3),
			wantErr: false,
		},
		{
			name: "contains(2, (1, 3, 'test', 5, 4))",
			args: args{
				args: []Value{
					MakeInt(2),
					MakeTuple(
						[]Value{
							MakeInt(1),
							MakeInt(3),
							MakeString("test"),
							MakeInt(5),
							MakeInt(4),
						},
					),
				},
				fun: FuncIndex,
			},
			want:    MakeInt(-1),
			wantErr: false,
		},
		{
			name: "nth(1, (1, 3, 'test', 5, 4))",
			args: args{
				args: []Value{
					MakeInt(1),
					MakeTuple(
						[]Value{
							MakeInt(1),
							MakeInt(3),
							MakeString("test"),
							MakeInt(5),
							MakeInt(4),
						},
					),
				},
				fun: FuncNth,
			},
			want:    MakeInt(3),
			wantErr: false,
		},
		{
			name: "nth(2, (1, 3, 'test', 5, 4))",
			args: args{
				args: []Value{
					MakeInt(2),
					MakeTuple(
						[]Value{
							MakeInt(1),
							MakeInt(3),
							MakeString("test"),
							MakeInt(5),
							MakeInt(4),
						},
					),
				},
				fun: FuncNth,
			},
			want:    MakeString("test"),
			wantErr: false,
		},
		{
			name: "length((1, 3, 'test', 5, 4))",
			args: args{
				args: []Value{
					MakeTuple(
						[]Value{
							MakeInt(1),
							MakeInt(3),
							MakeString("test"),
							MakeInt(5),
							MakeInt(4),
						},
					),
				},
				fun: FuncLength,
			},
			want:    MakeInt(5),
			wantErr: false,
		},
		{
			name: "length('hello hello')",
			args: args{
				args: []Value{MakeString("hello hello")},
				fun:  FuncLength,
			},
			want:    MakeInt(11),
			wantErr: false,
		},
		{
			name: "strjoin('hello hello')",
			args: args{
				args: []Value{
					MakeString(" - "),
					MakeTuple(
						[]Value{
							MakeString("test1"),
							MakeString("test2"),
							MakeString("test3"),
							MakeString("test4"),
						},
					),
				},
				fun: FuncStringJoin,
			},
			want:    MakeString("test1 - test2 - test3 - test4"),
			wantErr: false,
		},
		{
			name: "2s + 3s",
			args: args{
				args: []Value{
					MakeDuration(time.Second * 2),
					MakeDuration(time.Second * 3),
				},
				fun: FuncAdd,
			},
			want:    MakeDuration(time.Second * 5),
			wantErr: false,
		},
		{
			name: "date + duration",
			args: args{
				args: []Value{
					MakeTime(time.Date(2018, 11, 21, 10, 0, 0, 0, time.UTC)),
					MakeDuration(time.Hour * 3),
				},
				fun: FuncAdd,
			},
			want:    MakeTime(time.Date(2018, 11, 21, 13, 0, 0, 0, time.UTC)),
			wantErr: false,
		},
		{
			name: "2s - 3s",
			args: args{
				args: []Value{
					MakeDuration(time.Second * 2),
					MakeDuration(time.Second * 3),
				},
				fun: FuncSubtract,
			},
			want:    MakeDuration(time.Second * -1),
			wantErr: false,
		},
		{
			name: "date - duration",
			args: args{
				args: []Value{
					MakeTime(time.Date(2018, 11, 21, 10, 0, 0, 0, time.UTC)),
					MakeDuration(time.Hour * 3),
				},
				fun: FuncSubtract,
			},
			want:    MakeTime(time.Date(2018, 11, 21, 7, 0, 0, 0, time.UTC)),
			wantErr: false,
		},
		{
			name: "2s * 3s",
			args: args{
				args: []Value{
					MakeDuration(time.Second * 2),
					MakeDuration(time.Second * 3),
				},
				fun: FuncMultiply,
			},
			want:    ZeroValue(),
			wantErr: true,
		},
		{
			name: "2s * 3",
			args: args{
				args: []Value{
					MakeDuration(time.Second * 2),
					MakeInt(3),
				},
				fun: FuncMultiply,
			},
			want:    MakeDuration(time.Second * 6),
			wantErr: false,
		},
		{
			name: "4s / 2s",
			args: args{
				args: []Value{
					MakeDuration(time.Second * 4),
					MakeDuration(time.Second * 2),
				},
				fun: FuncDivide,
			},
			want:    MakeFloat(2.0),
			wantErr: false,
		},
		{
			name: "4s / 2s",
			args: args{
				args: []Value{
					MakeDuration(time.Second * 4),
					MakeInt(2),
				},
				fun: FuncDivide,
			},
			want:    MakeDuration(time.Second * 2),
			wantErr: false,
		},
		{
			name: "4s / 2.0s",
			args: args{
				args: []Value{
					MakeDuration(time.Second * 4),
					MakeFloat(2.0),
				},
				fun: FuncDivide,
			},
			want:    MakeDuration(time.Second * 2),
			wantErr: false,
		},
		{
			name: "coalesce(2, 5)",
			args: args{
				args: []Value{
					MakeInt(2),
					MakeInt(5),
				},
				fun: FuncCoalesce,
			},
			want:    MakeInt(2),
			wantErr: false,
		},
		{
			name: "coalesce(2, <null>)",
			args: args{
				args: []Value{
					MakeInt(2),
					MakeNull(),
				},
				fun: FuncCoalesce,
			},
			want:    MakeInt(2),
			wantErr: false,
		},
		{
			name: "coalesce('text', <null>)",
			args: args{
				args: []Value{
					MakeString("text"),
					MakeNull(),
				},
				fun: FuncCoalesce,
			},
			want:    MakeString("text"),
			wantErr: false,
		},
		{
			name: "coalesce(<null>, 2)",
			args: args{
				args: []Value{
					MakeNull(),
					MakeInt(2),
				},
				fun: FuncCoalesce,
			},
			want:    MakeInt(2),
			wantErr: false,
		},
		{
			name: "coalesce(<null>, 'text')",
			args: args{
				args: []Value{
					MakeNull(),
					MakeString("text"),
				},
				fun: FuncCoalesce,
			},
			want:    MakeString("text"),
			wantErr: false,
		},
		{
			name: "coalesce(<null>, <null>, 1, 2)",
			args: args{
				args: []Value{
					MakeNull(),
					MakeNull(),
					MakeInt(1),
					MakeInt(2),
				},
				fun: FuncCoalesce,
			},
			want:    MakeInt(1),
			wantErr: false,
		},
		{
			name: "coalesce(<null>, <null>, <null>)",
			args: args{
				args: []Value{
					MakeNull(),
					MakeNull(),
					MakeNull(),
				},
				fun: FuncCoalesce,
			},
			want:    MakeNull(),
			wantErr: false,
		},
		{
			name: "nullif('test', 'test')",
			args: args{
				args: []Value{
					MakeString("test"),
					MakeString("test"),
				},
				fun: FuncNullIf,
			},
			want:    MakeNull(),
			wantErr: false,
		},
		{
			name: "nullif('test', 'test2')",
			args: args{
				args: []Value{
					MakeString("test"),
					MakeString("test2"),
				},
				fun: FuncNullIf,
			},
			want:    MakeString("test2"),
			wantErr: false,
		},
		{
			name: "nullif('test', 5)",
			args: args{
				args: []Value{
					MakeString("test"),
					MakeInt(5),
				},
				fun: FuncNullIf,
			},
			want:    MakeInt(5),
			wantErr: false,
		},
		{
			name: "nullif(5, 5)",
			args: args{
				args: []Value{
					MakeInt(5),
					MakeInt(5),
				},
				fun: FuncNullIf,
			},
			want:    MakeNull(),
			wantErr: false,
		},
		{
			name: "parse_time('2006-01-02', '2019-06-13')",
			args: args{
				args: []Value{
					MakeString("2006-01-02"),
					MakeString("2019-06-13"),
				},
				fun: FuncParseTime,
			},
			want:    MakeTime(time.Date(2019, 06, 13, 0, 0, 0, 0, time.UTC)),
			wantErr: false,
		},
		{
			name: "parse_time('2006-01-02', '2019-06-13')",
			args: args{
				args: []Value{
					MakeString("2006-01-02 15:04"),
					MakeString("2019-06-13 17:21"),
				},
				fun: FuncParseTime,
			},
			want:    MakeTime(time.Date(2019, 06, 13, 17, 21, 0, 0, time.UTC)),
			wantErr: false,
		},
		{
			name: "parse_time('2006-01-02', '2019-06-13')",
			args: args{
				args: []Value{
					MakeString("2006-01-02 15:04"),
					MakeString("2019-13-06 17:21"),
				},
				fun: FuncParseTime,
			},
			want:    ZeroValue(),
			wantErr: true,
		},
		{
			name: "decode_base32('M5ZGKZLO')",
			args: args{
				args: []Value{
					MakeString("M5ZGKZLO"),
				},
				fun: FuncDecodeBase32,
			},
			want:    MakeString("green"),
			wantErr: false,
		},
		{
			name: "decode_base32('OB2XE4DMMU======')",
			args: args{
				args: []Value{
					MakeString("OB2XE4DMMU======"),
				},
				fun: FuncDecodeBase32,
			},
			want:    MakeString("purple"),
			wantErr: false,
		},
		{
			name: "decode_base32('OB2XE=DMMU======')",
			args: args{
				args: []Value{
					MakeString("OB2XE=DMMU======"),
				},
				fun: FuncDecodeBase32,
			},
			want:    ZeroValue(),
			wantErr: true,
		},
		{
			name: "decode_base32('YO74HPY=')",
			args: args{
				args: []Value{
					MakeString("YO74HPY="),
				},
				fun: FuncDecodeBase32,
			},
			want:    MakeString("ÿÿ"),
			wantErr: false,
		},
		{
			name: "decode_base32('IVHFKTJAEAQDAMBQ')",
			args: args{
				args: []Value{
					MakeString("IVHFKTJAEAQDAMBQ"),
				},
				fun: FuncDecodeBase32,
			},
			want:    MakeString("ENUM   000"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := execute(tt.args.fun, tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Func error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Func = %v, want %v", got.Show(), tt.want.Show())
			}
		})
	}
}
