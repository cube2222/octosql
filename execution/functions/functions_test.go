package functions

import (
	"math"
	"reflect"
	"testing"

	"github.com/cube2222/octosql/execution"
)

func Test_parseInt(t *testing.T) {
	type args []interface{}
	tests := []struct {
		name    string
		args    args
		want    octosql.Value
		wantErr bool
	}{
		{
			name:    "FuncInt - parse float",
			args:    []interface{}{7.0},
			want:    7,
			wantErr: false,
		},
		{
			name:    "FuncInt - parse string",
			args:    []interface{}{"192"},
			want:    192,
			wantErr: false,
		},
		{
			name:    "FuncInt - parse bool",
			args:    []interface{}{true},
			want:    1,
			wantErr: false,
		},
		{
			name:    "FuncInt - invalid string",
			args:    []interface{}{"17a"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "FuncInt - no args",
			args:    []interface{}{},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "FuncInt - too many args",
			args:    []interface{}{7.0, "1"},
			want:    nil,
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
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    octosql.Value
		wantErr bool
	}{
		/* uppercase tests */
		{
			name: "uppercase - pass",
			args: args{
				fun:  FuncUpper,
				args: []interface{}{"aLa MA kotA i PSA"},
			},
			want:    "ALA MA KOTA I PSA",
			wantErr: false,
		},
		{
			name: "uppercase too many args - fail",
			args: args{
				fun:  FuncUpper,
				args: []interface{}{"aLa MA kotA i PSA", "a co to jest?"},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "uppercase int - fail",
			args: args{
				fun:  FuncUpper,
				args: []interface{}{17},
			},
			want:    nil,
			wantErr: true,
		},
		/* lowercase tests */
		{
			name: "lowercase - pass",
			args: args{
				fun:  FuncLower,
				args: []interface{}{"aLa MA kotA i PSA"},
			},
			want:    "ala ma kota i psa",
			wantErr: false,
		},
		{
			name: "lowercase no args - fail",
			args: args{
				fun:  FuncLower,
				args: []interface{}{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "lowercase float - fail",
			args: args{
				fun:  FuncLower,
				args: []interface{}{17.16},
			},
			want:    nil,
			wantErr: true,
		},
		/* capitalize tests */
		{
			name: "capitalize - pass",
			args: args{
				fun:  FuncCapitalize,
				args: []interface{}{"pawełjumperLEGENDApolskiegoYT"},
			},
			want:    "Pawełjumperlegendapolskiegoyt",
			wantErr: false,
		},
		{
			name: "capitalize - pass2",
			args: args{
				fun:  FuncCapitalize,
				args: []interface{}{"there Are Several Words here"},
			},
			want:    "There Are Several Words Here",
			wantErr: false,
		},
		{
			name: "capitalize no args - fail",
			args: args{
				fun:  FuncCapitalize,
				args: []interface{}{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "capitalize float - fail",
			args: args{
				fun:  FuncCapitalize,
				args: []interface{}{17.16},
			},
			want:    nil,
			wantErr: true,
		},
		/* reverse tests */
		{
			name: "reverse - pass",
			args: args{
				fun:  FuncReverse,
				args: []interface{}{"ala ma kota"},
			},
			want:    "atok am ala",
			wantErr: false,
		},
		{
			name: "reverse - pass2",
			args: args{
				fun:  FuncReverse,
				args: []interface{}{"aBcD123-"},
			},
			want:    "-321DcBa",
			wantErr: false,
		},
		{
			name: "reverse no args - fail",
			args: args{
				fun:  FuncReverse,
				args: []interface{}{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "reverse bool - fail",
			args: args{
				fun:  FuncReverse,
				args: []interface{}{true},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "reverse too many args - fail",
			args: args{
				fun:  FuncReverse,
				args: []interface{}{"aa", "bb"},
			},
			want:    nil,
			wantErr: true,
		},

		/* regexp matching tests */
		{
			name: "simple match - 1",
			args: args{
				fun:  FuncRegexp,
				args: []interface{}{"t[a+b]e", "tcetdetaetbe"},
			},
			want:    "tae",
			wantErr: false,
		},
		{
			name: "simple match - 2",
			args: args{
				fun:  FuncRegexp,
				args: []interface{}{"a..d", "axdaxxdaxdxa"},
			},
			want:    "axxd",
			wantErr: false,
		},
		{
			name: "simple match - 3",
			args: args{
				fun:  FuncRegexp,
				args: []interface{}{".[0-9].", "this is a bit longer but 4 the matcher it's no problem"},
			},
			want:    " 4 ", /* matches the spaces with . */
			wantErr: false,
		},
		{
			name: "simple match - 4",
			args: args{
				fun:  FuncRegexp,
				args: []interface{}{"[1-9][0-9]{3}", "The year was 2312 and the aliens began their invasion"},
			},
			want:    "2312",
			wantErr: false,
		},
		{
			name: "star regexp match - 1",
			args: args{
				fun:  FuncRegexp,
				args: []interface{}{"AB*A", "My favourite band is not ABA, it's ABBA."},
			},
			want:    "ABA",
			wantErr: false,
		},
		{
			name: "star regexp match - 2",
			args: args{
				fun:  FuncRegexp,
				args: []interface{}{"a*ba*", "What is a bbba?"},
			},
			want:    "b", /* matches the shortest */
			wantErr: false,
		},
		{
			name: "complex regexp",
			args: args{
				fun:  FuncRegexp,
				args: []interface{}{`[a + b]{2}c*d{3}`, "abcddaaacdddbacccdddabcda"},
			},
			want:    "aacddd",
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

func Test_numerical(t *testing.T) {
	type args struct {
		fun  execution.Function
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    octosql.Value
		wantErr bool
	}{

		/* sqrt() */
		{
			name: "sqrt(4)",
			args: args{
				args: []interface{}{4},
				fun:  FuncSqrt,
			},
			want:    2.0, /* type is important */
			wantErr: false,
		},
		{
			name: "sqrt(7)",
			args: args{
				args: []interface{}{7},
				fun:  FuncSqrt,
			},
			want:    math.Sqrt(7.0), /* type is important */
			wantErr: false,
		},
		{
			name: "sqrt(-1)",
			args: args{
				args: []interface{}{-1},
				fun:  FuncSqrt,
			},
			want:    nil,
			wantErr: true,
		},

		/* log */
		{
			name: "log(8)",
			args: args{
				args: []interface{}{8},
				fun:  FuncLog,
			},
			want:    3.0,
			wantErr: false,
		},
		{
			name: "log(15.5)",
			args: args{
				args: []interface{}{15.5},
				fun:  FuncLog,
			},
			want:    math.Log2(15.5),
			wantErr: false,
		},
		{
			name: "log(0)",
			args: args{
				args: []interface{}{0},
				fun:  FuncLog,
			},
			want:    nil,
			wantErr: true,
		},

		/* x^y */
		{
			name: "3^4",
			args: args{
				args: []interface{}{3.0, 4.0},
				fun:  FuncPower,
			},
			want:    81.0,
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
				t.Errorf("Func = %v, want %v", got, tt.want)
			}
		})
	}
}
