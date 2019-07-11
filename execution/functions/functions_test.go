package functions

import (
	"math"
	"reflect"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

func Test_parseInt(t *testing.T) {
	type args []octosql.Value
	tests := []struct {
		name    string
		args    args
		want    octosql.Value
		wantErr bool
	}{
		{
			name:    "FuncInt - parse float",
			args:    []octosql.Value{octosql.MakeFloat(7.0)},
			want:    octosql.MakeInt(7),
			wantErr: false,
		},
		{
			name:    "FuncInt - parse string",
			args:    []octosql.Value{octosql.MakeString("192")},
			want:    octosql.MakeInt(192),
			wantErr: false,
		},
		{
			name:    "FuncInt - parse bool",
			args:    []octosql.Value{octosql.MakeBool(true)},
			want:    octosql.MakeInt(1),
			wantErr: false,
		},
		{
			name:    "FuncInt - invalid string",
			args:    []octosql.Value{octosql.MakeString("17a")},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "FuncInt - no args",
			args:    []octosql.Value{},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "FuncInt - too many args",
			args:    []octosql.Value{octosql.MakeFloat(7.0), octosql.MakeString("1")},
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
		args []octosql.Value
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
				args: []octosql.Value{octosql.MakeString("aLa MA kotA i PSA")},
			},
			want:    octosql.MakeString("ALA MA KOTA I PSA"),
			wantErr: false,
		},
		{
			name: "uppercase too many args - fail",
			args: args{
				fun:  FuncUpper,
				args: []octosql.Value{octosql.MakeString("aLa MA kotA i PSA"), octosql.MakeString("a co to jest?")},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "uppercase int - fail",
			args: args{
				fun:  FuncUpper,
				args: []octosql.Value{octosql.MakeInt(17)},
			},
			want:    nil,
			wantErr: true,
		},
		/* lowercase tests */
		{
			name: "lowercase - pass",
			args: args{
				fun:  FuncLower,
				args: []octosql.Value{octosql.MakeString("aLa MA kotA i PSA")},
			},
			want:    octosql.MakeString("ala ma kota i psa"),
			wantErr: false,
		},
		{
			name: "lowercase no args - fail",
			args: args{
				fun:  FuncLower,
				args: []octosql.Value{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "lowercase float - fail",
			args: args{
				fun:  FuncLower,
				args: []octosql.Value{octosql.MakeFloat(17.16)},
			},
			want:    nil,
			wantErr: true,
		},
		/* capitalize tests */
		{
			name: "capitalize - pass",
			args: args{
				fun:  FuncCapitalize,
				args: []octosql.Value{octosql.MakeString("pawełjumperLEGENDApolskiegoYT")},
			},
			want:    octosql.MakeString("Pawełjumperlegendapolskiegoyt"),
			wantErr: false,
		},
		{
			name: "capitalize - pass2",
			args: args{
				fun:  FuncCapitalize,
				args: []octosql.Value{octosql.MakeString("there Are Several Words here")},
			},
			want:    octosql.MakeString("There Are Several Words Here"),
			wantErr: false,
		},
		{
			name: "capitalize no args - fail",
			args: args{
				fun:  FuncCapitalize,
				args: []octosql.Value{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "capitalize float - fail",
			args: args{
				fun:  FuncCapitalize,
				args: []octosql.Value{octosql.MakeFloat(17.16)},
			},
			want:    nil,
			wantErr: true,
		},
		/* reverse tests */
		{
			name: "reverse - pass",
			args: args{
				fun:  FuncReverse,
				args: []octosql.Value{octosql.MakeString("ala ma kota")},
			},
			want:    octosql.MakeString("atok am ala"),
			wantErr: false,
		},
		{
			name: "reverse - pass2",
			args: args{
				fun:  FuncReverse,
				args: []octosql.Value{octosql.MakeString("aBcD123-")},
			},
			want:    octosql.MakeString("-321DcBa"),
			wantErr: false,
		},
		{
			name: "reverse no args - fail",
			args: args{
				fun:  FuncReverse,
				args: []octosql.Value{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "reverse bool - fail",
			args: args{
				fun:  FuncReverse,
				args: []octosql.Value{octosql.MakeBool(true)},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "reverse too many args - fail",
			args: args{
				fun:  FuncReverse,
				args: []octosql.Value{octosql.MakeString("aa"), octosql.MakeString("bb")},
			},
			want:    nil,
			wantErr: true,
		},

		/* regexp matching tests */
		{
			name: "simple match - 1",
			args: args{
				fun:  FuncRegexp,
				args: []octosql.Value{octosql.MakeString("t[a+b]e"), octosql.MakeString("tcetdetaetbe")},
			},
			want:    octosql.MakeString("tae"),
			wantErr: false,
		},
		{
			name: "simple match - 2",
			args: args{
				fun:  FuncRegexp,
				args: []octosql.Value{octosql.MakeString("a..d"), octosql.MakeString("axdaxxdaxdxa")},
			},
			want:    octosql.MakeString("axxd"),
			wantErr: false,
		},
		{
			name: "simple match - 3",
			args: args{
				fun:  FuncRegexp,
				args: []octosql.Value{octosql.MakeString(".[0-9]."), octosql.MakeString("this is a bit longer but 4 the matcher it's no problem")},
			},
			want:    octosql.MakeString(" 4 "), /* matches the spaces with . */
			wantErr: false,
		},
		{
			name: "simple match - 4",
			args: args{
				fun:  FuncRegexp,
				args: []octosql.Value{octosql.MakeString("[1-9][0-9]{3}"), octosql.MakeString("The year was 2312 and the aliens began their invasion")},
			},
			want:    octosql.MakeString("2312"),
			wantErr: false,
		},
		{
			name: "star regexp match - 1",
			args: args{
				fun:  FuncRegexp,
				args: []octosql.Value{octosql.MakeString("AB*A"), octosql.MakeString("My favourite band is not ABA, it's ABBA.")},
			},
			want:    octosql.MakeString("ABA"),
			wantErr: false,
		},
		{
			name: "star regexp match - 2",
			args: args{
				fun:  FuncRegexp,
				args: []octosql.Value{octosql.MakeString("a*ba*"), octosql.MakeString("What is a bbba?")},
			},
			want:    octosql.MakeString("b"), /* matches the shortest */
			wantErr: false,
		},
		{
			name: "complex regexp",
			args: args{
				fun:  FuncRegexp,
				args: []octosql.Value{octosql.MakeString(`[a + b]{2}c*d{3}`), octosql.MakeString("abcddaaacdddbacccdddabcda")},
			},
			want:    octosql.MakeString("aacddd"),
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
		args []octosql.Value
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
				args: []octosql.Value{octosql.MakeInt(4)},
				fun:  FuncSqrt,
			},
			want:    octosql.MakeFloat(2.0), /* type is important */
			wantErr: false,
		},
		{
			name: "sqrt(7)",
			args: args{
				args: []octosql.Value{octosql.MakeInt(7)},
				fun:  FuncSqrt,
			},
			want:    octosql.MakeFloat(math.Sqrt(7.0)), /* type is important */
			wantErr: false,
		},
		{
			name: "sqrt(-1)",
			args: args{
				args: []octosql.Value{octosql.MakeInt(-1)},
				fun:  FuncSqrt,
			},
			want:    nil,
			wantErr: true,
		},

		/* log */
		{
			name: "log(8)",
			args: args{
				args: []octosql.Value{octosql.MakeInt(8)},
				fun:  FuncLog,
			},
			want:    octosql.MakeFloat(3.0),
			wantErr: false,
		},
		{
			name: "log(15.5)",
			args: args{
				args: []octosql.Value{octosql.MakeFloat(15.5)},
				fun:  FuncLog,
			},
			want:    octosql.MakeFloat(math.Log2(15.5)),
			wantErr: false,
		},
		{
			name: "log(0)",
			args: args{
				args: []octosql.Value{octosql.MakeInt(0)},
				fun:  FuncLog,
			},
			want:    nil,
			wantErr: true,
		},

		/* x^y */
		{
			name: "3^4",
			args: args{
				args: []octosql.Value{octosql.MakeFloat(3.0), octosql.MakeFloat(4.0)},
				fun:  FuncPower,
			},
			want:    octosql.MakeFloat(81.0),
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
