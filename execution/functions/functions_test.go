package functions

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql/execution"
)

func Test_parseInt(t *testing.T) {
	type args []interface{}
	tests := []struct {
		name    string
		args    args
		want    interface{}
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
		want    interface{}
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
