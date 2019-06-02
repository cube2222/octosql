package functions

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql/execution"
)

func Test_execute(t *testing.T) {
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
		{
			name: "FuncInt - parse float",
			args: args{
				fun:  FuncInt,
				args: []interface{}{7.0},
			},
			want:    7,
			wantErr: false,
		},
		{
			name: "FuncInt - parse string",
			args: args{
				fun:  FuncInt,
				args: []interface{}{"192"},
			},
			want:    192,
			wantErr: false,
		},
		{
			name: "FuncInt - parse bool",
			args: args{
				fun:  FuncInt,
				args: []interface{}{true},
			},
			want:    1,
			wantErr: false,
		},
		{
			name: "FuncInt - invalid string",
			args: args{
				fun:  FuncInt,
				args: []interface{}{"17a"},
			},
			want:    nil,
			wantErr: true,
		},

		{
			name: "FuncNegate - negate int",
			args: args{
				fun:  FuncNegate,
				args: []interface{}{-1},
			},
			want:    1,
			wantErr: false,
		},
		{
			name: "FuncNegate - negate float",
			args: args{
				fun:  FuncNegate,
				args: []interface{}{3.5},
			},
			want:    -3.5,
			wantErr: false,
		},
		{
			name: "FuncNegate - negate bool",
			args: args{
				fun:  FuncNegate,
				args: []interface{}{true},
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
