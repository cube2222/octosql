package functions

import (
	"testing"

	"github.com/cube2222/octosql"
)

func Test_exactlyNArgs(t *testing.T) {
	type args struct {
		n    int
		args []octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "matching number",
			args: args{
				n:    2,
				args: []octosql.Value{octosql.MakeInt(7), octosql.MakeString("a")},
			},
			wantErr: false,
		},
		{
			name: "non-matching number - too long",
			args: args{
				n:    2,
				args: []octosql.Value{octosql.MakeInt(7), octosql.MakeString("a"), octosql.MakeBool(true)},
			},
			wantErr: true,
		},
		{
			name: "non-matching number - too short",
			args: args{
				n:    4,
				args: []octosql.Value{octosql.MakeInt(7), octosql.MakeString("a"), octosql.MakeBool(true)},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := exactlyNArgs(tt.args.n)(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("exactlyNArgs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_atLeastNArgs(t *testing.T) {
	type args struct {
		n    int
		args []octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "one arg - pass",
			args: args{
				1,
				[]octosql.Value{octosql.MakeInt(1)},
			},
			wantErr: false,
		},
		{
			name: "two args - pass",
			args: args{
				1,
				[]octosql.Value{octosql.MakeInt(1), octosql.MakeString("hello")},
			},
			wantErr: false,
		},
		{
			name: "zero args - fail",
			args: args{
				1,
				[]octosql.Value{},
			},
			wantErr: true,
		},
		{
			name: "one arg - fail",
			args: args{
				2,
				[]octosql.Value{octosql.MakeInt(1)},
			},
			wantErr: true,
		},
		{
			name: "two args - pass",
			args: args{
				2,
				[]octosql.Value{octosql.MakeInt(1), octosql.MakeString("hello")},
			},
			wantErr: false,
		},
		{
			name: "zero args - fail",
			args: args{
				2,
				[]octosql.Value{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := atLeastNArgs(tt.args.n)(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("atLeastOneArg() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_atMostNArgs(t *testing.T) {
	type args struct {
		n    int
		args []octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "one arg - pass",
			args: args{
				1,
				[]octosql.Value{octosql.MakeInt(1)},
			},
			wantErr: false,
		},
		{
			name: "two args - fail",
			args: args{
				1,
				[]octosql.Value{octosql.MakeInt(1), octosql.MakeString("hello")},
			},
			wantErr: true,
		},
		{
			name: "zero args - pass",
			args: args{
				1,
				[]octosql.Value{},
			},
			wantErr: false,
		},
		{
			name: "one arg - pass",
			args: args{
				2,
				[]octosql.Value{octosql.MakeInt(1)},
			},
			wantErr: false,
		},
		{
			name: "two args - pass",
			args: args{
				2,
				[]octosql.Value{octosql.MakeInt(1), octosql.MakeString("hello")},
			},
			wantErr: false,
		},
		{
			name: "zero args - pass",
			args: args{
				2,
				[]octosql.Value{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := atMostNArgs(tt.args.n)(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("atMostOneArg() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_wantedType(t *testing.T) {
	type args struct {
		wantedType octosql.Value
		arg        octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - int - pass",
			args: args{
				octosql.ZeroInt(),
				octosql.MakeInt(7),
			},
			wantErr: false,
		},
		{
			name: "int - float - fail",
			args: args{
				octosql.ZeroInt(),
				octosql.MakeFloat(7.0),
			},
			wantErr: true,
		},
		{
			name: "int - string - fail",
			args: args{
				octosql.ZeroInt(),
				octosql.MakeString("aaa"),
			},
			wantErr: true,
		},
		{
			name: "float - float - pass",
			args: args{
				octosql.ZeroFloat(),
				octosql.MakeFloat(7.0),
			},
			wantErr: false,
		},
		{
			name: "float - float - pass",
			args: args{
				octosql.ZeroFloat(),
				octosql.MakeFloat(7.0),
			},
			wantErr: false,
		},
		{
			name: "float - string - fail",
			args: args{
				octosql.ZeroFloat(),
				octosql.MakeString("aaa"),
			},
			wantErr: true,
		},
		{
			name: "bool - bool - pass",
			args: args{
				octosql.ZeroBool(),
				octosql.MakeBool(false),
			},
			wantErr: false,
		},
		{
			name: "string - string - pass",
			args: args{
				octosql.ZeroString(),
				octosql.MakeString("nice"),
			},
			wantErr: false,
		},
		{
			name: "string - int - fail",
			args: args{
				octosql.ZeroString(),
				octosql.MakeInt(7),
			},
			wantErr: true,
		},
		{
			name: "string - float - fail",
			args: args{
				octosql.ZeroString(),
				octosql.MakeFloat(7.0),
			},
			wantErr: true,
		},
		{
			name: "string - string - pass",
			args: args{
				octosql.ZeroString(),
				octosql.MakeString("aaa"),
			},
			wantErr: false,
		},
		{
			name: "tuple - tuple - pass",
			args: args{
				octosql.ZeroTuple(),
				octosql.MakeTuple(octosql.Tuple{octosql.MakeInt(1), octosql.MakeInt(2), octosql.MakeInt(3)}),
			},
			wantErr: false,
		},
		{
			name: "tuple - int - fail",
			args: args{
				octosql.ZeroTuple(),
				octosql.MakeInt(4),
			},
			wantErr: true,
		},
		{
			name: "object - object - pass",
			args: args{
				octosql.ZeroObject(),
				octosql.MakeObject(map[string]octosql.Value{}),
			},
			wantErr: false,
		},
		{
			name: "object - int - fail",
			args: args{
				octosql.ZeroObject(),
				octosql.MakeInt(4),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := typeOf(tt.args.wantedType)(tt.args.arg); (err != nil) != tt.wantErr {
				t.Errorf("basicType() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
