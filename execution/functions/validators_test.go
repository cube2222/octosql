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
			if err := exactlyNArgs(tt.args.n, tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("exactlyNArgs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_atLeastOneArg(t *testing.T) {
	type args struct {
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
				[]octosql.Value{octosql.MakeInt(1)},
			},
			wantErr: false,
		},
		{
			name: "two args - pass",
			args: args{
				[]octosql.Value{octosql.MakeInt(1), octosql.MakeString("hello")},
			},
			wantErr: false,
		},
		{
			name: "zero args - fail",
			args: args{
				[]octosql.Value{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := atLeastOneArg(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("atLeastOneArg() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_basicType(t *testing.T) {
	type args struct {
		args []octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - pass",
			args: args{
				[]octosql.Value{octosql.MakeInt(7)},
			},
			wantErr: false,
		},
		{
			name: "float - pass",
			args: args{
				[]octosql.Value{octosql.MakeFloat(7.0)},
			},
			wantErr: false,
		},
		{
			name: "bool - pass",
			args: args{
				[]octosql.Value{octosql.MakeBool(false)},
			},
			wantErr: false,
		},
		{
			name: "string - pass",
			args: args{
				[]octosql.Value{octosql.MakeString("nice")},
			},
			wantErr: false,
		},
		{
			name: "slice - fail",
			args: args{
				[]octosql.Value{octosql.MakeTuple(octosql.Tuple{octosql.MakeInt(1), octosql.MakeInt(2), octosql.MakeInt(3)})},
			},
			wantErr: true,
		},
		{
			name: "map - fail",
			args: args{
				[]octosql.Value{octosql.MakeObject(map[string]octosql.Value{})},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := basicType(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("basicType() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_wantInt(t *testing.T) {
	type args struct {
		args []octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - pass",
			args: args{
				[]octosql.Value{octosql.MakeInt(7)},
			},
			wantErr: false,
		},
		{
			name: "float - fail",
			args: args{
				[]octosql.Value{octosql.MakeFloat(7.0)},
			},
			wantErr: true,
		},
		{
			name: "string - fail",
			args: args{
				[]octosql.Value{octosql.MakeString("aaa")},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := wantInt(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("wantInt() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_wantFloat(t *testing.T) {
	type args struct {
		args []octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - fail",
			args: args{
				[]octosql.Value{octosql.MakeInt(7)},
			},
			wantErr: true,
		},
		{
			name: "float - pass",
			args: args{
				[]octosql.Value{octosql.MakeFloat(7.0)},
			},
			wantErr: false,
		},
		{
			name: "string - fail",
			args: args{
				[]octosql.Value{octosql.MakeString("aaa")},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := wantFloat(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("wantFloat() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_wantString(t *testing.T) {
	type args struct {
		args []octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - fail",
			args: args{
				[]octosql.Value{octosql.MakeInt(7)},
			},
			wantErr: true,
		},
		{
			name: "float - fail",
			args: args{
				[]octosql.Value{octosql.MakeFloat(7.0)},
			},
			wantErr: true,
		},
		{
			name: "string - fail",
			args: args{
				[]octosql.Value{octosql.MakeString("aaa")},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := wantString(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("wantString() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_wantNumber(t *testing.T) {
	type args struct {
		args []octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - pass",
			args: args{
				[]octosql.Value{octosql.MakeInt(7)},
			},
			wantErr: false,
		},
		{
			name: "float - pass",
			args: args{
				[]octosql.Value{octosql.MakeFloat(7.0)},
			},
			wantErr: false,
		},
		{
			name: "string - fail",
			args: args{
				[]octosql.Value{octosql.MakeString("aaa")},
			},
			wantErr: true,
		},
		{
			name: "bool - fail",
			args: args{
				[]octosql.Value{octosql.MakeBool(true)},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := wantNumber(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("wantNumber() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_allInts(t *testing.T) {
	type args struct {
		args []octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "all ints - pass",
			args: args{
				[]octosql.Value{octosql.MakeInt(1), octosql.MakeInt(2), octosql.MakeInt(3), octosql.MakeInt(4)},
			},
			wantErr: false,
		},
		{
			name: "empty - pass",
			args: args{
				[]octosql.Value{},
			},
			wantErr: false,
		},
		{
			name: "mixed1 - fail",
			args: args{
				[]octosql.Value{octosql.MakeInt(1), octosql.MakeInt(2), octosql.MakeFloat(2.0), octosql.MakeInt(3), octosql.MakeInt(4)},
			},
			wantErr: true,
		},
		{
			name: "mixed2 - fail",
			args: args{
				[]octosql.Value{octosql.MakeInt(1), octosql.MakeInt(2), octosql.MakeString("2"), octosql.MakeInt(3), octosql.MakeInt(4)},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := allInts(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("allInts() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_allFloats(t *testing.T) {
	type args struct {
		args []octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "all floats - pass",
			args: args{
				[]octosql.Value{octosql.MakeFloat(1.0), octosql.MakeFloat(2.0), octosql.MakeFloat(3.0), octosql.MakeFloat(4.0)},
			},
			wantErr: false,
		},
		{
			name: "empty - pass",
			args: args{
				[]octosql.Value{},
			},
			wantErr: false,
		},
		{
			name: "mixed1 - fail",
			args: args{
				[]octosql.Value{octosql.MakeFloat(1.0), octosql.MakeInt(2), octosql.MakeFloat(2.0), octosql.MakeFloat(3.0), octosql.MakeInt(4)},
			},
			wantErr: true,
		},
		{
			name: "mixed2 - fail",
			args: args{
				[]octosql.Value{octosql.MakeFloat(1.0), octosql.MakeFloat(2.0), octosql.MakeString("2"), octosql.MakeFloat(3.0), octosql.MakeFloat(4.0)},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := allFloats(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("allFloats() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_allNumbers(t *testing.T) {
	type args struct {
		args []octosql.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "all ints - pass",
			args: args{
				[]octosql.Value{octosql.MakeInt(1), octosql.MakeInt(2), octosql.MakeInt(3), octosql.MakeInt(4)},
			},
			wantErr: false,
		},
		{
			name: "empty - pass",
			args: args{
				[]octosql.Value{},
			},
			wantErr: false,
		},
		{
			name: "mixed1 - pass",
			args: args{
				[]octosql.Value{octosql.MakeInt(1), octosql.MakeInt(2), octosql.MakeFloat(2.0), octosql.MakeInt(3), octosql.MakeInt(4)},
			},
			wantErr: false,
		},
		{
			name: "mixed2 - fail",
			args: args{
				[]octosql.Value{octosql.MakeInt(1), octosql.MakeInt(2), octosql.MakeString("2"), octosql.MakeInt(3), octosql.MakeInt(4)},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := allNumbers(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("allNumbers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
