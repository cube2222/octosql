package functions

import (
	"testing"
)

func Test_exactlyNArgs(t *testing.T) {
	type args struct {
		n    int
		args []interface{}
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
				args: []interface{}{7, "a"},
			},
			wantErr: false,
		},
		{
			name: "non-matching number - too long",
			args: args{
				n:    2,
				args: []interface{}{7, "a", true},
			},
			wantErr: true,
		},
		{
			name: "non-matching number - too short",
			args: args{
				n:    4,
				args: []interface{}{7, "a", true},
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
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "one arg - pass",
			args: args{
				[]interface{}{1},
			},
			wantErr: false,
		},
		{
			name: "two args - pass",
			args: args{
				[]interface{}{1, "hello"},
			},
			wantErr: false,
		},
		{
			name: "zero args - fail",
			args: args{
				[]interface{}{},
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
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - pass",
			args: args{
				[]interface{}{7},
			},
			wantErr: false,
		},
		{
			name: "float - pass",
			args: args{
				[]interface{}{7.0},
			},
			wantErr: false,
		},
		{
			name: "bool - pass",
			args: args{
				[]interface{}{false},
			},
			wantErr: false,
		},
		{
			name: "string - pass",
			args: args{
				[]interface{}{"nice"},
			},
			wantErr: false,
		},
		{
			name: "slice - fail",
			args: args{
				[]interface{}{[]interface{}{1, 2, 3}},
			},
			wantErr: true,
		},
		{
			name: "map - fail",
			args: args{
				[]interface{}{map[string]string{}},
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
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - pass",
			args: args{
				[]interface{}{7},
			},
			wantErr: false,
		},
		{
			name: "float - fail",
			args: args{
				[]interface{}{7.0},
			},
			wantErr: true,
		},
		{
			name: "string - fail",
			args: args{
				[]interface{}{"aaa"},
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
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - fail",
			args: args{
				[]interface{}{7},
			},
			wantErr: true,
		},
		{
			name: "float - pass",
			args: args{
				[]interface{}{7.0},
			},
			wantErr: false,
		},
		{
			name: "string - fail",
			args: args{
				[]interface{}{"aaa"},
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
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - fail",
			args: args{
				[]interface{}{7},
			},
			wantErr: true,
		},
		{
			name: "float - fail",
			args: args{
				[]interface{}{7.0},
			},
			wantErr: true,
		},
		{
			name: "string - fail",
			args: args{
				[]interface{}{"aaa"},
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
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - pass",
			args: args{
				[]interface{}{7},
			},
			wantErr: false,
		},
		{
			name: "float - pass",
			args: args{
				[]interface{}{7.0},
			},
			wantErr: false,
		},
		{
			name: "string - fail",
			args: args{
				[]interface{}{"aaa"},
			},
			wantErr: true,
		},
		{
			name: "bool - fail",
			args: args{
				[]interface{}{true},
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
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "all ints - pass",
			args: args{
				[]interface{}{1, 2, 3, 4},
			},
			wantErr: false,
		},
		{
			name: "empty - pass",
			args: args{
				[]interface{}{},
			},
			wantErr: false,
		},
		{
			name: "mixed1 - fail",
			args: args{
				[]interface{}{1, 2, 2.0, 3, 4},
			},
			wantErr: true,
		},
		{
			name: "mixed2 - fail",
			args: args{
				[]interface{}{1, 2, "2", 3, 4},
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
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "all floats - pass",
			args: args{
				[]interface{}{1.0, 2.0, 3.0, 4.0},
			},
			wantErr: false,
		},
		{
			name: "empty - pass",
			args: args{
				[]interface{}{},
			},
			wantErr: false,
		},
		{
			name: "mixed1 - fail",
			args: args{
				[]interface{}{1.0, 2, 2.0, 3.0, 4},
			},
			wantErr: true,
		},
		{
			name: "mixed2 - fail",
			args: args{
				[]interface{}{1.0, 2.0, "2", 3.0, 4.0},
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
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "all ints - pass",
			args: args{
				[]interface{}{1, 2, 3, 4},
			},
			wantErr: false,
		},
		{
			name: "empty - pass",
			args: args{
				[]interface{}{},
			},
			wantErr: false,
		},
		{
			name: "mixed1 - pass",
			args: args{
				[]interface{}{1, 2, 2.0, 3, 4},
			},
			wantErr: false,
		},
		{
			name: "mixed2 - fail",
			args: args{
				[]interface{}{1, 2, "2", 3, 4},
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
