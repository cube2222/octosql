package excel

import (
	"testing"
)

func Test_getNextColumn(t *testing.T) {
	type args struct {
		cell string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "simple one character long name no curry",
			args: args{
				cell: "E",
			},
			want: "F",
		},

		{
			name: "Z -> AA curry",
			args: args{
				cell: "Z",
			},
			want: "AA",
		},

		{
			name: "some curry, but not all",
			args: args{
				cell: "ADZZ",
			},
			want: "AEAA",
		},

		{
			name: "some curry, but the last",
			args: args{
				cell: "FZZZ",
			},
			want: "GAAA",
		},

		{
			name: "many Z's",
			args: args{
				cell: "ZZZZ",
			},
			want: "AAAAA",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getNextColumn(tt.args.cell); got != tt.want {
				t.Errorf("getNextColumn() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_createCellName(t *testing.T) {
	type args struct {
		colName string
		row     int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "single column, single row",
			args: args{
				colName: "F",
				row:     7,
			},
			want: "F7",
		},

		{
			name: "multi column, single row",
			args: args{
				colName: "FAZZ",
				row:     7,
			},
			want: "FAZZ7",
		},

		{
			name: "single column, multi row",
			args: args{
				colName: "F",
				row:     7213,
			},
			want: "F7213",
		},

		{
			name: "multi column, multi row",
			args: args{
				colName: "FXYY",
				row:     7213,
			},
			want: "FXYY7213",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createCellName(tt.args.colName, tt.args.row); got != tt.want {
				t.Errorf("createCellName() = %v, want %v", got, tt.want)
			}
		})
	}
}
