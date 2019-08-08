package excel

import (
	"reflect"
	"testing"
)

func Test_isAllZs(t *testing.T) {
	type args struct {
		columnName string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "single character - D",
			args: args{
				"D",
			},
			want: false,
		},

		{
			name: "single character - Y",
			args: args{
				"Y",
			},
			want: false,
		},

		{
			name: "single character - Z",
			args: args{
				"Z",
			},
			want: true,
		},

		{
			name: "multiple character - ZZAZZ",
			args: args{
				"ZZAZZ",
			},
			want: false,
		},

		{
			name: "multiple character - YZZZZZ",
			args: args{
				"YZZZZZ",
			},
			want: false,
		},

		{
			name: "multiple character - ZZZZ",
			args: args{
				"ZZZZ",
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAllZs(tt.args.columnName); got != tt.want {
				t.Errorf("isAllZs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCell_getCellName(t *testing.T) {
	type fields struct {
		column string
		row    int
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "single column, single-digit row",
			fields: fields{
				column: "A",
				row:    6,
			},
			want: "A6",
		},

		{
			name: "single column, multi-digit row",
			fields: fields{
				column: "A",
				row:    1923,
			},
			want: "A1923",
		},

		{
			name: "multi column, single-digit row",
			fields: fields{
				column: "AYZYB",
				row:    7,
			},
			want: "AYZYB7",
		},

		{
			name: "multi column, multi-digit row",
			fields: fields{
				column: "KKOPE",
				row:    9219,
			},
			want: "KKOPE9219",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cell := cell{
				column: tt.fields.column,
				row:    tt.fields.row,
			}
			if got := cell.getCellName(); got != tt.want {
				t.Errorf("cell.getCellName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCell_getCellToTheRight(t *testing.T) {
	type fields struct {
		column string
		row    int
	}
	tests := []struct {
		name   string
		fields fields
		want   cell
	}{
		{
			name: "simple single digit column with no curry",
			fields: fields{
				column: "E",
				row:    16,
			},
			want: newCell("F", 16),
		},

		{
			name: "single Z column",
			fields: fields{
				column: "Z",
				row:    44,
			},
			want: newCell("AA", 44),
		},

		{
			name: "multi-character column with no curry",
			fields: fields{
				column: "ABADAE",
				row:    69,
			},
			want: newCell("ABADAF", 69),
		},

		{
			name: "multi-character all Z's",
			fields: fields{
				column: "ZZZZ",
				row:    42,
			},
			want: newCell("AAAAA", 42),
		},

		{
			name: "multi-character curry 1",
			fields: fields{
				column: "ZKZZ",
				row:    420,
			},
			want: newCell("ZLAA", 420),
		},

		{
			name: "multi-character curry 2",
			fields: fields{
				column: "ABCZZZZZ",
				row:    420,
			},
			want: newCell("ABDAAAAA", 420),
		},

		{
			name: "multi-character curry 3",
			fields: fields{
				column: "AZZZ",
				row:    420,
			},
			want: newCell("BAAA", 420),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cell := cell{
				column: tt.fields.column,
				row:    tt.fields.row,
			}
			if got := cell.getCellToTheRight(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("cell.getCellToTheRight() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cell_getCellBelow(t *testing.T) {
	type fields struct {
		column string
		row    int
	}
	tests := []struct {
		name   string
		fields fields
		want   cell
	}{
		{
			name: "simple test 1",
			fields: fields{
				column: "AA",
				row:    1,
			},
			want: newCell("AA", 2),
		},

		{
			name: "simple test 2",
			fields: fields{
				column: "AA",
				row:    9,
			},
			want: newCell("AA", 10),
		},

		{
			name: "simple test 1",
			fields: fields{
				column: "AA",
				row:    199,
			},
			want: newCell("AA", 200),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cell := cell{
				column: tt.fields.column,
				row:    tt.fields.row,
			}
			if got := cell.getCellBelow(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("cell.getCellBelow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getCellFromName(t *testing.T) {
	type args struct {
		cell string
	}
	tests := []struct {
		name    string
		args    args
		want    cell
		wantErr bool
	}{
		{
			name: "correct cell - single column, single-digit row",
			args: args{
				cell: "A3",
			},
			want:    newCell("A", 3),
			wantErr: false,
		},

		{
			name: "correct cell - multi column, single-digit row",
			args: args{
				cell: "ABCD3",
			},
			want:    newCell("ABCD", 3),
			wantErr: false,
		},

		{
			name: "correct cell - single column, multi-digit row",
			args: args{
				cell: "Z1929",
			},
			want:    newCell("Z", 1929),
			wantErr: false,
		},

		{
			name: "correct cell - multi column, multi-digit row",
			args: args{
				cell: "BABAJAGA929291",
			},
			want:    newCell("BABAJAGA", 929291),
			wantErr: false,
		},

		{
			name: "mixed order 1",
			args: args{
				cell: "A1A",
			},
			want:    ErroneousCell,
			wantErr: true,
		},

		{
			name: "mixed order 2",
			args: args{
				cell: "123ABC",
			},
			want:    ErroneousCell,
			wantErr: true,
		},

		{
			name: "only digits",
			args: args{
				cell: "123",
			},
			want:    ErroneousCell,
			wantErr: true,
		},

		{
			name: "only chars",
			args: args{
				cell: "ABCD",
			},
			want:    ErroneousCell,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getCellFromName(tt.args.cell)
			if (err != nil) != tt.wantErr {
				t.Errorf("getCellFromName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getCellFromName() = %v, want %v", got, tt.want)
			}
		})
	}
}
