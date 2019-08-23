package excel

import (
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

func Test_isCellNameValid(t *testing.T) {
	type args struct {
		cell string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "correct cell name - single column, single row",
			args: args{
				cell: "B6",
			},
			want: true,
		},

		{
			name: "correct cell name - multi column, single row",
			args: args{
				cell: "AZBE6",
			},
			want: true,
		},

		{
			name: "correct cell name - single column, multi row",
			args: args{
				cell: "A9283",
			},
			want: true,
		},

		{
			name: "correct cell name - multi column, multi row",
			args: args{
				cell: "ZZXY9283",
			},
			want: true,
		},

		{
			name: "incorrect cell name - no column",
			args: args{
				cell: "1234",
			},
			want: false,
		},

		{
			name: "incorrect cell name - no row",
			args: args{
				cell: "ABCDE",
			},
			want: false,
		},

		{
			name: "incorrect cell name - mixed order",
			args: args{
				cell: "1A",
			},
			want: false,
		},

		{
			name: "incorrect cell name - mixed order 2",
			args: args{
				cell: "A1A",
			},
			want: false,
		},

		{
			name: "incorrect cell name - lowercase letter",
			args: args{
				cell: "c2",
			},
			want: false,
		},

		{
			name: "incorrect cell name - illegal character",
			args: args{
				cell: "AB?321",
			},
			want: false,
		},

		{
			name: "incorrect cell name - leading zeros",
			args: args{
				cell: "ABCD00123",
			},
			want: false,
		},

		{
			name: "incorrect cell name - zero row",
			args: args{
				cell: "ABCD0",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isCellNameValid(tt.args.cell); got != tt.want {
				t.Errorf("isCellNameValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getCellRowCol(t *testing.T) {
	type args struct {
		cell string
	}
	tests := []struct {
		name    string
		args    args
		wantRow string
		wantCol string
	}{
		{
			name: "single column, single row",
			args: args{
				cell: "A1",
			},
			wantCol: "A",
			wantRow: "1",
		},

		{
			name: "multi column, single row",
			args: args{
				cell: "ABZZ1",
			},
			wantCol: "ABZZ",
			wantRow: "1",
		},

		{
			name: "single column, multi row",
			args: args{
				cell: "A1234",
			},
			wantCol: "A",
			wantRow: "1234",
		},

		{
			name: "multi column, multi row",
			args: args{
				cell: "ABZ1234",
			},
			wantCol: "ABZ",
			wantRow: "1234",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRow, gotCol := getRowAndColumnFromCell(tt.args.cell)
			if gotRow != tt.wantRow {
				t.Errorf("getRowAndColumnFromCell() gotRow = %v, want %v", gotRow, tt.wantRow)
			}
			if gotCol != tt.wantCol {
				t.Errorf("getRowAndColumnFromCell() gotCol = %v, want %v", gotCol, tt.wantCol)
			}
		})
	}
}

func Test_getRowColCoords(t *testing.T) {
	type args struct {
		cell string
	}
	tests := []struct {
		name    string
		args    args
		wantRow int
		wantCol int
		wantErr bool
	}{
		{
			name: "single column single row",
			args: args{
				cell: "C4",
			},
			wantRow: 3,
			wantCol: 2,
			wantErr: false,
		},

		{
			name: "multi column single row",
			args: args{
				cell: "AA9",
			},
			wantRow: 8,
			wantCol: 26,
			wantErr: false,
		},

		{
			name: "single column multi row",
			args: args{
				cell: "A123",
			},
			wantRow: 122,
			wantCol: 0,
			wantErr: false,
		},

		{
			name: "multi column multi row",
			args: args{
				cell: "AD982",
			},
			wantRow: 981,
			wantCol: 29,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRow, gotCol, err := getCoordinatesFromCell(tt.args.cell)
			if (err != nil) != tt.wantErr {
				t.Errorf("getCoordinatesFromCell() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotRow != tt.wantRow {
				t.Errorf("getCoordinatesFromCell() gotRow = %v, want %v", gotRow, tt.wantRow)
			}
			if gotCol != tt.wantCol {
				t.Errorf("getCoordinatesFromCell() gotCol = %v, want %v", gotCol, tt.wantCol)
			}
		})
	}
}

func TestDataSource_Get(t *testing.T) {
	type fields struct {
		path             string
		alias            string
		hasColumnNames   bool
		sheet            string
		rootColumn       int
		rootRow          int
		rootColumnString string
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    execution.RecordStream
		wantErr bool
	}{
		{
			name: "simple default config test",
			fields: fields{
				path:             "fixtures/test.xlsx",
				alias:            "t",
				hasColumnNames:   true,
				sheet:            "Sheet1",
				rootColumn:       0,
				rootRow:          0,
				rootColumnString: "A",
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.surname", "t.age"},
					[]interface{}{"Jan", "Chomiak", 20},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.surname", "t.age"},
					[]interface{}{"Kuba", "Martin", 21},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.surname", "t.age"},
					[]interface{}{"Wojtek", "Kuźmiński", 21},
				),
			}),
			wantErr: false,
		},

		{
			name: "simple modified config test",
			fields: fields{
				path:             "fixtures/test.xlsx",
				alias:            "t",
				hasColumnNames:   false,
				sheet:            "CustomSheet",
				rootColumn:       1,
				rootRow:          2,
				rootColumnString: "B",
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{"Warsaw", 1700000},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{"Atlanta", 2000},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{"New York", 2},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{"Miami", -5},
				),
			}),
			wantErr: false,
		},

		{
			name: "table with preceeding data",
			fields: fields{
				path:             "fixtures/test.xlsx",
				alias:            "t",
				hasColumnNames:   true,
				sheet:            "CustomSheet",
				rootColumn:       4,
				rootRow:          1,
				rootColumnString: "E",
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.id", "t.points"},
					[]interface{}{1, 10},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.id", "t.points"},
					[]interface{}{2, 4},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.id", "t.points"},
					[]interface{}{3, 19},
				),
			}),
			wantErr: false,
		},

		{
			name: "table with nil inside",
			fields: fields{
				path:             "fixtures/test.xlsx",
				alias:            "t",
				hasColumnNames:   true,
				sheet:            "CustomSheet",
				rootColumn:       0,
				rootRow:          8,
				rootColumnString: "A",
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.age", "t.id"},
					[]interface{}{"Bob", 13, 1},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.age", "t.id"},
					[]interface{}{"Ally", nil, 2},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.age", "t.id"},
					[]interface{}{nil, 7, nil},
				),
			}),
			wantErr: false,
		},

		{
			name: "dates with no header row",
			fields: fields{
				path:             "fixtures/test.xlsx",
				alias:            "t",
				hasColumnNames:   false,
				sheet:            "DateSheet",
				rootColumn:       0,
				rootRow:          1,
				rootColumnString: "A",
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{time.Date(2017, 3, 14, 13, 0, 0, 0, time.UTC), 1},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{time.Date(2017, 3, 15, 13, 0, 0, 0, time.UTC), 2},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{time.Date(2019, 5, 19, 14, 0, 0, 0, time.UTC), 3},
				),
			}),
			wantErr: false,
		},

		{
			name: "dates with header row",
			fields: fields{
				path:             "fixtures/test.xlsx",
				alias:            "t",
				hasColumnNames:   true,
				sheet:            "DateSheet",
				rootColumn:       3,
				rootRow:          2,
				rootColumnString: "D",
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.date", "t.points"},
					[]interface{}{time.Date(2017, 3, 14, 13, 0, 0, 0, time.UTC), 101},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.date", "t.points"},
					[]interface{}{time.Date(2017, 3, 15, 13, 0, 0, 0, time.UTC), 102},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.date", "t.points"},
					[]interface{}{time.Date(2019, 5, 19, 14, 0, 0, 0, time.UTC), 103},
				),
			}),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &DataSource{
				path:             tt.fields.path,
				alias:            tt.fields.alias,
				hasColumnNames:   tt.fields.hasColumnNames,
				sheet:            tt.fields.sheet,
				rootColumn:       tt.fields.rootColumn,
				rootRow:          tt.fields.rootRow,
				rootColumnString: tt.fields.rootColumnString,
			}
			got, err := ds.Get(tt.args.variables)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataSource.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			areEqual, err := execution.AreStreamsEqual(got, tt.want)
			if err != nil {
				t.Errorf("Error in areStreamsEqual %v", err)
				return
			}

			if !areEqual {
				t.Errorf("DataSource.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}
