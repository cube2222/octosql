package execution

import (
	"testing"

	"github.com/cube2222/octosql"
)

func TestAreStreamsEqual(t *testing.T) {
	type args struct {
		first  RecordStream
		second RecordStream
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "empty streams",
			args: args{
				first:  NewInMemoryStream([]*Record{}),
				second: NewInMemoryStream([]*Record{}),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "identical streams",
			args: args{
				first: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"age"}, []interface{}{7}),
						NewRecordFromSlice([]octosql.VariableName{"age"}, []interface{}{10}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"age"}, []interface{}{7}),
						NewRecordFromSlice([]octosql.VariableName{"age"}, []interface{}{10}),
					},
				),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "indentical streams with different column order",
			args: args{
				first: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"name", "age"}, []interface{}{"Janek", 4}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"age", "name"}, []interface{}{4, "Janek"}),
					},
				),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "different order streams",
			args: args{
				first: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"age"}, []interface{}{10}),
						NewRecordFromSlice([]octosql.VariableName{"age"}, []interface{}{7}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"age"}, []interface{}{7}),
						NewRecordFromSlice([]octosql.VariableName{"age"}, []interface{}{10}),
					},
				),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "very complex test",
			args: args{
				first: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"age", "name"}, []interface{}{10, "Janek"}),
						NewRecordFromSlice([]octosql.VariableName{"name", "age"}, []interface{}{"Wojtek", 7}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"age", "name"}, []interface{}{7, "Wojtek"}),
						NewRecordFromSlice([]octosql.VariableName{"name", "age"}, []interface{}{"Janek", 10}),
					},
				),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "not equal streams - mismatched record",
			args: args{
				first: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"age", "name"}, []interface{}{10, "Janek"}),
						NewRecordFromSlice([]octosql.VariableName{"name", "age"}, []interface{}{"Wojtek", 7}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"age", "name"}, []interface{}{7, "Wojtek"}),
						NewRecordFromSlice([]octosql.VariableName{"name", "age"}, []interface{}{"Janek", 12}),
					},
				),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "mismatched column name",
			args: args{
				first: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"age"}, []interface{}{10}),
						NewRecordFromSlice([]octosql.VariableName{"age"}, []interface{}{7}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice([]octosql.VariableName{"age"}, []interface{}{7}),
						NewRecordFromSlice([]octosql.VariableName{"ageButBetter"}, []interface{}{10}),
					},
				),
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AreStreamsEqual(tt.args.first, tt.args.second)
			if (err != nil) != tt.wantErr {
				t.Errorf("AreStreamsEqual() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AreStreamsEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}
