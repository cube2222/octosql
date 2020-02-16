package execution

import (
	"context"
	"testing"
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
						NewRecordFromSliceWithNormalize([]string{"age"}, []interface{}{7}),
						NewRecordFromSliceWithNormalize([]string{"age"}, []interface{}{10}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize([]string{"age"}, []interface{}{7}),
						NewRecordFromSliceWithNormalize([]string{"age"}, []interface{}{10}),
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
						NewRecordFromSliceWithNormalize([]string{"name", "age"}, []interface{}{"Janek", 4}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize([]string{"age", "name"}, []interface{}{4, "Janek"}),
					},
				),
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "different order streams",
			args: args{
				first: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize([]string{"age"}, []interface{}{10}),
						NewRecordFromSliceWithNormalize([]string{"age"}, []interface{}{7}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize([]string{"age"}, []interface{}{7}),
						NewRecordFromSliceWithNormalize([]string{"age"}, []interface{}{10}),
					},
				),
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "very complex test",
			args: args{
				first: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize([]string{"age", "name"}, []interface{}{10, "Janek"}),
						NewRecordFromSliceWithNormalize([]string{"name", "age"}, []interface{}{"Wojtek", 7}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize([]string{"age", "name"}, []interface{}{10, "Janek"}),
						NewRecordFromSliceWithNormalize([]string{"name", "age"}, []interface{}{"Wojtek", 7}),
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
						NewRecordFromSliceWithNormalize([]string{"age", "name"}, []interface{}{10, "Janek"}),
						NewRecordFromSliceWithNormalize([]string{"name", "age"}, []interface{}{"Wojtek", 7}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize([]string{"age", "name"}, []interface{}{7, "Wojtek"}),
						NewRecordFromSliceWithNormalize([]string{"name", "age"}, []interface{}{"Janek", 12}),
					},
				),
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "mismatched column name",
			args: args{
				first: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize([]string{"age"}, []interface{}{10}),
						NewRecordFromSliceWithNormalize([]string{"age"}, []interface{}{7}),
					},
				),
				second: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize([]string{"age"}, []interface{}{7}),
						NewRecordFromSliceWithNormalize([]string{"ageButBetter"}, []interface{}{10}),
					},
				),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AreStreamsEqual(context.Background(), tt.args.first, tt.args.second)
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
