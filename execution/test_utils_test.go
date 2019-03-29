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
			name: "single column, equal streams",
			args: args{
				NewInMemoryStream(
					[]octosql.VariableName{"id"},
					[]map[octosql.VariableName]interface{}{
						{
							"id": 1,
						},
						{
							"id": 2,
						},
						{
							"id": 3,
						},
					},
				),

				NewInMemoryStream(
					[]octosql.VariableName{"id"},
					[]map[octosql.VariableName]interface{}{
						{
							"id": 1,
						},
						{
							"id": 2,
						},
						{
							"id": 3,
						},
					},
				),
			},

			want:    true,
			wantErr: false,
		},

		{
			name: "two columns, different column order, equal streams",
			args: args{
				NewInMemoryStream(
					[]octosql.VariableName{"id", "age"},
					[]map[octosql.VariableName]interface{}{
						{
							"id":  1,
							"age": 7,
						},
						{
							"id":  2,
							"age": 13,
						},
						{
							"id":  3,
							"age": 19,
						},
					},
				),

				NewInMemoryStream(
					[]octosql.VariableName{"age", "id"},
					[]map[octosql.VariableName]interface{}{
						{
							"age": 7,
							"id":  1,
						},
						{
							"id":  2,
							"age": 13,
						},
						{
							"age": 19,
							"id":  3,
						},
					},
				),
			},

			want:    true,
			wantErr: false,
		},

		{
			name: "single column, first stream longer",
			args: args{
				NewInMemoryStream(
					[]octosql.VariableName{"id"},
					[]map[octosql.VariableName]interface{}{
						{
							"id": 1,
						},
						{
							"id": 2,
						},
						{
							"id": 3,
						},
						{
							"id": 4,
						},
					},
				),

				NewInMemoryStream(
					[]octosql.VariableName{"id"},
					[]map[octosql.VariableName]interface{}{
						{
							"id": 1,
						},
						{
							"id": 2,
						},
						{
							"id": 3,
						},
					},
				),
			},

			want:    false,
			wantErr: false,
		},

		{
			name: "single column, second stream longer",
			args: args{
				NewInMemoryStream(
					[]octosql.VariableName{"id"},
					[]map[octosql.VariableName]interface{}{
						{
							"id": 1,
						},
						{
							"id": 2,
						},
						{
							"id": 3,
						},
					},
				),

				NewInMemoryStream(
					[]octosql.VariableName{"id"},
					[]map[octosql.VariableName]interface{}{
						{
							"id": 1,
						},
						{
							"id": 2,
						},
						{
							"id": 3,
						},
						{
							"id": 4,
						},
					},
				),
			},

			want:    false,
			wantErr: false,
		},

		{
			name: "non-matching column names, equal values",
			args: args{
				NewInMemoryStream(
					[]octosql.VariableName{"id", "name"},
					[]map[octosql.VariableName]interface{}{
						{
							"id":   1,
							"name": "a",
						},
						{
							"id":   2,
							"name": "b",
						},
					},
				),

				NewInMemoryStream(
					[]octosql.VariableName{"id", "surname"},
					[]map[octosql.VariableName]interface{}{
						{
							"id":      1,
							"surname": "a",
						},
						{
							"id":      2,
							"surname": "b",
						},
						{
							"id": 3,
						},
					},
				),
			},

			want:    false,
			wantErr: false,
		},

		{
			name: "single column, non-matching values",
			args: args{
				NewInMemoryStream(
					[]octosql.VariableName{"id"},
					[]map[octosql.VariableName]interface{}{
						{
							"id": 1,
						},
						{
							"id": 2,
						},
						{
							"id": 3,
						},
					},
				),

				NewInMemoryStream(
					[]octosql.VariableName{"id"},
					[]map[octosql.VariableName]interface{}{
						{
							"id": 1,
						},
						{
							"id": 2,
						},
						{
							"id": 4,
						},
					},
				),
			},

			want:    false,
			wantErr: false,
		},

		{
			name: "multiple columns, non-matching values",
			args: args{
				NewInMemoryStream(
					[]octosql.VariableName{"id", "age"},
					[]map[octosql.VariableName]interface{}{
						{
							"id":  1,
							"age": 3,
						},
						{
							"id":  2,
							"age": 5,
						},
						{
							"id":  3,
							"age": 10,
						},
					},
				),

				NewInMemoryStream(
					[]octosql.VariableName{"id", "age"},
					[]map[octosql.VariableName]interface{}{
						{
							"id":  1,
							"age": 3,
						},
						{
							"id":  2,
							"age": 6,
						},
						{
							"id":  3,
							"age": 10,
						},
					},
				),
			},

			want:    false,
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
