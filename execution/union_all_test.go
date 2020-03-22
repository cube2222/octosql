package execution

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestUnionAll(t *testing.T) {
	stateStorage := GetTestStorage(t)
	defer func() {
		go stateStorage.Close()
	}()
	tx := stateStorage.BeginTransaction()
	defer tx.Abort()
	ctx := storage.InjectStateTransaction(context.Background(), tx)

	fieldNames := []octosql.VariableName{
		octosql.NewVariableName("age"),
		octosql.NewVariableName("something"),
	}

	type fields struct {
		sources []Node
	}
	tests := []struct {
		name    string
		fields  fields
		want    RecordStream
		wantErr bool
	}{
		{
			name: "simple union all",
			fields: fields{
				sources: []Node{
					NewDummyNode(
						[]*Record{
							NewRecordFromSliceWithNormalize(
								fieldNames,
								[]interface{}{4, "test2"},
							),
							NewRecordFromSliceWithNormalize(
								fieldNames,
								[]interface{}{3, "test3"},
							),
						},
					),
					NewDummyNode(
						[]*Record{
							NewRecordFromSliceWithNormalize(
								fieldNames,
								[]interface{}{5, "test"},
							),
							NewRecordFromSliceWithNormalize(
								fieldNames,
								[]interface{}{3, "test33"},
							),
							NewRecordFromSliceWithNormalize(
								fieldNames,
								[]interface{}{2, "test2"},
							),
						},
					),
					NewDummyNode(
						[]*Record{
							NewRecordFromSliceWithNormalize(
								fieldNames,
								[]interface{}{5, "test"},
							),
						},
					),
				},
			},
			want: NewInMemoryStream(ctx, []*Record{
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{4, "test2"},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{3, "test3"},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{5, "test"},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{3, "test33"},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{2, "test2"},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{5, "test"},
				),
			}),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := GetTestStorage(t)
			defer func() {
				go stateStorage.Close()
			}()
			tx := stateStorage.BeginTransaction()
			ctx := storage.InjectStateTransaction(context.Background(), tx)

			node := &UnionAll{
				sources: tt.fields.sources,
			}
			stream, _, err := node.Get(ctx, octosql.NoVariables(), GetRawStreamID())
			if err != nil {
				t.Fatal(err)
			}

			err = AreStreamsEqualNoOrdering(context.Background(), stateStorage, stream, tt.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnionAll.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
