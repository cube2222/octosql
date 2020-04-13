package execution

/*func TestName(t *testing.T) {
	ctx := context.Background()
	db := GetTestStorage(t)

	inPart0 := NewDummyNode([]*Record{
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("red"),
				octosql.MakeInt(5),
			},
			WithID(NewRecordID("0.0")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("blue"),
				octosql.MakeInt(3),
			},
			WithID(NewRecordID("0.1")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("red"),
				octosql.MakeInt(5),
			},
			WithID(NewRecordID("0.2")),
		),
	})
	inPart1 := NewDummyNode([]*Record{
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("green"),
				octosql.MakeInt(4),
			},
			WithID(NewRecordID("1.0")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("blue"),
				octosql.MakeInt(3),
			},
			WithID(NewRecordID("1.1")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("green"),
				octosql.MakeInt(5),
			},
			WithID(NewRecordID("1.2")),
		),
	})
	inPart2 := NewDummyNode([]*Record{
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("green"),
				octosql.MakeInt(5),
			},
			WithID(NewRecordID("2.0")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("blue"),
				octosql.MakeInt(3),
			},
			WithID(NewRecordID("2.1")),
		),
		NewRecordFromSlice(
			[]octosql.VariableName{octosql.NewVariableName("color"), octosql.NewVariableName("age")},
			[]octosql.Value{
				octosql.MakeString("red"),
				octosql.MakeInt(5),
			},
			WithID(NewRecordID("2.2")),
		),
	})

	shuffle := NewShuffle(
		2,
		NewKeyHashingStrategy(octosql.NoVariables(), []Expression{NewVariable(octosql.NewVariableName("color"))}),
		[]Node{inPart0, inPart1, inPart2},
	)

	tx := db.BeginTransaction()
	ctxWithTx := storage.InjectStateTransaction(ctx, tx)

	streams, execOutputs, err := GetAndStartAllShuffles(ctxWithTx, db, tx, []Node{shuffle, shuffle}, octosql.NoVariables())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	streams = streams
	execOutputs = execOutputs

	records0, err := ReadAll(ctx, db, streams[0])
	if err != nil {
		t.Fatal(err)
	}
	records0 = records0

	log.Printf("%+v", records0)

	records1, err := ReadAll(ctx, db, streams[1])
	if err != nil {
		t.Fatal(err)
	}
	records1 = records1

	log.Printf("%+v", records1)

	t.Fatal("ok")
}*/

/*func TestUnionAll(t *testing.T) {
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
}*/
