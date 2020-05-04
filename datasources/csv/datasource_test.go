package csv

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage"
)

type csvDsc struct {
	alias string
	path  string
}

const exampleDir = "fixtures/"

var csvDbs = map[string]csvDsc{
	"people": {
		alias: "p",
		path:  exampleDir + "people.csv",
	},
	"cities": {
		alias: "c",
		path:  exampleDir + "cities.csv",
	},
	"wrongCount": {
		alias: "wc",
		path:  exampleDir + "wrongCount.csv",
	},
	"notUnique": {
		alias: "nu",
		path:  exampleDir + "notUnique.csv",
	},
	"hasHeaders": {
		alias: "hh",
		path:  exampleDir + "hasHeaders.csv",
	},
	"noHeaders": {
		alias: "nh",
		path:  exampleDir + "noHeaders.csv",
	},
	"peopleDot": {
		alias: "p",
		path:  exampleDir + "peopleDot.csv",
	},
	"peopleSemicolon": {
		alias: "p",
		path:  exampleDir + "peopleSemicolon.csv",
	},
}

func TestCSVDataSource_Get(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		csvName string
		wantErr bool
	}{
		{
			name:    "happy path",
			csvName: "cities",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)

			ds, err := NewDataSourceBuilderFactory()("test", csvDbs[tt.csvName].alias)[0].Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: "test",
							Config: map[string]interface{}{
								"path":      csvDbs[tt.csvName].path,
								"headerRow": true,
								"separator": ",",
								"batchSize": 2,
							},
						},
					},
				},
				Storage: stateStorage,
			})
			if err != nil {
				t.Errorf("Error creating data source: %v", err)
			}

			tx := stateStorage.BeginTransaction()
			defer tx.Abort()

			_, _, err = ds.Get(storage.InjectStateTransaction(ctx, tx), octosql.NoVariables(), execution.GetRawStreamID())
			if (err != nil) != tt.wantErr {
				t.Errorf("DataSource.Get() error is %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestCSVRecordStream_Next(t *testing.T) {
	ctx := context.Background()
	streamId := execution.GetRawStreamID()

	tests := []struct {
		name            string
		csvName         string
		hasColumnHeader bool
		separator       string
		fields          []string
		want            []*execution.Record
	}{
		{
			name:            "reading people.csv - happy path",
			csvName:         "people",
			hasColumnHeader: true,
			separator:       ",",
			fields:          []string{"name", "surname", "age", "city"},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"jan", "chomiak", 3, "warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"wojtek", "kuzminski", 4, "warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"adam", "cz", 5, "ciechanowo"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"kuba", "m", 2, "warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3))),
			},
		},
		//{ Infinite loop, because error in reading line occurs
		//	name:            "wrong numbers of columns in a row",
		//	csvName:         "wrongCount",
		//	hasColumnHeader: true,
		//	separator:       ",",
		//	fields:          []string{"name", "surname"},
		//	want:            []*execution.Record{},
		//},
		//{ Infinite loop, because error in reading line occurs
		//	name:            "not unique columns",
		//	csvName:         "notUnique",
		//	hasColumnHeader: true,
		//	separator:       ",",
		//	fields:          []string{"jan", "kazimierz"},
		//	want: []*execution.Record{
		//		execution.NewRecordFromSliceWithNormalize(
		//			[]octosql.VariableName{"nu.jan", "nu.kazimierz"},
		//			[]interface{}{"stanislaw", "august"},
		//			execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
		//	},
		//},
		{
			name:            "file with header row",
			csvName:         "hasHeaders",
			hasColumnHeader: true,
			separator:       ",",
			fields:          []string{"dog", "age"},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"hh.dog", "hh.age"},
					[]interface{}{"Barry", 3},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"hh.dog", "hh.age"},
					[]interface{}{"Flower", 14},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
			},
		},
		{
			name:            "file without header row",
			csvName:         "noHeaders",
			hasColumnHeader: false,
			separator:       ",",
			fields:          []string{"col1", "col2"},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"nh.col1", "nh.col2"},
					[]interface{}{"Barry", 3},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"nh.col1", "nh.col2"},
					[]interface{}{"Flower", 14},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
			},
		},
		{
			name:            "reading peopleDot.csv - happy path",
			csvName:         "peopleDot",
			hasColumnHeader: true,
			separator:       ".",
			fields:          []string{"name", "surname", "age", "city"},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"jan", "chomiak", 3, "warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"wojtek", "kuzminski", 4, "warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"adam", "cz", 5, "ciechanowo"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"kuba", "m", 2, "warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3))),
			},
		},
		{
			name:            "reading peopleSemicolon.csv - happy path",
			csvName:         "peopleSemicolon",
			hasColumnHeader: true,
			separator:       ";",
			fields:          []string{"name", "surname", "age", "city"},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"jan", "chomiak", 3, "warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"wojtek", "kuzminski", 4, "warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"adam", "cz", 5, "ciechanowo"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name", "p.surname", "p.age", "p.city"},
					[]interface{}{"kuba", "m", 2, "warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3))),
			},
		},
		{
			name:            "reading peopleSemicolon.csv - wrong separator",
			csvName:         "people",
			hasColumnHeader: true,
			separator:       ";",
			fields:          []string{"name, surname, age, city"},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name, surname, age, city"},
					[]interface{}{"jan, chomiak, 3, warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name, surname, age, city"},
					[]interface{}{"wojtek, kuzminski, 4, warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name, surname, age, city"},
					[]interface{}{"adam, cz, 5, ciechanowo"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"p.name, surname, age, city"},
					[]interface{}{"kuba, m, 2, warsaw"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3))),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)

			ds, err := NewDataSourceBuilderFactory()("test", csvDbs[tt.csvName].alias)[0].Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: "test",
							Config: map[string]interface{}{
								"path":      csvDbs[tt.csvName].path,
								"headerRow": tt.hasColumnHeader,
								"separator": tt.separator,
								"batchSize": 2,
							},
						},
					},
				},
				Storage: stateStorage,
			})
			if err != nil {
				t.Errorf("Error creating data source: %v", err)
			}

			got := execution.GetTestStream(t, stateStorage, octosql.NoVariables(), ds, execution.GetTestStreamWithStreamID(streamId))

			tx := stateStorage.BeginTransaction()
			want, _, err := execution.NewDummyNode(tt.want).Get(storage.InjectStateTransaction(ctx, tx), octosql.NoVariables(), streamId)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			if err := execution.AreStreamsEqualNoOrdering(storage.InjectStateTransaction(ctx, tx), stateStorage, want, got); err != nil {
				t.Errorf("Streams aren't equal: %v", err)
				return
			}

			if err := got.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close csv stream: %v", err)
				return
			}

			if err := want.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close wanted in_memory stream: %v", err)
				return
			}
		})
	}
}
