package parquet

import (
	"context"
	"encoding/base32"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage"
)

type Bike struct {
	Id      int64  `parquet:"name=id, type=INT64"`
	Wheels  int64  `parquet:"name=wheels, type=INT64"`
	Year    int64  `parquet:"name=year, type=INT64"`
	OwnerId int64  `parquet:"name=ownerid, type=INT64"`
	Color   string `parquet:"name=color, type=BYTE_ARRAY"`
}

func TestParquetRecordStream_Get(t *testing.T) {
	ctx := context.Background()
	streamId := execution.GetRawStreamID()

	tests := []struct {
		name  string
		path  string
		alias string
		want  []*execution.Record
	}{
		{
			name:  "reading bikes.parquet - happy path",
			path:  "fixtures/bikes.parquet",
			alias: "b",
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{base32.StdEncoding.EncodeToString([]byte("green")), 1, 152849, 3, 2014},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{base32.StdEncoding.EncodeToString([]byte("black")), 2, 106332, 2, 1988},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{base32.StdEncoding.EncodeToString([]byte("purple")), 3, 99148, 2, 2009},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2))),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"b.color", "b.id", "b.ownerid", "b.wheels", "b.year"},
					[]interface{}{base32.StdEncoding.EncodeToString([]byte("orange")), 4, 97521, 2, 1979},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3))),
			},
		},
		{
			// Okay so a few questionable values (they are questionable looking at `output` values, maybe some of them are just wrong...):
			// 	- why is there a 4th almost fully nil record? In output there are 6 but "Total rows: 3" (whaaat)
			// 	- in `decimal_byte_array` fields there are some weird (probably non-ascii) values and I have no clue about their representation
			//	- in `twice_repeated_uint16` field no idea why there is slice of 1 element
			//	- in `map...` fields why there are pairs
			//	- some float values have different number of decimal places (probably faulty `output` file)

			name:  "killer test for all types of data",
			path:  "fixtures/generated_alltypes.uncompressed.parquet",
			alias: "",
			want: []*execution.Record{
				execution.NewRecordFromMapWithNormalize(
					map[octosql.VariableName]interface{}{
						"bool_ct":                               false,
						"bool_lt":                               false,
						"int8_ct":                               -1,
						"int8_lt":                               -1,
						"int16_ct":                              -1,
						"int16_lt":                              -1,
						"int32_ct":                              -1,
						"int32_lt":                              -1,
						"int64_ct":                              -1,
						"int64_lt":                              -1,
						"int96_ct":                              0, // we're just taking first 8 bytes of a number
						"uint8_ct":                              -1,
						"uint8_lt":                              -1,
						"uint16_ct":                             -1,
						"uint16_lt":                             -1,
						"uint32_ct":                             -1,
						"uint32_lt":                             -1,
						"uint64_ct":                             -1,
						"uint64_lt":                             -1,
						"float_ct":                              -1.100000023841858, // why different decimals (?)
						"float_lt":                              -1.100000023841858,
						"double_ct":                             -1.1111111, // why different decimals (?)
						"double_lt":                             -1.1111111,
						"utf8":                                  base32.StdEncoding.EncodeToString([]byte("parquet00/")),
						"string":                                base32.StdEncoding.EncodeToString([]byte("parquet00/")),
						"10_byte_array_ct":                      []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
						"10_byte_array_lt":                      []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
						"date_ct":                               -1,
						"date_lt":                               -1,
						"decimal_int32_ct":                      -1,
						"decimal_int32_lt":                      -1,
						"decimal_int64_ct":                      -1,
						"decimal_int64_lt":                      -1,
						"decimal_byte_array_ct":                 "777Q====", // for some reason, encoding "ÿÿ" isn't working (?)
						"decimal_byte_array_lt":                 "777Q====",
						"decimal_flba_ct":                       []byte{255, 255, 255, 255},
						"decimal_flba_lt":                       []byte{255, 255, 255, 255},
						"enum_ct":                               base32.StdEncoding.EncodeToString([]byte("ENUM   000")),
						"enum_lt":                               base32.StdEncoding.EncodeToString([]byte("ENUM   000")),
						"time_millis_ct":                        0,
						"time_utc_millis_lt":                    0,
						"time_nonutc_millis_lt":                 0,
						"time_micros_ct":                        0,
						"time_utc_micros_lt":                    0,
						"time_nonutc_micros_lt":                 0,
						"time_utc_nanos":                        0,
						"time_nonutc_nanos":                     0,
						"timestamp_millis_ct":                   -1,
						"timestamp_utc_millis_lt":               -1,
						"timestamp_nonutc_millis_lt":            -1,
						"timestamp_micros_ct":                   -1,
						"timestamp_utc_micros_lt":               -1,
						"timestamp_nonutc_micros_lt":            -1,
						"timestamp_utc_nanos":                   -1,
						"timestamp_nonutc_nanos":                -1,
						"interval_ct":                           []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						"interval_lt":                           []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						"json_ct":                               base32.StdEncoding.EncodeToString([]byte("{\"key\":\"value\"}")),
						"json_lt":                               base32.StdEncoding.EncodeToString([]byte("{\"key\":\"value\"}")),
						"bson_ct":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
						"bson_lt":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
						"uuid":                                  []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
						"uint64_dictionary":                     -1,
						"optional_uint32":                       -1,
						"twice_repeated_uint16":                 []interface{}{0}, // don't really understand why is this like that
						"optional_undefined_null":               nil,
						"map_int32_int32.key_value.key":         []interface{}{-1, 0}, // don't really understand why these are pairs
						"map_int32_int32.key_value.value":       []interface{}{-1, 0},
						"map_key_value_bool_bool.key_value.key": []interface{}{false, false},
						"map_key_value_bool_bool.key_value.value": []interface{}{false, false},
						"map_logical.key_value.key":               []interface{}{-1, 0},
						"map_logical.key_value.value":             []interface{}{-1, 0},
						"list_float.list.value":                   []interface{}{-1.100000023841858, 0.0},
						"list_double.list.value":                  []interface{}{-1.111109972000122, 0.0},
					},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0))),
				execution.NewRecordFromMapWithNormalize(
					map[octosql.VariableName]interface{}{
						"bool_ct":                               true,
						"bool_lt":                               true,
						"int8_ct":                               0,
						"int8_lt":                               0,
						"int16_ct":                              0,
						"int16_lt":                              0,
						"int32_ct":                              0,
						"int32_lt":                              0,
						"int64_ct":                              0,
						"int64_lt":                              0,
						"int96_ct":                              0,
						"uint8_ct":                              0,
						"uint8_lt":                              0,
						"uint16_ct":                             0,
						"uint16_lt":                             0,
						"uint32_ct":                             0,
						"uint32_lt":                             0,
						"uint64_ct":                             0,
						"uint64_lt":                             0,
						"float_ct":                              0.000000,
						"float_lt":                              0.000000,
						"double_ct":                             0.000000,
						"double_lt":                             0.000000,
						"utf8":                                  base32.StdEncoding.EncodeToString([]byte("parquet000")),
						"string":                                base32.StdEncoding.EncodeToString([]byte("parquet000")),
						"10_byte_array_ct":                      []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						"10_byte_array_lt":                      []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						"date_ct":                               0,
						"date_lt":                               0,
						"decimal_int32_ct":                      0,
						"decimal_int32_lt":                      0,
						"decimal_int64_ct":                      0,
						"decimal_int64_lt":                      0,
						"decimal_byte_array_ct":                 "AAAA====", // don't understand what is this
						"decimal_byte_array_lt":                 "AAAA====",
						"decimal_flba_ct":                       []byte{0, 0, 0, 0},
						"decimal_flba_lt":                       []byte{0, 0, 0, 0},
						"enum_ct":                               base32.StdEncoding.EncodeToString([]byte("ENUM   001")),
						"enum_lt":                               base32.StdEncoding.EncodeToString([]byte("ENUM   001")),
						"time_millis_ct":                        3661000,
						"time_utc_millis_lt":                    3661000,
						"time_nonutc_millis_lt":                 3661000,
						"time_micros_ct":                        3661000000,
						"time_utc_micros_lt":                    3661000000,
						"time_nonutc_micros_lt":                 3661000000,
						"time_utc_nanos":                        3661000000000,
						"time_nonutc_nanos":                     3661000000000,
						"timestamp_millis_ct":                   0,
						"timestamp_utc_millis_lt":               0,
						"timestamp_nonutc_millis_lt":            0,
						"timestamp_micros_ct":                   0,
						"timestamp_utc_micros_lt":               0,
						"timestamp_nonutc_micros_lt":            0,
						"timestamp_utc_nanos":                   0,
						"timestamp_nonutc_nanos":                0,
						"interval_ct":                           []byte{1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},
						"interval_lt":                           []byte{1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},
						"json_ct":                               base32.StdEncoding.EncodeToString([]byte("{\"key\":\"value\"}")),
						"json_lt":                               base32.StdEncoding.EncodeToString([]byte("{\"key\":\"value\"}")),
						"bson_ct":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
						"bson_lt":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
						"uuid":                                  []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						"uint64_dictionary":                     0,
						"optional_uint32":                       nil,
						"twice_repeated_uint16":                 []interface{}{1, 2},
						"optional_undefined_null":               nil,
						"map_int32_int32.key_value.key":         []interface{}{0, 1},
						"map_int32_int32.key_value.value":       []interface{}{0, 1},
						"map_key_value_bool_bool.key_value.key": []interface{}{true, false},
						"map_key_value_bool_bool.key_value.value": []interface{}{true, false},
						"map_logical.key_value.key":               []interface{}{0, 1},
						"map_logical.key_value.value":             []interface{}{0, 1},
						"list_float.list.value":                   []interface{}{0.0, 1.100000023841858},
						"list_double.list.value":                  []interface{}{0.0, 1.111109972000122},
					},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1))),
				execution.NewRecordFromMapWithNormalize(
					map[octosql.VariableName]interface{}{
						"bool_ct":                               false,
						"bool_lt":                               false,
						"int8_ct":                               1,
						"int8_lt":                               1,
						"int16_ct":                              1,
						"int16_lt":                              1,
						"int32_ct":                              1,
						"int32_lt":                              1,
						"int64_ct":                              1,
						"int64_lt":                              1,
						"int96_ct":                              0,
						"uint8_ct":                              1,
						"uint8_lt":                              1,
						"uint16_ct":                             1,
						"uint16_lt":                             1,
						"uint32_ct":                             1,
						"uint32_lt":                             1,
						"uint64_ct":                             1,
						"uint64_lt":                             1,
						"float_ct":                              1.100000023841858,
						"float_lt":                              1.100000023841858,
						"double_ct":                             1.1111111,
						"double_lt":                             1.1111111,
						"utf8":                                  base32.StdEncoding.EncodeToString([]byte("parquet001")),
						"string":                                base32.StdEncoding.EncodeToString([]byte("parquet001")),
						"10_byte_array_ct":                      []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
						"10_byte_array_lt":                      []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
						"date_ct":                               1,
						"date_lt":                               1,
						"decimal_int32_ct":                      1,
						"decimal_int32_lt":                      1,
						"decimal_int64_ct":                      1,
						"decimal_int64_lt":                      1,
						"decimal_byte_array_ct":                 "AAAQ====", // don't understand what is this
						"decimal_byte_array_lt":                 "AAAQ====",
						"decimal_flba_ct":                       []byte{0, 0, 0, 1},
						"decimal_flba_lt":                       []byte{0, 0, 0, 1},
						"enum_ct":                               base32.StdEncoding.EncodeToString([]byte("ENUM   002")),
						"enum_lt":                               base32.StdEncoding.EncodeToString([]byte("ENUM   002")),
						"time_millis_ct":                        7322000,
						"time_utc_millis_lt":                    7322000,
						"time_nonutc_millis_lt":                 7322000,
						"time_micros_ct":                        7322000000,
						"time_utc_micros_lt":                    7322000000,
						"time_nonutc_micros_lt":                 7322000000,
						"time_utc_nanos":                        7322000000000,
						"time_nonutc_nanos":                     7322000000000,
						"timestamp_millis_ct":                   1,
						"timestamp_utc_millis_lt":               1,
						"timestamp_nonutc_millis_lt":            1,
						"timestamp_micros_ct":                   1,
						"timestamp_utc_micros_lt":               1,
						"timestamp_nonutc_micros_lt":            1,
						"timestamp_utc_nanos":                   1,
						"timestamp_nonutc_nanos":                1,
						"interval_ct":                           []byte{2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0},
						"interval_lt":                           []byte{2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0},
						"json_ct":                               base32.StdEncoding.EncodeToString([]byte("{\"key\":\"value\"}")),
						"json_lt":                               base32.StdEncoding.EncodeToString([]byte("{\"key\":\"value\"}")),
						"bson_ct":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
						"bson_lt":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
						"uuid":                                  []byte{1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},
						"uint64_dictionary":                     1,
						"optional_uint32":                       1,
						"twice_repeated_uint16":                 []interface{}{3, 4},
						"optional_undefined_null":               nil,
						"map_int32_int32.key_value.key":         []interface{}{1, 2},
						"map_int32_int32.key_value.value":       []interface{}{1, 2},
						"map_key_value_bool_bool.key_value.key": []interface{}{false, true},
						"map_key_value_bool_bool.key_value.value": []interface{}{false, true},
						"map_logical.key_value.key":               []interface{}{1, 2},
						"map_logical.key_value.value":             []interface{}{1, 2},
						"list_float.list.value":                   []interface{}{1.100000023841858, 2.200000047683716},
						"list_double.list.value":                  []interface{}{1.111109972000122, 2.222219944000244},
					},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2))),
				execution.NewRecordFromMapWithNormalize(
					map[octosql.VariableName]interface{}{
						"bool_ct":                               nil,
						"bool_lt":                               nil,
						"int8_ct":                               nil,
						"int8_lt":                               nil,
						"int16_ct":                              nil,
						"int16_lt":                              nil,
						"int32_ct":                              nil,
						"int32_lt":                              nil,
						"int64_ct":                              nil,
						"int64_lt":                              nil,
						"int96_ct":                              nil,
						"uint8_ct":                              nil,
						"uint8_lt":                              nil,
						"uint16_ct":                             nil,
						"uint16_lt":                             nil,
						"uint32_ct":                             nil,
						"uint32_lt":                             nil,
						"uint64_ct":                             nil,
						"uint64_lt":                             nil,
						"float_ct":                              nil,
						"float_lt":                              nil,
						"double_ct":                             nil,
						"double_lt":                             nil,
						"utf8":                                  nil,
						"string":                                nil,
						"10_byte_array_ct":                      nil,
						"10_byte_array_lt":                      nil,
						"date_ct":                               nil,
						"date_lt":                               nil,
						"decimal_int32_ct":                      nil,
						"decimal_int32_lt":                      nil,
						"decimal_int64_ct":                      nil,
						"decimal_int64_lt":                      nil,
						"decimal_byte_array_ct":                 nil,
						"decimal_byte_array_lt":                 nil,
						"decimal_flba_ct":                       nil,
						"decimal_flba_lt":                       nil,
						"enum_ct":                               nil,
						"enum_lt":                               nil,
						"time_millis_ct":                        nil,
						"time_utc_millis_lt":                    nil,
						"time_nonutc_millis_lt":                 nil,
						"time_micros_ct":                        nil,
						"time_utc_micros_lt":                    nil,
						"time_nonutc_micros_lt":                 nil,
						"time_utc_nanos":                        nil,
						"time_nonutc_nanos":                     nil,
						"timestamp_millis_ct":                   nil,
						"timestamp_utc_millis_lt":               nil,
						"timestamp_nonutc_millis_lt":            nil,
						"timestamp_micros_ct":                   nil,
						"timestamp_utc_micros_lt":               nil,
						"timestamp_nonutc_micros_lt":            nil,
						"timestamp_utc_nanos":                   nil,
						"timestamp_nonutc_nanos":                nil,
						"interval_ct":                           nil,
						"interval_lt":                           nil,
						"json_ct":                               nil,
						"json_lt":                               nil,
						"bson_ct":                               nil,
						"bson_lt":                               nil,
						"uuid":                                  nil,
						"uint64_dictionary":                     nil,
						"optional_uint32":                       nil,
						"twice_repeated_uint16":                 []interface{}{5},
						"optional_undefined_null":               nil,
						"map_int32_int32.key_value.key":         nil,
						"map_int32_int32.key_value.value":       nil,
						"map_key_value_bool_bool.key_value.key": nil,
						"map_key_value_bool_bool.key_value.value": nil,
						"map_logical.key_value.key":               nil,
						"map_logical.key_value.value":             nil,
						"list_float.list.value":                   nil,
						"list_double.list.value":                  nil,
					},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3))),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)

			ds, err := NewDataSourceBuilderFactory()("test", tt.alias)[0].Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: "test",
							Config: map[string]interface{}{
								"path":      tt.path,
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
				t.Errorf("Couldn't close parquet stream: %v", err)
				return
			}

			if err := want.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close wanted in_memory stream: %v", err)
				return
			}
		})
	}
}
