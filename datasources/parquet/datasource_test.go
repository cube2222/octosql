package parquet

import (
	"context"
	"encoding/base32"
	"testing"
	"time"

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
		name           string
		path           string
		alias          string
		wantFieldOrder []octosql.VariableName
		want           []map[octosql.VariableName]interface{}
	}{
		{
			name:           "reading bikes.parquet - happy path",
			path:           "fixtures/bikes.parquet",
			alias:          "b",
			wantFieldOrder: []octosql.VariableName{"b.id", "b.wheels", "b.year", "b.ownerid", "b.color"},
			want: []map[octosql.VariableName]interface{}{
				{
					"b.id":      1,
					"b.wheels":  3,
					"b.year":    2014,
					"b.ownerid": 152849,
					"b.color":   base32.StdEncoding.EncodeToString([]byte("green")),
				},
				{
					"b.id":      2,
					"b.wheels":  2,
					"b.year":    1988,
					"b.ownerid": 106332,
					"b.color":   base32.StdEncoding.EncodeToString([]byte("black")),
				},
				{
					"b.id":      3,
					"b.wheels":  2,
					"b.year":    2009,
					"b.ownerid": 99148,
					"b.color":   base32.StdEncoding.EncodeToString([]byte("purple")),
				},
				{
					"b.id":      4,
					"b.wheels":  2,
					"b.year":    1979,
					"b.ownerid": 97521,
					"b.color":   base32.StdEncoding.EncodeToString([]byte("orange")),
				},
			},
		},
		{
			// Okay so a few questionable values (they are questionable looking at `output` values, maybe some of them are just wrong...):
			// 	- why is there a 4th almost fully nil record? In output there are 6 but "Total rows: 3" (whaaat)
			// 	- in `decimal_byte_array` fields there are some weird (probably non-ascii) values and I have no clue about their representation
			//	- in `twice_repeated_uint16` field no idea why there is slice of 1 element
			//	- in `map...` fields why there are pairs
			//	- some float values have different number of decimal places (probably faulty `output` file)

			name:           "killer test for all types of data",
			path:           "fixtures/generated_alltypes.uncompressed.parquet",
			alias:          "",
			wantFieldOrder: []octosql.VariableName{"bool_ct", "bool_lt", "int8_ct", "int8_lt", "int16_ct", "int16_lt", "int32_ct", "int32_lt", "int64_ct", "int64_lt", "int96_ct", "uint8_ct", "uint8_lt", "uint16_ct", "uint16_lt", "uint32_ct", "uint32_lt", "uint64_ct", "uint64_lt", "float_ct", "float_lt", "double_ct", "double_lt", "utf8", "string", "10_byte_array_ct", "10_byte_array_lt", "date_ct", "date_lt", "decimal_int32_ct", "decimal_int32_lt", "decimal_int64_ct", "decimal_int64_lt", "decimal_byte_array_ct", "decimal_byte_array_lt", "decimal_flba_ct", "decimal_flba_lt", "enum_ct", "enum_lt", "time_millis_ct", "time_utc_millis_lt", "time_nonutc_millis_lt", "time_micros_ct", "time_utc_micros_lt", "time_nonutc_micros_lt", "time_utc_nanos", "time_nonutc_nanos", "timestamp_millis_ct", "timestamp_utc_millis_lt", "timestamp_nonutc_millis_lt", "timestamp_micros_ct", "timestamp_utc_micros_lt", "timestamp_nonutc_micros_lt", "timestamp_utc_nanos", "timestamp_nonutc_nanos", "interval_ct", "interval_lt", "json_ct", "json_lt", "bson_ct", "bson_lt", "uuid", "uint64_dictionary", "optional_uint32", "twice_repeated_uint16", "optional_undefined_null", "map_int32_int32.key_value.key", "map_int32_int32.key_value.value", "map_key_value_bool_bool.key_value.key", "map_key_value_bool_bool.key_value.value", "map_logical.key_value.key", "map_logical.key_value.value", "list_float.list.value", "list_double.list.value"},
			want: []map[octosql.VariableName]interface{}{
				{
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
					"uint8_ct":                              255,
					"uint8_lt":                              255,
					"uint16_ct":                             65535,
					"uint16_lt":                             65535,
					"uint32_ct":                             4294967295,
					"uint32_lt":                             4294967295,
					"uint64_ct":                             -1, // OctoSQL uses int64 under the hood, so this overflows.
					"uint64_lt":                             -1,
					"float_ct":                              -1.100000023841858, // why different decimals (?)
					"float_lt":                              -1.100000023841858,
					"double_ct":                             -1.1111111, // why different decimals (?)
					"double_lt":                             -1.1111111,
					"utf8":                                  "parquet00/",
					"string":                                "parquet00/",
					"10_byte_array_ct":                      []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
					"10_byte_array_lt":                      []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
					"date_ct":                               time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC),
					"date_lt":                               time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC),
					"decimal_int32_ct":                      -1e-05,
					"decimal_int32_lt":                      -1e-05,
					"decimal_int64_ct":                      -1e-10,
					"decimal_int64_lt":                      -1e-10,
					"decimal_byte_array_ct":                 -1e-2,
					"decimal_byte_array_lt":                 -1e-2,
					"decimal_flba_ct":                       -1e-5,
					"decimal_flba_lt":                       -1e-5,
					"enum_ct":                               base32.StdEncoding.EncodeToString([]byte("ENUM   000")),
					"enum_lt":                               base32.StdEncoding.EncodeToString([]byte("ENUM   000")),
					"time_millis_ct":                        time.Duration(0),
					"time_utc_millis_lt":                    time.Duration(0),
					"time_nonutc_millis_lt":                 time.Duration(0),
					"time_micros_ct":                        time.Duration(0),
					"time_utc_micros_lt":                    time.Duration(0),
					"time_nonutc_micros_lt":                 time.Duration(0),
					"time_utc_nanos":                        0,
					"time_nonutc_nanos":                     0,
					"timestamp_millis_ct":                   -1,
					"timestamp_utc_millis_lt":               time.Date(1969, 12, 31, 23, 59, 59, int(time.Millisecond*999), time.UTC),
					"timestamp_nonutc_millis_lt":            time.Date(1969, 12, 31, 23, 59, 59, int(time.Millisecond*999), time.Local),
					"timestamp_micros_ct":                   -1,
					"timestamp_utc_micros_lt":               time.Date(1969, 12, 31, 23, 59, 59, int(time.Microsecond*999999), time.UTC),
					"timestamp_nonutc_micros_lt":            time.Date(1969, 12, 31, 23, 59, 59, int(time.Microsecond*999999), time.Local),
					"timestamp_utc_nanos":                   -1,
					"timestamp_nonutc_nanos":                -1,
					"interval_ct":                           []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					"interval_lt":                           []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					"json_ct":                               map[string]interface{}{"key": "value"},
					"json_lt":                               map[string]interface{}{"key": "value"},
					"bson_ct":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
					"bson_lt":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
					"uuid":                                  "ffffffff-ffff-ffff-ffff-ffffffffffff",
					"uint64_dictionary":                     -1,
					"optional_uint32":                       4294967295,
					"twice_repeated_uint16":                 []interface{}{0},
					"optional_undefined_null":               nil,
					"map_int32_int32.key_value.key":         []interface{}{-1, 0},
					"map_int32_int32.key_value.value":       []interface{}{-1, 0},
					"map_key_value_bool_bool.key_value.key": []interface{}{false, false},
					"map_key_value_bool_bool.key_value.value": []interface{}{false, false},
					"map_logical.key_value.key":               []interface{}{-1, 0},
					"map_logical.key_value.value":             []interface{}{-1, 0},
					"list_float.list.value":                   []interface{}{-1.100000023841858, 0.0},
					"list_double.list.value":                  []interface{}{-1.111109972000122, 0.0},
				},
				{
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
					"utf8":                                  "parquet000",
					"string":                                "parquet000",
					"10_byte_array_ct":                      []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					"10_byte_array_lt":                      []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					"date_ct":                               time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
					"date_lt":                               time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
					"decimal_int32_ct":                      0.0,
					"decimal_int32_lt":                      0.0,
					"decimal_int64_ct":                      0.0,
					"decimal_int64_lt":                      0.0,
					"decimal_byte_array_ct":                 0.0,
					"decimal_byte_array_lt":                 0.0,
					"decimal_flba_ct":                       0.0,
					"decimal_flba_lt":                       0.0,
					"enum_ct":                               base32.StdEncoding.EncodeToString([]byte("ENUM   001")),
					"enum_lt":                               base32.StdEncoding.EncodeToString([]byte("ENUM   001")),
					"time_millis_ct":                        time.Hour + time.Minute + time.Second,
					"time_utc_millis_lt":                    time.Hour + time.Minute + time.Second,
					"time_nonutc_millis_lt":                 time.Hour + time.Minute + time.Second,
					"time_micros_ct":                        time.Hour + time.Minute + time.Second,
					"time_utc_micros_lt":                    time.Hour + time.Minute + time.Second,
					"time_nonutc_micros_lt":                 time.Hour + time.Minute + time.Second,
					"time_utc_nanos":                        3661000000000,
					"time_nonutc_nanos":                     3661000000000,
					"timestamp_millis_ct":                   0,
					"timestamp_utc_millis_lt":               time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
					"timestamp_nonutc_millis_lt":            time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local),
					"timestamp_micros_ct":                   0,
					"timestamp_utc_micros_lt":               time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
					"timestamp_nonutc_micros_lt":            time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local),
					"timestamp_utc_nanos":                   0,
					"timestamp_nonutc_nanos":                0,
					"interval_ct":                           []byte{1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},
					"interval_lt":                           []byte{1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},
					"json_ct":                               map[string]interface{}{"key": "value"},
					"json_lt":                               map[string]interface{}{"key": "value"},
					"bson_ct":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
					"bson_lt":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
					"uuid":                                  "00000000-0000-0000-0000-000000000000",
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
				{
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
					"utf8":                                  "parquet001",
					"string":                                "parquet001",
					"10_byte_array_ct":                      []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					"10_byte_array_lt":                      []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					"date_ct":                               time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
					"date_lt":                               time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
					"decimal_int32_ct":                      1e-05,
					"decimal_int32_lt":                      1e-05,
					"decimal_int64_ct":                      1e-10,
					"decimal_int64_lt":                      1e-10,
					"decimal_byte_array_ct":                 1e-2,
					"decimal_byte_array_lt":                 1e-2,
					"decimal_flba_ct":                       1e-05,
					"decimal_flba_lt":                       1e-05,
					"enum_ct":                               base32.StdEncoding.EncodeToString([]byte("ENUM   002")),
					"enum_lt":                               base32.StdEncoding.EncodeToString([]byte("ENUM   002")),
					"time_millis_ct":                        time.Hour*2 + time.Minute*2 + time.Second*2,
					"time_utc_millis_lt":                    time.Hour*2 + time.Minute*2 + time.Second*2,
					"time_nonutc_millis_lt":                 time.Hour*2 + time.Minute*2 + time.Second*2,
					"time_micros_ct":                        time.Hour*2 + time.Minute*2 + time.Second*2,
					"time_utc_micros_lt":                    time.Hour*2 + time.Minute*2 + time.Second*2,
					"time_nonutc_micros_lt":                 time.Hour*2 + time.Minute*2 + time.Second*2,
					"time_utc_nanos":                        7322000000000,
					"time_nonutc_nanos":                     7322000000000,
					"timestamp_millis_ct":                   1,
					"timestamp_utc_millis_lt":               time.Date(1970, 1, 1, 0, 0, 0, int(time.Millisecond), time.UTC),
					"timestamp_nonutc_millis_lt":            time.Date(1970, 1, 1, 0, 0, 0, int(time.Millisecond), time.Local),
					"timestamp_micros_ct":                   1,
					"timestamp_utc_micros_lt":               time.Date(1970, 1, 1, 0, 0, 0, int(time.Microsecond), time.UTC),
					"timestamp_nonutc_micros_lt":            time.Date(1970, 1, 1, 0, 0, 0, int(time.Microsecond), time.Local),
					"timestamp_utc_nanos":                   1,
					"timestamp_nonutc_nanos":                1,
					"interval_ct":                           []byte{2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0},
					"interval_lt":                           []byte{2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0},
					"json_ct":                               map[string]interface{}{"key": "value"},
					"json_lt":                               map[string]interface{}{"key": "value"},
					"bson_ct":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
					"bson_lt":                               base32.StdEncoding.EncodeToString([]byte("BSON")),
					"uuid":                                  "01000000-0100-0000-0100-000001000000",
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
				{
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

			wantRecords := make([]*execution.Record, len(tt.want))
			for i := range wantRecords {
				values := make([]interface{}, len(tt.want[i]))
				for j, field := range tt.wantFieldOrder {
					values[j] = tt.want[i][field]
				}
				wantRecords[i] = execution.NewRecordFromSliceWithNormalize(
					tt.wantFieldOrder,
					values,
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, i)),
				)
			}

			tx := stateStorage.BeginTransaction()
			want, _, err := execution.NewDummyNode(wantRecords).Get(storage.InjectStateTransaction(ctx, tx), octosql.NoVariables(), streamId)
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
