package execution

import (
	"log"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/golang/protobuf/proto"
)

func TestAreEqual(t *testing.T) {
	type args struct {
		first, second *Record
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "simple equal records - no metadata",
			args: args{
				first: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeInt(3),
						octosql.MakeString("ma kota ala"),
					},
				),
				second: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeInt(3),
						octosql.MakeString("ma kota ala"),
					},
				),
			},
			want: true,
		},

		{
			name: "simple equal records - with metadata",
			args: args{
				first: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeInt(3),
						octosql.MakeString("ma kota ala"),
					},
					WithUndo(),
					WithEventTimeField("something"),
					WithID(&RecordID{ID: "ID"}),
				),
				second: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeInt(3),
						octosql.MakeString("ma kota ala"),
					},
					WithUndo(),
					WithEventTimeField("something"),
					WithID(&RecordID{ID: "ID"}),
				),
			},
			want: true,
		},

		{
			name: "different records - values mismatch",
			args: args{
				first: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeInt(3),
						octosql.MakeString("ma kota ala"),
					},
				),
				second: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeInt(4),
						octosql.MakeString("ma kota ala"),
					},
				),
			},
			want: false,
		},

		{
			name: "different records - variable names mismatch",
			args: args{
				first: NewRecordFromSlice(
					[]octosql.VariableName{"a", "c"},
					[]octosql.Value{
						octosql.MakeInt(3),
						octosql.MakeString("ma kota ala"),
					},
				),
				second: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeInt(3),
						octosql.MakeString("ma kota ala"),
					},
				),
			},
			want: false,
		},

		{
			name: "different records - different order",
			args: args{
				first: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeInt(3),
						octosql.MakeString("ma kota ala"),
					},
				),
				second: NewRecordFromSlice(
					[]octosql.VariableName{"b", "a"},
					[]octosql.Value{
						octosql.MakeString("ma kota ala"),
						octosql.MakeInt(3),
					},
				),
			},
			want: false,
		},

		{
			name: "different records - different metadata",
			args: args{
				first: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeInt(3),
						octosql.MakeString("ma kota ala"),
					},
					WithUndo(),
					WithID(&RecordID{ID: "ID"}),
				),
				second: NewRecordFromSlice(
					[]octosql.VariableName{"b", "a"},
					[]octosql.Value{
						octosql.MakeString("ma kota ala"),
						octosql.MakeInt(3),
					},
					WithID(&RecordID{ID: "ID"}),
				),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.args.first.Equal(tt.args.second); got != tt.want {
				t.Errorf("NewRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecordMarshal(t *testing.T) {
	type args struct {
		rec *Record
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "simple record 1 - no metadata",
			args: args{
				rec: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b", "c"},
					[]octosql.Value{
						octosql.MakeInt(2),
						octosql.MakeNull(),
						octosql.MakeString("here we are"),
					},
				),
			},
		},
		{
			name: "simple record 2 - all basic types, no metadata",
			args: args{
				rec: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b", "c", "d", "e", "f", "g", "h"},
					[]octosql.Value{
						octosql.MakeNull(),
						octosql.MakePhantom(),
						octosql.MakeInt(1),
						octosql.MakeFloat(1.284),
						octosql.MakeBool(true),
						octosql.MakeString("something else"),
						octosql.MakeTime(time.Unix(42690321, 123456)),
						octosql.MakeDuration(18247),
					},
				),
			},
		},

		{
			name: "simple record 3 - tuple and object, no metadata",
			args: args{
				rec: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeTuple([]octosql.Value{
							octosql.MakeInt(3),
							octosql.MakeString("inside the tuple..."),
							octosql.MakeBool(false),
						},
						),
						octosql.MakeObject(map[string]octosql.Value{
							"key1": octosql.MakeNull(),
							"key2": octosql.MakePhantom(),
							"key3": octosql.MakeFloat(1.114),
						}),
					},
				),
			},
		},

		{
			name: "complex record with metadata",
			args: args{
				rec: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b", "c", "d", "e"},
					[]octosql.Value{
						octosql.MakeTuple([]octosql.Value{
							octosql.MakeInt(3),
							octosql.MakeString("inside the tuple..."),
							octosql.MakeBool(false),
						},
						),
						octosql.MakeObject(map[string]octosql.Value{
							"key1": octosql.MakeNull(),
							"key2": octosql.MakePhantom(),
							"key3": octosql.MakeFloat(1.114),
						}),
						octosql.MakeInt(15),
						octosql.MakePhantom(),
						octosql.MakeTime(time.Unix(42690321, 123456)),
					},
					WithUndo(),
					WithEventTimeField("this is my event time field"),
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bytes, err := proto.Marshal(tt.args.rec)
			if err != nil {
				log.Fatal(err)
			}

			var resultRec Record

			err = proto.Unmarshal(bytes, &resultRec)
			if err != nil {
				log.Fatal(err)
			}

			if !tt.args.rec.Equal(&resultRec) {
				log.Fatal("The unmarshaled record is different from the original one")
			}
		})
	}
}

func TestRecordConstructors(t *testing.T) {
	type args struct {
		first  *Record
		second *Record
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "first test - basic types",
			args: args{
				first: NewRecord(
					[]octosql.VariableName{"a", "b", "c", "d", "e", "f", "g", "h"},
					map[octosql.VariableName]octosql.Value{
						"a": octosql.MakeNull(),
						"b": octosql.MakePhantom(),
						"c": octosql.MakeInt(3),
						"d": octosql.MakeFloat(5.182),
						"e": octosql.MakeBool(false),
						"f": octosql.MakeString("a string of great importance"),
						"g": octosql.MakeTime(time.Unix(1111111, 492)),
						"h": octosql.MakeDuration(1823),
					},
				),
				second: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b", "c", "d", "e", "f", "g", "h"},
					[]octosql.Value{
						octosql.MakeNull(),
						octosql.MakePhantom(),
						octosql.MakeInt(3),
						octosql.MakeFloat(5.182),
						octosql.MakeBool(false),
						octosql.MakeString("a string of great importance"),
						octosql.MakeTime(time.Unix(1111111, 492)),
						octosql.MakeDuration(1823),
					},
				),
			},
		},

		{
			name: "second test - complex types",
			args: args{
				first: NewRecord(
					[]octosql.VariableName{"a", "b"},
					map[octosql.VariableName]octosql.Value{
						"a": octosql.MakeTuple(
							[]octosql.Value{
								octosql.MakeNull(),
								octosql.MakeTime(time.Unix(1111111, 492)),
								octosql.MakeInt(7),
								octosql.MakeFloat(11.1145678),
							},
						),
						"b": octosql.MakeObject(
							map[string]octosql.Value{
								"key1": octosql.MakeDuration(114),
								"key2": octosql.MakeString("something_123$$$!@#__"),
							},
						),
					},
				),
				second: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeTuple(
							[]octosql.Value{
								octosql.MakeNull(),
								octosql.MakeTime(time.Unix(1111111, 492)),
								octosql.MakeInt(7),
								octosql.MakeFloat(11.1145678),
							},
						),
						octosql.MakeObject(
							map[string]octosql.Value{
								"key1": octosql.MakeDuration(114),
								"key2": octosql.MakeString("something_123$$$!@#__"),
							},
						),
					},
				),
			},
		},

		{
			name: "third test - metadata",
			args: args{
				first: NewRecord(
					[]octosql.VariableName{"a", "b", "c"},
					map[octosql.VariableName]octosql.Value{
						"a": octosql.MakeInt(42),
						"b": octosql.MakeFloat(42.0),
						"c": octosql.MakePhantom(),
					},
					WithUndo(),
					WithID(&RecordID{ID: "this_is_my_id"}),
				),
				second: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b", "c"},
					[]octosql.Value{
						octosql.MakeInt(42),
						octosql.MakeFloat(42.0),
						octosql.MakePhantom(),
					},
					WithID(&RecordID{ID: "this_is_my_id"}),
					WithUndo(),
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.args.first.Equal(tt.args.second) {
				log.Fatalf("The records aren't equal!\n %v \n %v", tt.args.first, tt.args.second)
			}
		})
	}
}
