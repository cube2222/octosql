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
					WithID(ID{ID: "ID"}),
				),
				second: NewRecordFromSlice(
					[]octosql.VariableName{"a", "b"},
					[]octosql.Value{
						octosql.MakeInt(3),
						octosql.MakeString("ma kota ala"),
					},
					WithUndo(),
					WithEventTimeField("something"),
					WithID(ID{ID: "ID"}),
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
					WithID(ID{ID: "ID"}),
				),
				second: NewRecordFromSlice(
					[]octosql.VariableName{"b", "a"},
					[]octosql.Value{
						octosql.MakeString("ma kota ala"),
						octosql.MakeInt(3),
					},
					WithID(ID{ID: "ID"}),
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
						octosql.MakeTime(time.Now()),
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
						octosql.MakeTime(time.Now()),
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
