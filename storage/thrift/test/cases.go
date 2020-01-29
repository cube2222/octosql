package test

import (
	"github.com/thrift-iterator/go/general"
)

// Simple test cases
var THRIFT_DATA_SOURCE_TEST_CASES = []ThriftTestCase{
	{
		name:     "basic string fields",
		ip:       "localhost:9090",
		protocol: "binary",
		secure:   false,
		alias:    "b",
		thriftMeta: `
				struct Record {
					1: string text,
				}
				
				service SomeService {
				   i32 openRecords(),
				   Record getRecord(1:i32 streamID),
				}
			`,
		data: []general.Struct{
			{
				1: "ala ma kota",
			},
			{
				1: "dummy",
			},
		},
		want: SimpleOctosqlData{
			headers:     []string{
				"b.text",
			},
			recordsData: [][]interface{}{
				{ "ala ma kota", },
				{ "dummy", },
			},
		},
	},
	{
		name:     "basic primitive fields",
		ip:       "localhost:9090",
		protocol: "binary",
		secure:   false,
		alias:    "b",
		thriftMeta: `
				struct Record {
					1: string fooString,
					2: i16 fooInt16,
					3: i32 fooInt32,
					4: i64 fooInt64,
					5: double fooDouble,
					6: byte fooByte,
					7: bool fooBool,
				}
				
				service SomeService {
				   i32 openRecords(),
				   Record getRecord(1:i32 streamID),
				}
			`,
		data: []general.Struct{
			{
				1: "sample text",
				2: int16(16),
				3: int32(32),
				4: int64(64),
				5: 0.75,
				6: byte(99),
				7: true,
			},
		},
		want: SimpleOctosqlData{
			headers:     []string{
				"b.foobool",
				"b.foobyte",
				"b.foodouble",
				"b.fooint16",
				"b.fooint32",
				"b.fooint64",
				"b.foostring",
			},
			recordsData: [][]interface{}{
				{
					true,
					byte(99),
					0.75,
					int32(16), // automatic cast: int16 -> int32
					int32(32),
					int64(64),
					"sample text",
				},
			},
		},

	},
	{
		name:     "nullable optionals",
		ip:       "localhost:9090",
		protocol: "binary",
		secure:   false,
		alias:    "b",
		thriftMeta: `
				struct Bar {
					1: string barstring,
					2: int32 bari32,
				}

				struct Record {
					1: string fooString,
					2: optional Bar bar,
					3: i64 fooInt64,
				}
				
				service SomeService {
				   i32 openRecords(),
				   Record getRecord(1:i32 streamID),
				}
			`,
		data: []general.Struct{
			{
				1: "sample text",
				3: int64(64),
			},
		},
		want: SimpleOctosqlData{
			headers: []string{
				"b.bari32",
				"b.barstring",
				"b.fooint64",
				"b.foostring",
			},
			recordsData: [][]interface{}{
				{
					nil, // optional fields should be flattened and inferred from specs
					nil,
					int64(64),
					"sample text",
				},
			},
		},
	},
}