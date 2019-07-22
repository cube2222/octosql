/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Copyright 2019 The OctoSQL Authors

Licensed under the MIT license, as in the LICENSE file
*/

package sqlparser

import (
	"testing"
)

func TestPreview(t *testing.T) {
	testcases := []struct {
		sql  string
		want int
	}{
		{"select ...", StmtSelect},
		{"    select ...", StmtSelect},
		{"(select ...", StmtSelect},
		{"insert ...", StmtInsert},
		{"replace ....", StmtReplace},
		{"   update ...", StmtUpdate},
		{"Update", StmtUpdate},
		{"UPDATE ...", StmtUpdate},
		{"\n\t    delete ...", StmtDelete},
		{"", StmtUnknown},
		{" ", StmtUnknown},
		{"begin", StmtBegin},
		{" begin", StmtBegin},
		{" begin ", StmtBegin},
		{"\n\t begin ", StmtBegin},
		{"... begin ", StmtUnknown},
		{"begin ...", StmtUnknown},
		{"begin /* ... */", StmtBegin},
		{"begin /* ... *//*test*/", StmtBegin},
		{"start transaction", StmtBegin},
		{"commit", StmtCommit},
		{"commit /*...*/", StmtCommit},
		{"rollback", StmtRollback},
		{"rollback /*...*/", StmtRollback},
		{"create", StmtDDL},
		{"alter", StmtDDL},
		{"rename", StmtDDL},
		{"drop", StmtDDL},
		{"set", StmtSet},
		{"show", StmtShow},
		{"use", StmtUse},
		{"analyze", StmtOther},
		{"describe", StmtOther},
		{"desc", StmtOther},
		{"explain", StmtOther},
		{"repair", StmtOther},
		{"optimize", StmtOther},
		{"truncate", StmtDDL},
		{"unknown", StmtUnknown},

		{"/* leading comment */ select ...", StmtSelect},
		{"/* leading comment */ (select ...", StmtSelect},
		{"/* leading comment */ /* leading comment 2 */ select ...", StmtSelect},
		{"/*! MySQL-specific comment */", StmtComment},
		{"/*!50708 MySQL-version comment */", StmtComment},
		{"-- leading single line comment \n select ...", StmtSelect},
		{"-- leading single line comment \n -- leading single line comment 2\n select ...", StmtSelect},

		{"/* leading comment no end select ...", StmtUnknown},
		{"-- leading single line comment no end select ...", StmtUnknown},
	}
	for _, tcase := range testcases {
		if got := Preview(tcase.sql); got != tcase.want {
			t.Errorf("Preview(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}

func TestIsDML(t *testing.T) {
	testcases := []struct {
		sql  string
		want bool
	}{
		{"   update ...", true},
		{"Update", true},
		{"UPDATE ...", true},
		{"\n\t    delete ...", true},
		{"insert ...", true},
		{"replace ...", true},
		{"select ...", false},
		{"    select ...", false},
		{"", false},
		{" ", false},
	}
	for _, tcase := range testcases {
		if got := IsDML(tcase.sql); got != tcase.want {
			t.Errorf("IsDML(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}

func TestGetTableName(t *testing.T) {
	testcases := []struct {
		in, out string
	}{{
		in:  "select * from t",
		out: "t",
	}, {
		in:  "select * from t.t",
		out: "",
	}, {
		in:  "select * from (select * from t) as tt",
		out: "",
	}}

	for _, tc := range testcases {
		tree, err := Parse(tc.in)
		if err != nil {
			t.Error(err)
			continue
		}
		out := GetTableName(tree.(*Select).From[0].(*AliasedTableExpr).Expr)
		if out.String() != tc.out {
			t.Errorf("GetTableName('%s'): %s, want %s", tc.in, out, tc.out)
		}
	}
}

func TestIsColName(t *testing.T) {
	testcases := []struct {
		in  Expr
		out bool
	}{{
		in:  &ColName{},
		out: true,
	}, {
		in: newHexVal(""),
	}}
	for _, tc := range testcases {
		out := IsColName(tc.in)
		if out != tc.out {
			t.Errorf("IsColName(%T): %v, want %v", tc.in, out, tc.out)
		}
	}
}

func TestIsNull(t *testing.T) {
	testcases := []struct {
		in  Expr
		out bool
	}{{
		in:  &NullVal{},
		out: true,
	}, {
		in: newStrVal(""),
	}}
	for _, tc := range testcases {
		out := IsNull(tc.in)
		if out != tc.out {
			t.Errorf("IsNull(%T): %v, want %v", tc.in, out, tc.out)
		}
	}
}

func newStrVal(in string) *SQLVal {
	return NewStrVal([]byte(in))
}

func newIntVal(in string) *SQLVal {
	return NewIntVal([]byte(in))
}

func newHexVal(in string) *SQLVal {
	return NewHexVal([]byte(in))
}

func newValArg(in string) *SQLVal {
	return NewValArg([]byte(in))
}
