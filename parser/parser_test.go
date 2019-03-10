package parser

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql/logical"
	"github.com/xwb1989/sqlparser"
)

func TestParseSelect(t *testing.T) {
	type args struct {
		statement string
	}
	tests := []struct {
		name    string
		args    args
		want    logical.Node
		wantErr bool
	}{
		{
			name: "simple select",
			args: args{
				statement: "SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 3",
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewVariable("p2.name"),
					logical.NewVariable("p2.age"),
				},
				logical.NewFilter(
					logical.NewPredicate(
						logical.NewVariable("p2.age"),
						logical.MoreThan,
						logical.NewConstant(3),
					),
					logical.NewDataSource("people", "p2"),
				),
			),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tt.args.statement)
			if err != nil {
				t.Fatal(err)
			}

			statement := stmt.(*sqlparser.Select)

			got, err := ParseSelect(statement)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseSelect() = %v, want %v", got, tt.want)
			}
		})
	}
}
