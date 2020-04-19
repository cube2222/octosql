package logical

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
)

func Test_isConjunctionOfEqualities(t *testing.T) {
	type args struct {
		f physical.Formula
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "fail - OR",
			args: args{
				f: physical.NewOr(
					physical.NewConstant(true),
					physical.NewPredicate(
						physical.NewVariable("a"),
						physical.Equal,
						physical.NewVariable("b"),
					),
				),
			},
			want: false,
		},
		{
			name: "fail - negation",
			args: args{
				f: physical.NewAnd(
					physical.NewNot(
						physical.NewConstant(true),
					),
					physical.NewPredicate(
						physical.NewVariable("a"),
						physical.Equal,
						physical.NewVariable("b"),
					),
				),
			},
			want: false,
		},
		{
			name: "fail - predicate with inequality",
			args: args{
				f: physical.NewAnd(
					physical.NewConstant(true),
					physical.NewPredicate(
						physical.NewVariable("a"),
						physical.LessThan,
						physical.NewVariable("b"),
					),
				),
			},
			want: false,
		},
		{
			name: "fail - false constant",
			args: args{
				f: physical.NewAnd(
					physical.NewConstant(false),
					physical.NewPredicate(
						physical.NewVariable("a"),
						physical.Equal,
						physical.NewVariable("b"),
					),
				),
			},
			want: false,
		},
		{
			name: "pass",
			args: args{
				f: physical.NewAnd(
					physical.NewConstant(true),
					physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable("a"),
							physical.Equal,
							physical.NewVariable("b"),
						),
						physical.NewPredicate(
							physical.NewVariable("y"),
							physical.Equal,
							physical.NewVariable("x"),
						),
					),
				),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isConjunctionOfEqualities(tt.args.f); got != tt.want {
				t.Errorf("isConjunctionOfEqualities() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getKeysFromFormula(t *testing.T) {
	type args struct {
		formula         physical.Formula
		sourceNamespace *metadata.Namespace
		joinedNamespace *metadata.Namespace
	}
	tests := []struct {
		name          string
		args          args
		wantSourceKey []physical.Expression
		wantJoinedKey []physical.Expression
		wantErr       bool
	}{
		{
			name: "empty formula",
			args: args{
				formula:         physical.NewConstant(true),
				sourceNamespace: metadata.NewNamespace(nil, nil),
				joinedNamespace: metadata.NewNamespace(nil, nil),
			},
			wantSourceKey: []physical.Expression{},
			wantJoinedKey: []physical.Expression{},
			wantErr:       false,
		},

		{
			name: "single correct predicate",
			args: args{
				formula: physical.NewPredicate(
					physical.NewVariable("source"),
					physical.Equal,
					physical.NewVariable("joined"),
				),
				sourceNamespace: metadata.NewNamespace(nil, []octosql.VariableName{"source"}),
				joinedNamespace: metadata.NewNamespace(nil, []octosql.VariableName{"joined"}),
			},
			wantSourceKey: []physical.Expression{physical.NewVariable("source")},
			wantJoinedKey: []physical.Expression{physical.NewVariable("joined")},
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceKey, joinedKey, err := getKeysFromFormula(tt.args.formula, tt.args.sourceNamespace, tt.args.joinedNamespace)
			if (err != nil) != tt.wantErr {
				t.Errorf("getKeysFromFormula() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(sourceKey, tt.wantSourceKey) {
				t.Errorf("getKeysAndEventTimeFromFormula() sourceKey = %v, want %v", sourceKey, tt.wantSourceKey)
			}

			if !reflect.DeepEqual(joinedKey, tt.wantJoinedKey) {
				t.Errorf("getKeysAndEventTimeFromFormula() joinedKey = %v, want %v", joinedKey, tt.wantJoinedKey)
			}
		})
	}
}
