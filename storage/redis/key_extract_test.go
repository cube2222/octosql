package redis

import (
	"context"
	"reflect"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

func TestNewKeyFormula(t *testing.T) {
	type args struct {
		formula physical.Formula
		key     string
		alias   string
	}
	tests := []struct {
		name    string
		args    args
		want    KeyFormula
		wantErr bool
	}{
		{
			name: "simple constant",
			args: args{
				formula: physical.NewConstant(
					true,
				),
				key:   "key",
				alias: "p",
			},
			want: &Constant{
				true,
			},
			wantErr: false,
		},
		{
			name: "simple constant vol2",
			args: args{
				formula: physical.NewConstant(
					false,
				),
				key:   "key",
				alias: "p",
			},
			want: &Constant{
				false,
			},
			wantErr: false,
		},
		{
			// "p.key" = "const_0"
			name: "simple equal",
			args: args{
				formula: physical.NewPredicate(
					physical.NewVariable("p.key"),
					physical.NewRelation("equal"),
					physical.NewVariable("const_0"),
				),
				key:   "key",
				alias: "p",
			},
			want: &Equal{
				execution.NewVariable("const_0"),
			},
			wantErr: false,
		},
		{
			// "const_0" = "p.key"
			name: "simple equal reverse",
			args: args{
				formula: physical.NewPredicate(
					physical.NewVariable("const_0"),
					physical.NewRelation("equal"),
					physical.NewVariable("p.key"),
				),
				key:   "key",
				alias: "p",
			},
			want: &Equal{
				execution.NewVariable("const_0"),
			},
			wantErr: false,
		},
		{
			// "const_0" = "const_1"
			name: "simple equal no p.key",
			args: args{
				formula: physical.NewPredicate(
					physical.NewVariable("const_0"),
					physical.NewRelation("equal"),
					physical.NewVariable("const_1"),
				),
				key:   "key",
				alias: "p",
			},
			want:    nil,
			wantErr: true,
		},
		{
			// ("p.key" = "const_0") or ("p.key" = "const_0")
			name: "simple or dummy",
			args: args{
				formula: physical.NewOr(
					physical.NewPredicate(
						physical.NewVariable("p.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_0"),
					),
					physical.NewPredicate(
						physical.NewVariable("p.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_0"),
					),
				),
				key:   "key",
				alias: "p",
			},
			want: &Or{
				&Equal{execution.NewVariable("const_0")},
				&Equal{execution.NewVariable("const_0")},
			},
			wantErr: false,
		},
		{
			// ("p.key" = "const_0") or ("p.key" = "const_1")
			name: "simple or",
			args: args{
				formula: physical.NewOr(
					physical.NewPredicate(
						physical.NewVariable("p.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_0"),
					),
					physical.NewPredicate(
						physical.NewVariable("p.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_1"),
					),
				),
				key:   "key",
				alias: "p",
			},
			want: &Or{
				&Equal{execution.NewVariable("const_0")},
				&Equal{execution.NewVariable("const_1")},
			},
			wantErr: false,
		},
		{
			// (("p.key" = "const_1") or ("p.key" = "const_1")) or (("p.key" = "const_0") or ("p.key" = "const_1"))
			name: "complex or",
			args: args{
				formula: physical.NewOr(
					physical.NewOr(
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_1"),
						),
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_0"),
						),
					),
					physical.NewOr(
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_0"),
						),
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_1"),
						),
					),
				),
				key:   "key",
				alias: "p",
			},
			want: &Or{
				&Or{
					&Equal{execution.NewVariable("const_1")},
					&Equal{execution.NewVariable("const_0")},
				},
				&Or{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_1")},
				},
			},
			wantErr: false,
		},
		{
			//("p.key" = "const_0") and ("p.key" = "const_0")
			name: "simple and dummy",
			args: args{
				formula: physical.NewAnd(
					physical.NewPredicate(
						physical.NewVariable("p.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_0"),
					),
					physical.NewPredicate(
						physical.NewVariable("p.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_0"),
					),
				),
				key:   "key",
				alias: "p",
			},
			want: &And{
				&Equal{execution.NewVariable("const_0")},
				&Equal{execution.NewVariable("const_0")},
			},
			wantErr: false,
		},
		{
			//("p.key" = "const_1") and ("p.key" = "const_0")
			name: "simple and",
			args: args{
				formula: physical.NewAnd(
					physical.NewPredicate(
						physical.NewVariable("p.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_1"),
					),
					physical.NewPredicate(
						physical.NewVariable("p.key"),
						physical.NewRelation("equal"),
						physical.NewVariable("const_0"),
					),
				),
				key:   "key",
				alias: "p",
			},
			want: &And{
				&Equal{execution.NewVariable("const_1")},
				&Equal{execution.NewVariable("const_0")},
			},
			wantErr: false,
		},
		{
			//(("p.key" = "const_0") and ("p.key" = "const_1")) and (("p.key" = "const_2") and ("p.key" = "const_3"))
			name: "complex and",
			args: args{
				formula: physical.NewAnd(
					physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_0"),
						),
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_1"),
						),
					),
					physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_2"),
						),
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_3"),
						),
					),
				),
				key:   "key",
				alias: "p",
			},
			want: &And{
				&And{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_1")},
				},
				&And{
					&Equal{execution.NewVariable("const_2")},
					&Equal{execution.NewVariable("const_3")},
				},
			},
			wantErr: false,
		},
		{
			//[("p.key" = "const_0") and ("p.key" = "const_1")] or {[("p.key" = "const_2") or ("p.key" = "const_1")] and ("p.key" = "const_42")}
			name: "most complex",
			args: args{
				formula: physical.NewOr(
					physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_0"),
						),
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_1"),
						),
					),
					physical.NewAnd(
						physical.NewOr(
							physical.NewPredicate(
								physical.NewVariable("p.key"),
								physical.NewRelation("equal"),
								physical.NewVariable("const_2"),
							),
							physical.NewPredicate(
								physical.NewVariable("p.key"),
								physical.NewRelation("equal"),
								physical.NewVariable("const_1"),
							),
						),
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_42"),
						),
					),
				),
				key:   "key",
				alias: "p",
			},
			want: &Or{
				&And{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_1")},
				},
				&And{
					&Or{
						&Equal{execution.NewVariable("const_2")},
						&Equal{execution.NewVariable("const_1")},
					},
					&Equal{execution.NewVariable("const_42")},
				},
			},
			wantErr: false,
		},
		{
			//[("p.key" = "const_0") and TRUE] or {[FALSE or ("p.key" = "const_1")] and TRUE}
			name: "complex with constants",
			args: args{
				formula: physical.NewOr(
					physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable("p.key"),
							physical.NewRelation("equal"),
							physical.NewVariable("const_0"),
						),
						physical.NewConstant(
							true,
						),
					),
					physical.NewAnd(
						physical.NewOr(
							physical.NewConstant(
								false,
							),
							physical.NewPredicate(
								physical.NewVariable("p.key"),
								physical.NewRelation("equal"),
								physical.NewVariable("const_1"),
							),
						),
						physical.NewConstant(
							true,
						),
					),
				),
				key:   "key",
				alias: "p",
			},
			want: &Or{
				&And{
					&Equal{execution.NewVariable("const_0")},
					&Constant{true},
				},
				&And{
					&Or{
						&Constant{false},
						&Equal{execution.NewVariable("const_1")},
					},
					&Constant{true},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewKeyFormula(tt.args.formula, tt.args.key, tt.args.alias, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKeyFormula() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewKeyFormula() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnd_GetAllKeys(t *testing.T) {
	ctx := context.Background()
	type fields struct {
		left  KeyFormula
		right KeyFormula
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *redisKeys
		wantErr bool
	}{
		{
			// "const_0" and "const_0"
			name: "simple and",
			fields: fields{
				left:  &Equal{execution.NewVariable("const_0")},
				right: &Equal{execution.NewVariable("const_0")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key0": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// "const_0" and TRUE
			name: "simple and constant true",
			fields: fields{
				left:  &Equal{execution.NewVariable("const_0")},
				right: &Constant{true},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key0": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// TRUE and TRUE
			name: "dumb true",
			fields: fields{
				left:  &Constant{true},
				right: &Constant{true},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{},
			},
			want: &redisKeys{
				map[string]interface{}{},
				True,
			},
			wantErr: false,
		},
		{
			// FALSE and FALSE
			name: "dumb false",
			fields: fields{
				left:  &Constant{false},
				right: &Constant{false},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{},
			},
			want: &redisKeys{
				map[string]interface{}{},
				False,
			},
			wantErr: false,
		},
		{
			// "const_0" and FALSE
			name: "simple and constant false",
			fields: fields{
				left:  &Equal{execution.NewVariable("const_0")},
				right: &Constant{false},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{},
				False,
			},
			wantErr: false,
		},
		{
			// "const_0" and "const_1"
			name: "simple and wrong",
			fields: fields{
				left:  &Equal{execution.NewVariable("const_0")},
				right: &Equal{execution.NewVariable("const_1")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// "const_0" and "const_0"
			name: "simple and no variables",
			fields: fields{
				left:  &Equal{execution.NewVariable("const_0")},
				right: &Equal{execution.NewVariable("const_0")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_1": octosql.MakeString("key1"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			// ("const_0" or True) and "const_1"
			name: "tricky and with constant",
			fields: fields{
				left: &Or{
					&Equal{execution.NewVariable("const_0")},
					&Constant{true},
				},
				right: &Equal{execution.NewVariable("const_1")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key1": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// ("const_0" and "const_0") and ("const_0" and "const_0")
			name: "complex and dummy",
			fields: fields{
				left: &And{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_0")},
				},
				right: &And{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_0")},
				},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key0": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// ("const_0" and "const_0") and ("const_1" and "const_1")
			name: "complex and wrong",
			fields: fields{
				left: &And{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_0")},
				},
				right: &And{
					&Equal{execution.NewVariable("const_1")},
					&Equal{execution.NewVariable("const_1")},
				},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// ("const_0" or "const_1") and ("const_1" and "const_1")
			name: "very complex and",
			fields: fields{
				left: &Or{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_1")},
				},
				right: &And{
					&Equal{execution.NewVariable("const_1")},
					&Equal{execution.NewVariable("const_1")},
				},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key1": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// ("const_0" or "const_1") and (("const_1" or "const_2") or "const_0))
			name: "very complex and vol2",
			fields: fields{
				left: &Or{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_1")},
				},
				right: &Or{
					&Or{
						&Equal{execution.NewVariable("const_1")},
						&Equal{execution.NewVariable("const_2")},
					},
					&Equal{execution.NewVariable("const_0")},
				},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
					"const_2": octosql.MakeString("key2"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key0": nil,
					"key1": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &And{
				left:  tt.fields.left,
				right: tt.fields.right,
			}
			got, err := f.getAllKeys(ctx, tt.args.variables)
			if (err != nil) != tt.wantErr {
				t.Errorf("And.getAllKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("And.getAllKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOr_GetAllKeys(t *testing.T) {
	ctx := context.Background()
	type fields struct {
		left  KeyFormula
		right KeyFormula
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *redisKeys
		wantErr bool
	}{
		{
			// "const_0" or "const_0"
			name: "simple or dummy",
			fields: fields{
				left:  &Equal{execution.NewVariable("const_0")},
				right: &Equal{execution.NewVariable("const_0")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key0": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// "const_0" or "const_1"
			name: "simple or",
			fields: fields{
				left:  &Equal{execution.NewVariable("const_0")},
				right: &Equal{execution.NewVariable("const_1")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key0": nil,
					"key1": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// TRUE or "const_1"
			name: "simple or constant true",
			fields: fields{
				left:  &Constant{true},
				right: &Equal{execution.NewVariable("const_1")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{},
				True,
			},
			wantErr: false,
		},
		{
			// FALSE or "const_1"
			name: "simple or constant false",
			fields: fields{
				left:  &Constant{false},
				right: &Equal{execution.NewVariable("const_1")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key1": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// TRUE or TRUE
			name: "dumb true",
			fields: fields{
				left:  &Constant{true},
				right: &Constant{true},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{},
			},
			want: &redisKeys{
				map[string]interface{}{},
				True,
			},
			wantErr: false,
		},
		{
			// FALSE or FALSE
			name: "dumb false",
			fields: fields{
				left:  &Constant{false},
				right: &Constant{false},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{},
			},
			want: &redisKeys{
				map[string]interface{}{},
				False,
			},
			wantErr: false,
		},
		{
			// "const_0" or "const_1"
			name: "simple or no variables",
			fields: fields{
				left:  &Equal{execution.NewVariable("const_0")},
				right: &Equal{execution.NewVariable("const_1")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			// ("const_0" or "const_1") or ("const_2" or "const_1")
			name: "or inside or",
			fields: fields{
				left: &Or{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_1")},
				},
				right: &Equal{execution.NewVariable("const_3")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
					"const_3": octosql.MakeString("key3"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key0": nil,
					"key1": nil,
					"key3": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// ("const_0" or "const_1") or ("const_2" or "const_1")
			name: "complex or",
			fields: fields{
				left: &Or{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_1")},
				},
				right: &Or{
					&Equal{execution.NewVariable("const_2")},
					&Equal{execution.NewVariable("const_1")},
				},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
					"const_2": octosql.MakeString("key2"),
					"const_3": octosql.MakeString("key3"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key0": nil,
					"key1": nil,
					"key2": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// (TRUE and TRUE) or "const_1"
			name: "constant or and",
			fields: fields{
				left: &And{
					&Constant{true},
					&Constant{true},
				},
				right: &Equal{execution.NewVariable("const_1")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{},
				True,
			},
			wantErr: false,
		},
		{
			// ("const_0" and TRUE) or "const_1"
			name: "constant or and vol2",
			fields: fields{
				left: &And{
					&Equal{execution.NewVariable("const_0")},
					&Constant{true},
				},
				right: &Equal{execution.NewVariable("const_1")},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key0": nil,
					"key1": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// ("const_0" and "const_2") or ("const_2" or "const_1")
			name: "complex or vol2",
			fields: fields{
				left: &And{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_2")},
				},
				right: &Or{
					&Equal{execution.NewVariable("const_2")},
					&Equal{execution.NewVariable("const_1")},
				},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
					"const_2": octosql.MakeString("key2"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key1": nil,
					"key2": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			// ("const_0" and "const_1") or (("const_1" or "const_2") and "const_1"))
			name: "very complex or",
			fields: fields{
				left: &And{
					&Equal{execution.NewVariable("const_0")},
					&Equal{execution.NewVariable("const_1")},
				},
				right: &And{
					&Or{
						&Equal{execution.NewVariable("const_1")},
						&Equal{execution.NewVariable("const_2")},
					},
					&Equal{execution.NewVariable("const_1")},
				},
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
					"const_1": octosql.MakeString("key1"),
					"const_2": octosql.MakeString("key2"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key1": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Or{
				left:  tt.fields.left,
				right: tt.fields.right,
			}
			got, err := f.getAllKeys(ctx, tt.args.variables)
			if (err != nil) != tt.wantErr {
				t.Errorf("Or.getAllKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Or.getAllKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEqual_GetAllKeys(t *testing.T) {
	ctx := context.Background()
	type fields struct {
		child execution.Expression
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *redisKeys
		wantErr bool
	}{
		{
			name: "simple equal",
			fields: fields{
				child: execution.NewVariable("const_0"),
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key0": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			name: "simple equal no variable",
			fields: fields{
				child: execution.NewVariable("const_0"),
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_1": octosql.MakeString("key1"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "simple equal aliased expression",
			fields: fields{
				child: execution.NewAliasedExpression(
					"const_0",
					execution.NewVariable("const_1"),
				),
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_1": octosql.MakeString("key1"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key1": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Equal{
				child: tt.fields.child,
			}
			got, err := f.getAllKeys(ctx, tt.args.variables)
			if (err != nil) != tt.wantErr {
				t.Errorf("Equal.getAllKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Equal.getAllKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIn_GetAllKeys(t *testing.T) {
	ctx := context.Background()
	type fields struct {
		child execution.Expression
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *redisKeys
		wantErr bool
	}{
		{
			name: "simple in string",
			fields: fields{
				child: execution.NewVariable("const_0"),
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key0"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key0": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
		{
			name: "simple in tuple",
			fields: fields{
				child: execution.NewTuple([]execution.Expression{
					execution.NewVariable("const_0"),
					execution.NewVariable("const_1"),
					execution.NewVariable("const_2"),
				}),
			},
			args: args{
				map[octosql.VariableName]octosql.Value{
					"const_0": octosql.MakeString("key1"),
					"const_1": octosql.MakeString("key2"),
					"const_2": octosql.MakeString("key3"),
				},
			},
			want: &redisKeys{
				map[string]interface{}{
					"key1": nil,
					"key2": nil,
					"key3": nil,
				},
				DefaultKeys,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &In{
				child: tt.fields.child,
			}
			got, err := f.getAllKeys(ctx, tt.args.variables)
			if (err != nil) != tt.wantErr {
				t.Errorf("In.getAllKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("In.getAllKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConstant_getAllKeys(t *testing.T) {
	ctx := context.Background()
	type fields struct {
		value bool
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *redisKeys
		wantErr bool
	}{
		{
			name: "simple constant true",
			fields: fields{
				value: true,
			},
			args: args{
				map[octosql.VariableName]octosql.Value{},
			},
			want: &redisKeys{
				map[string]interface{}{},
				True,
			},
			wantErr: false,
		},
		{
			name: "simple constant false",
			fields: fields{
				value: false,
			},
			args: args{
				map[octosql.VariableName]octosql.Value{},
			},
			want: &redisKeys{
				map[string]interface{}{},
				False,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Constant{
				value: tt.fields.value,
			}
			got, err := f.getAllKeys(ctx, tt.args.variables)
			if (err != nil) != tt.wantErr {
				t.Errorf("Constant.getAllKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Constant.getAllKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}
