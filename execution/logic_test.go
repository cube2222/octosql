package execution

import (
	"testing"

	"github.com/cube2222/octosql"
)

func TestAnd_Evaluate(t *testing.T) {
	type fields struct {
		Left  Formula
		Right Formula
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "false and false",
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			fields: fields{
				Left:  NewConstant(false),
				Right: NewConstant(false),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "false and true",
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			fields: fields{
				Left:  NewConstant(false),
				Right: NewConstant(true),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "true and false",
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			fields: fields{
				Left:  NewConstant(true),
				Right: NewConstant(false),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "true and true",
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			fields: fields{
				Left:  NewConstant(true),
				Right: NewConstant(true),
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &And{
				Left:  tt.fields.Left,
				Right: tt.fields.Right,
			}
			got, err := f.Evaluate(tt.args.variables)
			if (err != nil) != tt.wantErr {
				t.Errorf("And.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("And.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOr_Evaluate(t *testing.T) {
	type fields struct {
		Left  Formula
		Right Formula
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "false or false",
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			fields: fields{
				Left:  NewConstant(false),
				Right: NewConstant(false),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "false or true",
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			fields: fields{
				Left:  NewConstant(false),
				Right: NewConstant(true),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "true or false",
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			fields: fields{
				Left:  NewConstant(true),
				Right: NewConstant(false),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "true or true",
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			fields: fields{
				Left:  NewConstant(true),
				Right: NewConstant(true),
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Or{
				Left:  tt.fields.Left,
				Right: tt.fields.Right,
			}
			got, err := f.Evaluate(tt.args.variables)
			if (err != nil) != tt.wantErr {
				t.Errorf("Or.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Or.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNot_Evaluate(t *testing.T) {
	type fields struct {
		Child Formula
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "not false",
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			fields: fields{
				Child: NewConstant(false),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "not true",
			args: args{
				variables: map[octosql.VariableName]interface{}{},
			},
			fields: fields{
				Child: NewConstant(true),
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Not{
				Child: tt.fields.Child,
			}
			got, err := f.Evaluate(tt.args.variables)
			if (err != nil) != tt.wantErr {
				t.Errorf("Not.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Not.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}
