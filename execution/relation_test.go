package execution

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
)

func TestEqual_Apply(t *testing.T) {
	ctx := context.Background()
	type args struct {
		variables octosql.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple equal variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(3),
					"b": octosql.MakeInt(3),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple unequal variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(3),
					"b": octosql.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "simple incompatible variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(3),
					"b": octosql.MakeFloat(3.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &Equal{}
			got, err := rel.Apply(ctx, tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("Equal.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Equal.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNotEqual_Apply(t *testing.T) {
	ctx := context.Background()
	type args struct {
		variables octosql.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple equal variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(3),
					"b": octosql.MakeInt(3),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "simple unequal variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(3),
					"b": octosql.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple incompatible variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(3),
					"b": octosql.MakeFloat(3.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &NotEqual{}
			got, err := rel.Apply(ctx, tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("NotEqual.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("NotEqual.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMoreThan_Apply(t *testing.T) {
	ctx := context.Background()
	type args struct {
		variables octosql.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple greater than variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(4),
					"b": octosql.MakeInt(3),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple greater than variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeFloat(4.0),
					"b": octosql.MakeFloat(3.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple greater than variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("b"),
					"b": octosql.MakeString("a"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple greater than variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeTime(time.Date(2019, 03, 17, 16, 44, 16, 0, time.UTC)),
					"b": octosql.MakeTime(time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC)),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple incompatible variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(3),
					"b": octosql.MakeFloat(3.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &MoreThan{}
			got, err := rel.Apply(ctx, tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("MoreThan.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MoreThan.Apply() = %v, want %v", got, tt.want)
			}
			gotOpposite, err := rel.Apply(ctx, tt.args.variables, tt.args.right, tt.args.left)
			if (err != nil) != tt.wantErr {
				t.Errorf("MoreThan.Apply() opposite error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if gotOpposite != !tt.want {
				t.Errorf("MoreThan.Apply() opposite = %v, want %v", gotOpposite, tt.want)
			}
		})
	}
}

func TestLessThan_Apply(t *testing.T) {
	ctx := context.Background()
	type args struct {
		variables octosql.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple less than variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(3),
					"b": octosql.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple less than variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeFloat(3.0),
					"b": octosql.MakeFloat(4.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple less than variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("a"),
					"b": octosql.MakeString("b"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple less than variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeTime(time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC)),
					"b": octosql.MakeTime(time.Date(2019, 03, 17, 16, 44, 16, 0, time.UTC)),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple incompatible variable check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(3),
					"b": octosql.MakeFloat(3.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &LessThan{}
			got, err := rel.Apply(ctx, tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("LessThan.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LessThan.Apply() = %v, want %v", got, tt.want)
			}
			gotOpposite, err := rel.Apply(ctx, tt.args.variables, tt.args.right, tt.args.left)
			if (err != nil) != tt.wantErr {
				t.Errorf("MoreThan.Apply() opposite error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if gotOpposite != !tt.want {
				t.Errorf("MoreThan.Apply() opposite = %v, want %v", gotOpposite, tt.want)
			}
		})
	}
}

func TestGreaterEqual_Apply(t *testing.T) {
	ctx := context.Background()
	type args struct {
		variables octosql.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple >= integer variable check (>)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(5),
					"b": octosql.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple >= integer variable check (==)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(4),
					"b": octosql.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple >= integer variable check (<)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(4),
					"b": octosql.MakeInt(6),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},

		{
			name: "simple >= string variable check (>)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("baba"),
					"b": octosql.MakeString("baaa"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple >= string variable check (==)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("baba"),
					"b": octosql.MakeString("baba"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple >= string variable check (<)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("baba"),
					"b": octosql.MakeString("baca"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},

		{
			name: "simple incompatible variables check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeFloat(3.0),
					"b": octosql.MakeInt(3),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &GreaterEqual{}
			got, err := rel.Apply(ctx, tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("LessThan.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LessThan.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLessEqual_Apply(t *testing.T) {
	ctx := context.Background()
	type args struct {
		variables octosql.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		rel     *LessThan
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple <= integer variable check (>)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(5),
					"b": octosql.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},

		{
			name: "simple <= integer variable check (==)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(4),
					"b": octosql.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple <= integer variable check (<)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeInt(4),
					"b": octosql.MakeInt(6),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple <= string variable check (>)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("baba"),
					"b": octosql.MakeString("baaa"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},

		{
			name: "simple <= string variable check (==)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("baba"),
					"b": octosql.MakeString("baba"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple <= string variable check (<)",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("baba"),
					"b": octosql.MakeString("baca"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple incompatible variables check",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeFloat(3.0),
					"b": octosql.MakeInt(3),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &LessEqual{}
			got, err := rel.Apply(ctx, tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("LessThan.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LessThan.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLike_Apply(t *testing.T) {
	ctx := context.Background()
	type args struct {
		variables octosql.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		rel     *Like
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "test1 - usage of _",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("abcd"),
					"b": octosql.MakeString("a__d"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "test2 - usage of ?",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("abcdef"),
					"b": octosql.MakeString("a?f"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "test3 - usage of both",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("abcdef"),
					"b": octosql.MakeString("a_c?f"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "test4 - amount of dots doesn't match",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("abcd"),
					"b": octosql.MakeString("a.d"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},

		{
			name: "test5 - weird characters",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("a*{([])}$$bcd"),
					"b": octosql.MakeString("_*{([])}$$?"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "test6 - escaped characters",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("_?abc}"),
					"b": octosql.MakeString(`\_\??}`),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "test7 - illegal escape",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("_?abc}"),
					"b": octosql.MakeString(`\*\_\??}`),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},

		{
			name: "test8 - all of it",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("_?{}[]()+?:xd{}[]()?._:+"),
					"b": octosql.MakeString(`\_\?{}[]()+.:?`),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := tt.rel
			got, err := rel.Apply(ctx, tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("Like.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Like.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIn_Apply(t *testing.T) {
	ctx := context.Background()
	type args struct {
		variables octosql.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		rel     *In
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("123123"),
					"b": octosql.MakeTuple(
						[]octosql.Value{
							octosql.MakeString("123124"),
							octosql.MakeString("123123"),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("123123"),
					"b": octosql.MakeTuple(
						[]octosql.Value{
							octosql.MakeString("123124"),
							octosql.MakeString("123125"),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "record in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeTuple([]octosql.Value{
						octosql.MakeString("123124"),
						octosql.MakeInt(13),
					}),
					"b": octosql.MakeTuple(
						[]octosql.Value{
							octosql.MakeTuple([]octosql.Value{
								octosql.MakeString("123124"),
								octosql.MakeInt(13),
							}),
							octosql.MakeTuple([]octosql.Value{
								octosql.MakeString("123123"),
								octosql.MakeInt(15),
							}),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "record in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeTuple([]octosql.Value{
						octosql.MakeString("123124"),
						octosql.MakeInt(13),
					}),
					"b": octosql.MakeTuple(
						[]octosql.Value{
							octosql.MakeTuple([]octosql.Value{
								octosql.MakeString("123125"),
								octosql.MakeInt(13),
							}),
							octosql.MakeTuple([]octosql.Value{
								octosql.MakeString("123123"),
								octosql.MakeInt(15),
							}),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("123123"),
					"b": octosql.MakeTuple(
						[]octosql.Value{
							octosql.MakeString("123123"),
							octosql.MakeInt(13),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("123123"),
					"b": octosql.MakeString("123123"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := tt.rel
			got, err := rel.Apply(ctx, tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("In.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("In.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNotIn_Apply(t *testing.T) {
	ctx := context.Background()
	type args struct {
		variables octosql.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		rel     *NotIn
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("123123"),
					"b": octosql.MakeTuple(
						[]octosql.Value{
							octosql.MakeString("123124"),
							octosql.MakeString("123123"),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("123123"),
					"b": octosql.MakeTuple(
						[]octosql.Value{
							octosql.MakeString("123124"),
							octosql.MakeString("123125"),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "record in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeTuple([]octosql.Value{
						octosql.MakeString("123124"),
						octosql.MakeInt(13),
					}),
					"b": octosql.MakeTuple(
						[]octosql.Value{
							octosql.MakeTuple([]octosql.Value{
								octosql.MakeString("123124"),
								octosql.MakeInt(13),
							}),
							octosql.MakeTuple([]octosql.Value{
								octosql.MakeString("123123"),
								octosql.MakeInt(15),
							}),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "record in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeTuple([]octosql.Value{
						octosql.MakeString("123124"),
						octosql.MakeInt(13),
					}),
					"b": octosql.MakeTuple(
						[]octosql.Value{
							octosql.MakeTuple([]octosql.Value{
								octosql.MakeString("123125"),
								octosql.MakeInt(13),
							}),
							octosql.MakeTuple([]octosql.Value{
								octosql.MakeString("123123"),
								octosql.MakeInt(15),
							}),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("123123"),
					"b": octosql.MakeTuple(
						[]octosql.Value{
							octosql.MakeString("123123"),
							octosql.MakeInt(13),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("123123"),
					"b": octosql.MakeString("123123"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := tt.rel
			got, err := rel.Apply(ctx, tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("In.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("In.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegexp_Apply(t *testing.T) {
	ctx := context.Background()
	type args struct {
		variables octosql.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		rel     *Regexp
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple number regex",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("123123"),
					"b": octosql.MakeString("^[0-9]+$"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple number regex",
			args: args{
				variables: map[octosql.VariableName]octosql.Value{
					"a": octosql.MakeString("123123"),
					"b": octosql.MakeString("^[0-9]$"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := tt.rel
			got, err := rel.Apply(ctx, tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("Like.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Like.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}
