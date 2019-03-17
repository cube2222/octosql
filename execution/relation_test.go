package execution

import (
	"testing"
	"time"

	"github.com/cube2222/octosql"
)

func TestEqual_Apply(t *testing.T) {
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
				variables: map[octosql.VariableName]interface{}{
					"a": 3,
					"b": 3,
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
				variables: map[octosql.VariableName]interface{}{
					"a": 3,
					"b": 4,
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
				variables: map[octosql.VariableName]interface{}{
					"a": 3,
					"b": 3.0,
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
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
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
				variables: map[octosql.VariableName]interface{}{
					"a": 3,
					"b": 3,
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
				variables: map[octosql.VariableName]interface{}{
					"a": 3,
					"b": 4,
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
				variables: map[octosql.VariableName]interface{}{
					"a": 3,
					"b": 3.0,
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
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
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
	type args struct {
		variables octosql.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		rel     *MoreThan
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple greater than variable check",
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"a": 4,
					"b": 3,
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
				variables: map[octosql.VariableName]interface{}{
					"a": 4.0,
					"b": 3.0,
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
				variables: map[octosql.VariableName]interface{}{
					"a": "b",
					"b": "a",
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
				variables: map[octosql.VariableName]interface{}{
					"a": time.Date(2019, 03, 17, 16, 44, 16, 0, time.UTC),
					"b": time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC),
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
				variables: map[octosql.VariableName]interface{}{
					"a": 3,
					"b": 3.0,
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
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("MoreThan.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MoreThan.Apply() = %v, want %v", got, tt.want)
			}
			gotOpposite, err := rel.Apply(tt.args.variables, tt.args.right, tt.args.left)
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
			name: "simple less than variable check",
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"a": 3,
					"b": 4,
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
				variables: map[octosql.VariableName]interface{}{
					"a": 3.0,
					"b": 4.0,
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
				variables: map[octosql.VariableName]interface{}{
					"a": "a",
					"b": "b",
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
				variables: map[octosql.VariableName]interface{}{
					"a": time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC),
					"b": time.Date(2019, 03, 17, 16, 44, 16, 0, time.UTC),
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
				variables: map[octosql.VariableName]interface{}{
					"a": 3,
					"b": 3.0,
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
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("LessThan.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LessThan.Apply() = %v, want %v", got, tt.want)
			}
			gotOpposite, err := rel.Apply(tt.args.variables, tt.args.right, tt.args.left)
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

func TestLike_Apply(t *testing.T) {
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
			name: "simple number regex",
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"a": "123123",
					"b": "^[0-9]+$",
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
				variables: map[octosql.VariableName]interface{}{
					"a": "123123",
					"b": "^[0-9]$",
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
			rel := &Like{}
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
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
			name: "simple in",
			args: args{
				variables: map[octosql.VariableName]interface{}{
					"a": "123123",
					"b": []Record{
						*NewRecord([]octosql.VariableName{"a"}, map[octosql.VariableName]interface{}{
							"a": "123124",
						}),
						*NewRecord([]octosql.VariableName{"a"}, map[octosql.VariableName]interface{}{
							"a": "123123",
						}),
					},
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
				variables: map[octosql.VariableName]interface{}{
					"a": "123123",
					"b": []Record{
						*NewRecord([]octosql.VariableName{"a"}, map[octosql.VariableName]interface{}{
							"a": "123124",
						}),
						*NewRecord([]octosql.VariableName{"a"}, map[octosql.VariableName]interface{}{
							"a": "123125",
						}),
					},
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
				variables: map[octosql.VariableName]interface{}{
					"a": NewRecord([]octosql.VariableName{"a", "b"}, map[octosql.VariableName]interface{}{
						"a": "123124",
						"b": 13,
					}),
					"b": []Record{
						*NewRecord([]octosql.VariableName{"a", "b"}, map[octosql.VariableName]interface{}{
							"a": "123124",
							"b": 13,
						}),
						*NewRecord([]octosql.VariableName{"a", "b"}, map[octosql.VariableName]interface{}{
							"a": "123123",
							"b": 15,
						}),
					},
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
				variables: map[octosql.VariableName]interface{}{
					"a": NewRecord([]octosql.VariableName{"a", "b"}, map[octosql.VariableName]interface{}{
						"a": "123124",
						"b": 13,
					}),
					"b": []Record{
						*NewRecord([]octosql.VariableName{"a", "b"}, map[octosql.VariableName]interface{}{
							"a": "123125",
							"b": 13,
						}),
						*NewRecord([]octosql.VariableName{"a", "b"}, map[octosql.VariableName]interface{}{
							"a": "123123",
							"b": 15,
						}),
					},
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
				variables: map[octosql.VariableName]interface{}{
					"a": "123123",
					"b": []Record{
						*NewRecord([]octosql.VariableName{"a"}, map[octosql.VariableName]interface{}{
							"a": "123123",
						}),
					},
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
				variables: map[octosql.VariableName]interface{}{
					"a": "123123",
					"b": "123123",
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
			rel := &In{}
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
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
