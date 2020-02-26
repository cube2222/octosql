package graph

import (
	"os"
	"testing"
)

func TestShow(t *testing.T) {
	tree := &Node{
		Name: "root",
		Fields: map[string]string{
			"root": "true",
		},
		Children: map[string]*Node{
			"left": {
				Name: "mid",
				Fields: map[string]string{
					"lefty": "true",
				},
				Children: nil,
			},
			"right": {
				Name:   "mid",
				Fields: map[string]string{},
				Children: map[string]*Node{
					"child": {
						Name: "the child",
						Fields: map[string]string{
							"young": "true",
							"age":   "3",
						},
						Children: map[string]*Node{},
					},
				},
			},
		},
	}

	f, err := os.Create("output.viz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	g := Show(tree)
	f.WriteString(g.String())
}
