package main

import (
	"os"

	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution/tvf"
)

func main() {
	var body []docs.Documentation

	tvfs := []docs.Documented{&tvf.Range{}}
	for i, el := range tvfs {
		body = append(body, el.Document())
		if i != len(tvfs)-1 {
			body = append(body, docs.Divider())
		}
	}

	page := docs.Section(
		"Table Valued Functions Documentation",
		docs.Body(body...),
	)

	docs.RenderDocumentation(page, os.Stdout)
}
