package main

import (
	"os"
	"sort"

	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution/functions"
)

func main() {
	var functionNames []string
	for k := range functions.FunctionTable {
		functionNames = append(functionNames, k)
	}
	sort.Strings(functionNames)

	functionDocs := make([]docs.Documentation, len(functionNames))
	for i, name := range functionNames {
		functionDocs[i] = functions.FunctionTable[name].Document()
	}

	var body []docs.Documentation
	for i := range functionDocs {
		body = append(body, functionDocs[i])
		if i != len(functionDocs)-1 {
			body = append(body, docs.Divider())
		}
	}

	page := docs.Section(
		"Function Documentation",
		docs.Body(body...),
	)

	docs.RenderDocumentation(page, os.Stdout)
}
