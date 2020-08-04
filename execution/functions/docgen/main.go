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

	// These names have different hashes than name, so we need to create separate 'hashes' slice
	hashesMap := map[string]string{
		"*": "", // this is weird, unfortunately it's not working
		"+": "-1",
		"/": "-2",
	}

	hashes := make([]string, len(functionNames))
	functionDocs := make([]docs.Documentation, len(functionNames))
	for i, name := range functionNames {
		functionDocs[i] = functions.FunctionTable[name].Document()
		hashes[i] = name
		if hash, ok := hashesMap[name]; ok {
			hashes[i] = hash
		}
	}

	var body []docs.Documentation

	body = append(body, docs.TableOfContents(functionNames, hashes))
	body = append(body, docs.Divider())

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
