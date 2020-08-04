package main

import (
	"os"
	"sort"

	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution/aggregates"
)

func main() {
	var aggregateNames []string
	for k := range aggregates.AggregateTable {
		aggregateNames = append(aggregateNames, k)
	}
	sort.Strings(aggregateNames)

	aggregateDocs := make([]docs.Documentation, len(aggregateNames))
	for i, name := range aggregateNames {
		aggregateDocs[i] = aggregates.AggregateTable[name]().Document()
	}

	var body []docs.Documentation

	body = append(body, docs.TableOfContents(aggregateNames, aggregateNames))
	body = append(body, docs.Divider())

	for i := range aggregateDocs {
		body = append(body, aggregateDocs[i])
		if i != len(aggregateDocs)-1 {
			body = append(body, docs.Divider())
		}
	}

	page := docs.Section(
		"Aggregate Documentation",
		docs.Body(body...),
	)

	docs.RenderDocumentation(page, os.Stdout)
}
