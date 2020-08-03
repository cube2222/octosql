package main

import (
	"os"

	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution/tvf"
)

func main() {
	var body []docs.Documentation

	body = append(body, docs.TableOfContents(
		[]string{"range", "tumble", "watermark generator: maximal difference", "watermark generator: percentile"},
		[]string{"range", "tumble", "watermark-generator-maximal-difference", "watermark-generator-percentile"},
	))
	body = append(body, docs.Divider())

	tvfs := []docs.Documented{&tvf.Range{}, &tvf.Tumble{}, &tvf.MaximumDifferenceWatermarkGenerator{}, &tvf.PercentileWatermarkGenerator{}}
	for i, el := range tvfs {
		body = append(body, el.Document())
		if i != len(tvfs)-1 {
			body = append(body, docs.Divider())
		}
	}

	page := docs.Section(
		"Table Valued Functions Documentation",
		docs.Body(
			append(
				[]docs.Documentation{
					docs.Section("How to write particular arguments", docs.List(
						docs.Text("Underlying source => TABLE(\\<source\\>) (e.g. `TABLE(e)`)"),
						docs.Text("Time field => DESCRIPTOR(\\<time_field\\>) (e.g. `DESCRIPTOR(e.time)`)"),
						docs.Text("Expression: any other value like integer constant, interval => no changes (e.g. `5`, `INTERVAL 1 SECOND`)"))),
				},
				body...,
			)...,
		),
	)

	docs.RenderDocumentation(page, os.Stdout)
}
