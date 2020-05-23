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
		[]string{"range", "tumble", "watermark_generator:_maximal_difference", "watermark_generator:_percentile"},
	))
	body = append(body, docs.Divider())

	tvfs := []docs.Documented{&tvf.Range{}, &tvf.Tumble{}, &tvf.WatermarkGenerator{}, &tvf.PercentileWatermarkGenerator{}}
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
					docs.Text("Example: SELECT * FROM range(range_start => 1, range_end => 5) r"),
				},
				body...,
			)...,
		),
	)

	docs.RenderDocumentation(page, os.Stdout)
}
