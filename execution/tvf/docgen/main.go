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

	tvfs := []docs.Documented{&tvf.Range{}, &tvf.Tumble{}, &tvf.WatermarkGenerator{}, &tvf.PercentileWatermarkGenerator{}}
	for i, el := range tvfs {
		body = append(body, el.Document())
		if i != len(tvfs)-1 {
			body = append(body, docs.Divider())
		}
	}

	/*
		WITH
		                                with_watermark AS (SELECT *
		                                                   FROM max_diff_watermark(source=>TABLE(events2),
		                                                                           offset=>INTERVAL 5 SECONDS,
		                                                                           time_field=>DESCRIPTOR(time)) e),
		                                with_tumble AS (SELECT *
		                                                FROM tumble(source=>TABLE(with_watermark),
		                                                            time_field=>DESCRIPTOR(e.time),
		                                                            window_length=> INTERVAL 1 MINUTE,
		                                                            offset => INTERVAL 0 SECONDS) e),
		                              SELECT e.window_end, e.team, COUNT(*) as goals
		                              FROM with_tumble e
		                              GROUP BY e.window_end, e.team
		                              TRIGGER COUNTING 100, ON WATERMARK
	*/

	page := docs.Section(
		"Table Valued Functions Documentation",
		docs.Body(
			append(
				[]docs.Documentation{
					docs.Section("How to write particular arguments", docs.List(
						docs.Text("Underlying source => TABLE(\\<source\\>) (e.g. `TABLE(e)`)"),
						docs.Text("Time field => DESCRIPTION(\\<time_field\\>) (e.g. `DESCRIPTOR(e.time)`)"),
						docs.Text("Expression: any other value like integer constant, interval => no changes (e.g. `5`, `INTERVAL 1 SECOND`)"))),
				},
				body...,
			)...,
		),
	)

	docs.RenderDocumentation(page, os.Stdout)
}
