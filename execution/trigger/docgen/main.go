package main

import (
	"os"
	"time"

	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution/trigger"
)

func main() {
	var body []docs.Documentation

	body = append(body, docs.TableOfContents(
		[]string{"counting trigger", "delay trigger", "multitrigger", "watermark trigger"},
		[]string{"counting-trigger", "delay-trigger", "multitrigger", "watermark-trigger"},
	))
	body = append(body, docs.Divider())

	triggers := []docs.Documented{trigger.NewCountingTrigger(0), trigger.NewDelayTrigger(0, func() time.Time { return time.Time{} }), trigger.NewMultiTrigger(), trigger.NewWatermarkTrigger()}
	for i, el := range triggers {
		body = append(body, el.Document())
		if i != len(triggers)-1 {
			body = append(body, docs.Divider())
		}
	}

	page := docs.Section(
		"Trigger Documentation",
		docs.Body(body...),
	)

	docs.RenderDocumentation(page, os.Stdout)
}
