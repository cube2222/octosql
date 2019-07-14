package docs

import (
	"fmt"
	"io"
)

type Documented interface {
	Document() Documentation
}

func RenderDocumentation(d Documentation, w io.Writer) {
	d.render(w, 0, 0)
}

type Documentation interface {
	render(w io.Writer, headerLevel int, indentation int)
}

type body struct {
	elements []Documentation
}

func (d *body) render(w io.Writer, headerLevel int, indentation int) {
	for i, el := range d.elements {
		el.render(w, headerLevel, indentation)
		if i != len(d.elements)-1 {
			fmt.Fprint(w, "\n")
		}
	}
}

func Body(elements ...Documentation) *body {
	return &body{elements: elements}
}

type paragraph struct {
	elements []Documentation
}

func (d *paragraph) render(w io.Writer, headerLevel int, indentation int) {
	for _, el := range d.elements {
		el.render(w, headerLevel, indentation)
		fmt.Fprint(w, " ")
	}
}

func Paragraph(elements ...Documentation) *paragraph {
	return &paragraph{elements: elements}
}

type list struct {
	elements []Documentation
}

func (d *list) render(w io.Writer, headerLevel int, indentation int) {
	fmt.Fprint(w, "\n")
	for _, el := range d.elements {
		for i := 0; i < indentation; i++ {
			fmt.Fprint(w, "\t")
		}
		fmt.Fprint(w, "* ")
		el.render(w, headerLevel, indentation+1)
		fmt.Fprint(w, "\n")
	}
}

func List(elements ...Documentation) *list {
	return &list{elements: elements}
}

type section struct {
	header string
	body   Documentation
}

func (d *section) render(w io.Writer, headerLevel int, indentation int) {
	for i := 0; i <= headerLevel; i++ {
		fmt.Fprint(w, "#")
	}
	fmt.Fprintf(w, " %s\n", d.header)

	d.body.render(w, headerLevel+1, indentation)
}

func Section(header string, body Documentation) *section {
	return &section{header: header, body: body}
}

type text struct {
	text string
}

func (d *text) render(w io.Writer, headerLevel int, indentation int) {
	fmt.Fprintf(w, "%s", d.text)
}

func Text(txt string) *text {
	return &text{text: txt}
}

type divider struct {
}

func (d *divider) render(w io.Writer, headerLevel int, indentation int) {
	fmt.Fprint(w, "\n---\n")
}

func Divider() *divider {
	return &divider{}
}

func Ordinal(n int) string {
	switch n {
	case 1:
		return "1st"
	case 2:
		return "2nd"
	case 3:
		return "3rd"
	default:
		return fmt.Sprintf("%dth", n)
	}
}
