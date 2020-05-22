package docs

import (
	"fmt"
	"io"
)

type Documented interface {
	Document() Documentation
}

func RenderDocumentation(d Documentation, w io.Writer) {
	d.render(w, 0, 0, false)
}

type Documentation interface {
	render(w io.Writer, headerLevel int, indentation int, parentIsList bool)
}

type tableOfContents struct {
	names  []string
	hashes []string
}

func (d *tableOfContents) render(w io.Writer, headerLevel int, indentation int, parentIsList bool) {
	for i := 0; i <= headerLevel; i++ {
		fmt.Fprint(w, "#")
	}
	fmt.Fprintf(w, " Table of Contents\n")

	for i := range d.names {
		fmt.Fprintf(w, "* [%s]", d.names[i])
		fmt.Fprintf(w, "(#%s)", d.hashes[i])

		if i != len(d.names)-1 {
			fmt.Fprint(w, "\n")
		}
	}
}

func TableOfContents(names, hashes []string) *tableOfContents {
	return &tableOfContents{names: names, hashes: hashes}
}

type body struct {
	elements []Documentation
}

func (d *body) render(w io.Writer, headerLevel int, indentation int, parentIsList bool) {
	for i, el := range d.elements {
		el.render(w, headerLevel, indentation, false)
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

func (d *paragraph) render(w io.Writer, headerLevel int, indentation int, parentIsList bool) {
	for _, el := range d.elements {
		el.render(w, headerLevel, indentation, false)
		fmt.Fprint(w, " ")
	}
}

func Paragraph(elements ...Documentation) *paragraph {
	return &paragraph{elements: elements}
}

type list struct {
	elements []Documentation
}

func (d *list) render(w io.Writer, headerLevel int, indentation int, parentIsList bool) {
	fmt.Fprint(w, "\n")
	if !parentIsList {
		fmt.Fprint(w, "\n")
	}
	for _, el := range d.elements {
		for i := 0; i < indentation; i++ {
			fmt.Fprint(w, "\t")
		}
		fmt.Fprint(w, "* ")
		el.render(w, headerLevel, indentation+1, true)
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

func (d *section) render(w io.Writer, headerLevel int, indentation int, parentIsList bool) {
	for i := 0; i <= headerLevel; i++ {
		fmt.Fprint(w, "#")
	}
	fmt.Fprintf(w, " %s\n", d.header)

	d.body.render(w, headerLevel+1, indentation, false)
}

func Section(header string, body Documentation) *section {
	return &section{header: header, body: body}
}

type text struct {
	text string
}

func (d *text) render(w io.Writer, headerLevel int, indentation int, parentIsList bool) {
	fmt.Fprintf(w, "%s", d.text)
}

func Text(txt string) *text {
	return &text{text: txt}
}

type divider struct {
}

func (d *divider) render(w io.Writer, headerLevel int, indentation int, parentIsList bool) {
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
