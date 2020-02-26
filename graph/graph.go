package graph

import (
	"fmt"
	"log"
	"strings"

	"github.com/awalterschulze/gographviz"
)

type Field struct {
	Name, Value string
}

type Child struct {
	Name string
	Node *Node
}

type Node struct {
	Name     string
	Fields   []Field
	Children []Child
}

func NewNode(name string) *Node {
	return &Node{
		Name: name,
	}
}

func (n *Node) AddField(name, value string) {
	n.Fields = append(n.Fields, Field{
		Name:  name,
		Value: value,
	})
}

func (n *Node) AddChild(name string, node *Node) {
	n.Children = append(n.Children, Child{
		Name: name,
		Node: node,
	})
}

type Visualizer interface {
	Visualize() *Node
}

func Show(node *Node) *gographviz.Graph {
	graph := gographviz.NewGraph()
	graph.Directed = true
	err := graph.AddAttr("", "rankdir", "LR")
	if err != nil {
		log.Fatal(err)
	}
	builder := &graphBuilder{
		graph:        graph,
		nameCounters: make(map[string]int),
	}

	getGraphNode(builder, node)

	return graph
}

type graphBuilder struct {
	graph        *gographviz.Graph
	nameCounters map[string]int
}

func (gb *graphBuilder) getID(name string) string {
	count := gb.nameCounters[name]
	gb.nameCounters[name]++
	return fmt.Sprintf("%s_%d", strings.Replace(name, " ", "_", -1), count)
}

func getGraphNode(gb *graphBuilder, node *Node) string {
	fields := make([]string, len(node.Fields))
	for i, field := range node.Fields {
		fields[i] = fmt.Sprintf("<%s> %s: %s", field.Name, field.Name, field.Value)
	}
	childPorts := make([]string, len(node.Children))
	for i, child := range node.Children {
		childPorts[i] = fmt.Sprintf("<%s> %s", child.Name, child.Name)
	}

	var labelParts []string
	labelParts = append(labelParts, fmt.Sprintf("<f0> %s", node.Name))

	if len(fields) > 0 {
		labelParts = append(labelParts, strings.Join(fields, "|"))
	}
	if len(childPorts) > 0 {
		labelParts = append(labelParts, strings.Join(childPorts, "|"))
	}

	label := fmt.Sprintf(
		"\"{{%s}}\"",
		strings.Join(labelParts, "}|{"),
	)

	id := gb.getID(node.Name)
	err := gb.graph.AddNode("", id, map[string]string{
		"shape": "record",
		"label": label,
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, child := range node.Children {
		childGraphNode := getGraphNode(gb, child.Node)
		err := gb.graph.AddPortEdge(id, child.Name, childGraphNode, "", true, map[string]string{})
		if err != nil {
			log.Fatal(err)
		}
	}
	return id
}
