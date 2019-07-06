package grouping

import (
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

var ErrNotFound = errors.New("group not found")

type Grouping struct {
	groups *execution.HashMap
}

func NewGrouping() *Grouping {
	return &Grouping{
		groups: execution.NewHashMap(),
	}
}

type entry struct {
	key    []interface{}
	values []interface{}
}

func (g *Grouping) AddElement(key []interface{}, value interface{}) error {
	group, ok, err := g.groups.Get(key)
	if err != nil {
		return errors.Wrap(err, "couldn't get group out of hashmap")
	}

	var newGroup []interface{}
	if ok {
		newGroup = append(group.([]interface{}), value)
	} else {
		newGroup = []interface{}{value}
	}

	err = g.groups.Set(key, newGroup)
	if err != nil {
		return errors.Wrap(err, "couldn't put group into hashmap")
	}

	return nil
}

func (g *Grouping) GetGroup(key []interface{}) ([]interface{}, error) {
	group, ok, err := g.groups.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get group out of hashmap")
	}

	if !ok {
		return nil, ErrNotFound
	}

	return group.([]interface{}), nil
}
