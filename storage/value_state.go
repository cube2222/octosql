package storage

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

var valueStates = sync.Map{}

type valueStateCell struct {
	sync.Mutex
	value []byte
}

type ValueState struct {
	valueStateCell *valueStateCell
}

func NewValueState(tx StateTransaction) *ValueState {
	return NewValueStateFromPrefix(tx.Prefix())
}

func NewValueStateFromPrefix(prefix string) *ValueState {
	newValueStateCell := &valueStateCell{}

	actualValueStateCell, _ := valueStates.LoadOrStore(prefix, newValueStateCell)

	return &ValueState{
		valueStateCell: actualValueStateCell.(*valueStateCell),
	}
}

func (vs *ValueState) Set(value proto.Message) error {
	byteValue, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't marshal value")
	}

	vs.valueStateCell.Lock()
	vs.valueStateCell.value = byteValue
	vs.valueStateCell.Unlock()

	return nil
}

func (vs *ValueState) Get(value proto.Message) error {
	vs.valueStateCell.Lock()
	byteValue := vs.valueStateCell.value
	vs.valueStateCell.Unlock()

	if byteValue == nil {
		return ErrNotFound
	}

	if err := proto.Unmarshal(byteValue, value); err != nil {
		return err
	}
	return nil
}

func (vs *ValueState) Clear() error {
	vs.valueStateCell.Lock()
	vs.valueStateCell.value = nil
	vs.valueStateCell.Unlock()

	return nil
}
