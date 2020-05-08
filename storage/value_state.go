package storage

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

type ValueState struct {
	tx StateTransaction
}

func NewValueState(tx StateTransaction) *ValueState {
	return &ValueState{
		tx: tx,
	}
}

func (vs *ValueState) Set(value proto.Message) error {
	byteValue, err := proto.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "couldn't marshal value")
	}

	return vs.setBytes(byteValue)
}

func (vs *ValueState) setBytes(b []byte) error {
	err := vs.tx.Set(nil, b)
	if err != nil {
		return errors.Wrap(err, "couldn't set value")
	}

	return nil
}

func (vs *ValueState) Get(value proto.Message) error {
	data, err := vs.tx.Get(nil)
	if err == ErrNotFound {
		return ErrNotFound
	} else if err != nil {
		return errors.Wrap(err, "an error occurred while reading the value")
	}

	err = proto.Unmarshal(data, value)
	return err
}

func (vs *ValueState) Clear() error {
	err := vs.tx.Delete(nil)
	if err != nil {
		return errors.Wrap(err, "couldn't clear value state")
	}

	return nil
}
