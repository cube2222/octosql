package tmp_storage

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

type AbstractKey = octosql.Tuple
type AbstractValue = interface{} // TODO CO TO MA BYC
type State = octosql.Tuple

type DomainOperation interface {
	DomainOperation()
}

type SetState struct {
	key      AbstractKey
	newState State
}

func (SetState) DomainOperation() {}

type AddOutgoingRecord struct {
	rec *execution.Record
}

func (AddOutgoingRecord) DomainOperation() {}

type DomainInterface interface {
	GetState(key AbstractKey) (State, bool) // (value, AlreadyFired)
	Transaction([]DomainOperation) error
	Iterate() // ???
}

type InternalOperation interface {
	InternalOperation()
}

type SetKey struct {
	key   AbstractKey
	value AbstractValue
}

func (SetKey) InternalOperation() {}

type DeleteKey struct {
	key AbstractKey
}

func (DeleteKey) InternalOperation() {}

type LowLevelStorage interface {
	GetKey(key AbstractKey) (AbstractValue, error)
	Transaction([]InternalOperation)
	Iterate() // ???
}

type Interpreter interface {
	Interpret([]DomainOperation) ([]InternalOperation, error)
}
