package output

import (
	"io"

	"github.com/cube2222/octosql/execution"
)

type Output interface {
	WriteRecord(record *execution.Record) error
	io.Closer
}
