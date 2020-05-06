package output

import "context"

type Printer interface {
	Run(ctx context.Context) error
}
