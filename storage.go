package uniquery

import "context"

type GetAll interface {
	GetAll(ctx context.Context, table string) (RecordGetter, error)
}

type GetWherePrimary interface {
	GetWherePrimary(ctx context.Context, table string, key interface{}) (RecordGetter, error)
}

type Sizer interface {
	Size() int64
}
