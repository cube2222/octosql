package execution

import (
	"context"

	"github.com/pkg/errors"
)

var ErrNoMetarecordPending = errors.New("no metarecord pending")

type MetaRecordHandler interface {
	Poll(ctx context.Context) (*MetaRecord, error)
}

type PassthroughMetaRecordHandler struct {
	child MetaRecordHandler
}

func NewPassthroughMetaRecordHandler(child MetaRecordHandler) *PassthroughMetaRecordHandler {
	return &PassthroughMetaRecordHandler{
		child: child,
	}
}

func (h *PassthroughMetaRecordHandler) Poll(ctx context.Context) (*MetaRecord, error) {
	r, err := h.child.Poll(ctx)
	if err != nil {
		if err == ErrNoMetarecordPending {
			return nil, ErrNoMetarecordPending
		}
		return nil, errors.Wrap(err, "couldn't poll child for metarecord")
	}

	return r, nil
}

type ReleasableMetaRecordHandler struct {
	released bool
	child    MetaRecordHandler
}

func NewReleasableMetaRecordHandler(recordStream MetaRecordHandler) *ReleasableMetaRecordHandler {
	return &ReleasableMetaRecordHandler{
		released: false,
		child:    &PassthroughMetaRecordHandler{child: recordStream},
	}
}

func (h *ReleasableMetaRecordHandler) Poll(ctx context.Context) (*MetaRecord, error) {
	if !h.released {
		return nil, ErrNoMetarecordPending
	}

	r, err := h.child.Poll(ctx)
	if err != nil {
		if err == ErrNoMetarecordPending {
			return nil, ErrNoMetarecordPending
		}
		return nil, errors.Wrap(err, "couldn't poll child for metarecord")
	}

	return r, nil
}

func (h *ReleasableMetaRecordHandler) Release() {
	h.released = true
}
