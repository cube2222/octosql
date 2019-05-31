package execution

type InMemoryStream struct {
	data  []*Record
	index int
}

func NewInMemoryStream(data []*Record) *InMemoryStream {
	return &InMemoryStream{
		data:  data,
		index: 0,
	}
}

func (ims *InMemoryStream) Close() error {
	return nil
}

func (ims *InMemoryStream) Next() (*Record, error) {
	if ims.index >= len(ims.data) {
		return nil, ErrEndOfStream
	}

	recordToReturn := ims.data[ims.index]
	ims.index++

	return recordToReturn, nil
}
