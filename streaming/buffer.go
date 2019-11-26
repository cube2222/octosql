package streaming

import (
	"context"
	"sync"
	"time"

	"github.com/cube2222/octosql/execution"
)

const retryPeriod = time.Second * 10

type Buffer struct {
	sync.Mutex
	done     bool
	records  []*execution.Record
	lastSend map[execution.ID]time.Time
	acked    map[execution.ID]bool
}

func NewBuffer() *Buffer {
	return &Buffer{
		Mutex:    sync.Mutex{},
		done:     false,
		records:  nil,
		lastSend: map[execution.ID]time.Time{},
		acked:    map[execution.ID]bool{},
	}
}

func (b *Buffer) Next(ctx context.Context) (*execution.Record, error) {
	b.Lock()
	defer b.Unlock()
	for {
		foundNotAcked := false
		for i := range b.records {
			if !b.acked[b.records[i].ID()] &&
				time.Now().Sub(b.lastSend[b.records[i].ID()]) > retryPeriod {
				b.lastSend[b.records[i].ID()] = time.Now()
				return b.records[i], nil
			}
			if !b.acked[b.records[i].ID()] {
				foundNotAcked = true
			}
		}
		if !foundNotAcked && b.done {
			return nil, execution.ErrEndOfStream
		}
		b.Unlock()
		time.Sleep(time.Millisecond * 100)
		b.Lock()
	}
}

func (b *Buffer) Close() error {
	return nil
}

func (b *Buffer) AddRecord(record *execution.Record, ack func()) error {
	b.Lock()
	defer b.Unlock()
	b.records = append(b.records, record)
	ack()
	return nil
}

func (b *Buffer) MarkEndOfStream() error {
	b.Lock()
	defer b.Unlock()
	b.done = true
	return nil
}

func (b *Buffer) Acknowledge(id execution.ID) {
	b.Lock()
	defer b.Unlock()
	b.acked[id] = true
}
