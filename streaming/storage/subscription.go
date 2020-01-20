package storage

import (
	"context"
	"errors"
	"sync"
)

// A Subscription lets the client listen for changes in the given prefixed storage.
// Subscriptions must be closed when not needed anymore.
type Subscription struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	changes <-chan struct{}
	errors  <-chan error
	closed  bool
}

type ChangesNotifierFunc func(ctx context.Context, changes chan<- struct{}) error

func NewSubscription(ctx context.Context, subscriptionFunc ChangesNotifierFunc) *Subscription {
	ctx, cancel := context.WithCancel(ctx)
	changes := make(chan struct{})
	errs := make(chan error, 1) // Buffered so there won't be a goroutine leak if nobody reads the error.
	sub := &Subscription{
		cancel:  cancel,
		changes: changes,
		errors:  errs,
		wg:      sync.WaitGroup{},
	}

	sub.wg.Add(1)
	// Here we start the goroutine that is the internal workhorse of this subscription.
	go func() {
		err := subscriptionFunc(ctx, changes)
		errs <- err
		sub.wg.Done()
	}()

	return sub
}

// ListenForChanges returns nil in case a change has been detected.
// Otherwise it returns an error containing the cause of the termination.
func (sub *Subscription) ListenForChanges(ctx context.Context) error {
	select {
	case <-sub.changes:
		return nil
	case err := <-sub.errors:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sub *Subscription) Close() error {
	if sub.closed {
		return errors.New("subscription already closed")
	}
	sub.closed = true
	sub.cancel()
	sub.wg.Wait()
	err := <-sub.errors
	if err == context.Canceled {
		return nil
	} else {
		return err
	}
}
