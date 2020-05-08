package storage

import (
	"context"
	"reflect"
	"sync"

	"github.com/pkg/errors"
)

// A Subscription lets the client listen for changes in the given prefixed storage.
// Subscriptions must be closed when not needed anymore.
type Subscription struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	changes <-chan struct{}
	errors  <-chan error
	closed  bool
	err     error
}

type ChangesNotifierFunc func(ctx context.Context, changes chan<- struct{}) error

func NewSubscription(ctx context.Context, subscriptionFunc ChangesNotifierFunc) *Subscription {
	ctx, cancel := context.WithCancel(ctx)
	changes := make(chan struct{}, 1) // TODO: Buffered to fix stalling issue
	errs := make(chan error, 1)       // Buffered so there won't be a goroutine leak if nobody reads the error.
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
		sub.closed = true
		sub.err = err
		if err == ErrChangeSent {
			return nil
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

var ErrChangeSent = errors.New("change sent")

func (sub *Subscription) Close() error {
	var err error
	if sub.closed {
		err = sub.err
	} else {
		sub.cancel()
		sub.wg.Wait()
		err = <-sub.errors

		sub.closed = true
		sub.err = err
	}

	if err == context.Canceled {
		return nil
	} else if err == ErrChangeSent {
		return nil
	} else {
		return err
	}
}

func ConcatSubscriptions(ctx context.Context, subs ...*Subscription) *Subscription {
	count := len(subs)

	// We create a slice of channels
	// First n positions are change channels
	// Next n positions are error channels
	// Next 1 position will be a ctx.Done channel
	channels := make([]reflect.SelectCase, count*2+1)
	for i := range subs {
		channels[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(subs[i].changes)}
	}
	for i := range subs {
		channels[count+i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(subs[i].errors)}
	}
	return NewSubscription(ctx, func(ctx context.Context, changes chan<- struct{}) error {
		channels[len(channels)-1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}
		var closedErr error
	loop:
		for {
			chosen, recv, _ := reflect.Select(channels)
			if chosen == len(channels)-1 { // ctx.Done
				closedErr = ctx.Err()
				break
			} else if chosen >= count { // error channel
				if err, ok := recv.Interface().(error); ok {
					return err
				} else {
					return errors.Errorf("unknown value in error receive, wanted error: %+v", err)
				}
			} else { // change channel
				select {
				case changes <- struct{}{}:
				case <-ctx.Done():
					closedErr = ctx.Err()
					break loop
				}
			}
		}

		// We have to close all underlying subscriptions when closing.
		for i := range subs {
			err := subs[i].Close()
			if err != nil {
				return errors.Wrapf(err, "couldn't close subscription with index %d", i)
			}
		}

		return closedErr
	})
}
