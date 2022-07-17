package files

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/nxadm/tail"
)

type customCloser struct {
	io.Reader
	close func() error
}

func (c *customCloser) Close() error {
	return c.close()
}

func Tail(ctx context.Context, path string) (io.ReadCloser, error) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	t, err := tail.TailFile(path, tail.Config{
		MustExist: true,
		Follow:    true,
		ReOpen:    true,
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't tail file: %w", err)
	}

	pr, pw := io.Pipe()

	go func() {
		defer wg.Done()
	loop:
		for {
			select {
			case line := <-t.Lines:
				if line == nil {
					t.Stop()
					t.Cleanup()
					pw.Close()
					break loop
				} else if line.Err != nil {
					pw.CloseWithError(fmt.Errorf("couldn't read line: %w", line.Err))
					t.Cleanup()
					break loop
				}
				pw.Write([]byte(line.Text + "\n"))
			case <-ctx.Done():
				t.Stop()
				t.Cleanup()
				pw.Close()
				break loop
			}
		}
	}()

	return &customCloser{
		Reader: pr,
		close: func() error {
			pr.Close()
			t.Kill(nil)
			wg.Wait()
			return nil
		},
	}, nil
}
