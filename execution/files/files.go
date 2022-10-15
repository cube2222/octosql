package files

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/nxadm/tail"

	"github.com/cube2222/octosql/config"
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

type openFileOptions struct {
	tail    bool
	preview bool
}

type OpenFileOption func(*openFileOptions)

// WithPreview means we're opening to preview the file, but we'll still need that portion later, when opening it again.
func WithPreview() OpenFileOption {
	return func(options *openFileOptions) {
		options.preview = true
	}
}

func WithTail(tail bool) OpenFileOption {
	return func(options *openFileOptions) {
		options.tail = tail
	}
}

func OpenLocalFile(ctx context.Context, path string, opts ...OpenFileOption) (io.ReadCloser, error) {
	openFileOpts := &openFileOptions{
		tail: false,
	}
	for _, opt := range opts {
		opt(openFileOpts)
	}

	if path == "stdin" || strings.HasPrefix(path, "stdin.") {
		f, err := openStdin(openFileOpts.preview)
		if err != nil {
			return nil, fmt.Errorf("couldn't open stdin: %w", err)
		}
		return f, nil
	} else if !openFileOpts.tail {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("couldn't open file: %w", err)
		}
		return &customCloser{
			Reader: bufio.NewReaderSize(f, config.FromContext(ctx).Files.BufferSizeBytes),
			close:  f.Close,
		}, nil
	} else {
		r, err := Tail(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("couldn't tail file: %w", err)
		}
		return r, nil
	}
}
