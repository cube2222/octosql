package files

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

var previewedBuffer = &bytes.Buffer{}
var previewedBufferMutex sync.Mutex
var alreadyOpenedNoPreview int64

type stdinPreviewingReader struct {
}

func (r *stdinPreviewingReader) Read(p []byte) (n int, err error) {
	n, err = os.Stdin.Read(p)
	previewedBufferMutex.Lock()
	previewedBuffer.Write(p[:n])
	previewedBufferMutex.Unlock()
	return
}

func openStdin(preview bool) (io.Reader, error) {
	if preview {
		previewedPortionCopy := make([]byte, previewedBuffer.Len())
		copy(previewedPortionCopy, previewedBuffer.Bytes())
		return io.MultiReader(bytes.NewReader(previewedPortionCopy), &stdinPreviewingReader{}), nil
	}

	if atomic.AddInt64(&alreadyOpenedNoPreview, 1) > 1 {
		return nil, fmt.Errorf("stdin already opened")
	}

	previewedBufferBytes := previewedBuffer.Bytes()
	previewedBuffer = nil

	return io.MultiReader(bytes.NewReader(previewedBufferBytes), os.Stdin), nil
}
