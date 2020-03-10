package execution

import "time"

// The batch size manager decides if a batch should take more records.
// It tries to satisfy the target latency and will try not to ever surpass it.
// It will also grow the batch size on successful commit by at least 1.
// In case the commit is too big to finalize, it will drastically reduce the batch size.
type BatchSizeManager struct {
	latencyTarget time.Duration
	lastCommit    time.Time
	curBatchSize  int
	batchSize     int
}

func NewBatchSizeManager(latencyTarget time.Duration) *BatchSizeManager {
	return &BatchSizeManager{
		latencyTarget: latencyTarget,
		lastCommit:    time.Now(),
		curBatchSize:  0,
		batchSize:     10,
	}
}

func (bsm *BatchSizeManager) CommitSuccessful() {
	delta := bsm.batchSize / 10
	if delta == 0 {
		delta = 1
	}
	if time.Since(bsm.lastCommit) > bsm.latencyTarget {
		delta *= -1
	}

	bsm.batchSize += delta

	bsm.curBatchSize = 0
	bsm.lastCommit = time.Now()
}

func (bsm *BatchSizeManager) CommitAborted() {
	bsm.curBatchSize = 0
	bsm.lastCommit = time.Now()
}

func (bsm *BatchSizeManager) CommitTooBig() {
	bsm.batchSize = bsm.batchSize / 2
	if bsm.batchSize == 0 {
		bsm.batchSize = 1
	}
	bsm.lastCommit = time.Now()
}

func (bsm *BatchSizeManager) ShouldTakeNextRecord() bool {
	if time.Since(bsm.lastCommit) > bsm.latencyTarget {
		bsm.batchSize = (bsm.curBatchSize * 9) / 10
		return false
	}
	if bsm.curBatchSize >= bsm.batchSize {
		return false
	}
	bsm.curBatchSize++
	return true
}

func (bsm *BatchSizeManager) Reset() {
	bsm.curBatchSize = 0
	bsm.lastCommit = time.Now()
}
