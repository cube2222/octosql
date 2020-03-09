package execution

import "time"

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
	var delta int
	delta = bsm.batchSize / 10
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
		bsm.batchSize += 1
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
