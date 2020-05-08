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
	// if we surpass the latency target we increase the batch size by 10%
	// otherwise we reduce it by 10%
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
	// if the commit was too big, we halve the batch size
	bsm.batchSize = bsm.batchSize / 2
	if bsm.batchSize == 0 {
		bsm.batchSize = 1
	}
	bsm.lastCommit = time.Now()
}

func (bsm *BatchSizeManager) ShouldTakeNextRecord() bool {
	if time.Since(bsm.lastCommit) > bsm.latencyTarget {
		// If we surpass the latency target during a batch,
		// then we set the batch size to 90% of the current batch size.
		bsm.batchSize = (bsm.curBatchSize * 9) / 10
		if bsm.batchSize == 0 {
			bsm.batchSize = 1
		}
		return false
	}
	if bsm.curBatchSize >= bsm.batchSize {
		return false
	}
	return true
}

// You can use this to process records in a way other than one by one.
func (bsm *BatchSizeManager) RecordsLeftToTake() int {
	return bsm.batchSize - bsm.curBatchSize
}

// You can use this to process records in a way other than one by one.
func (bsm *BatchSizeManager) MarkRecordsProcessed(count int) {
	bsm.curBatchSize += count
}

func (bsm *BatchSizeManager) Reset() {
	bsm.curBatchSize = 0
	bsm.lastCommit = time.Now()
}
