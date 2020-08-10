package alpaca

import (
	"errors"
	"sync"
	"time"
)

type BtLimit struct {
	startTime time.Time
	capacity  int64
	incrRate  time.Duration
	atk       int64
	lock      sync.Mutex
	lastGpos  int64
}

func NewRateLimit(capacity int64) (*BtLimit, error) {

	if capacity <= 0 {
		return nil, errors.New("Invalid capacity")
	}

	return &BtLimit{
		startTime: time.Now(),
		capacity:  capacity,
		incrRate:  time.Second,
		atk:       capacity,
		lastGpos:  0,
	}, nil
}

func (b *BtLimit) getAvailableTks() bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.adjustAtks(b.getIncrPos(time.Now()))

	if b.atk <= 0 {
		return false
	}

	b.atk -= 1

	return true
}

func (b *BtLimit) getIncrPos(now time.Time) int64 {
	return int64(now.Sub(b.startTime) / b.incrRate)
}

func (b *BtLimit) adjustAtks(incrPos int64) {

	if b.atk >= b.capacity {
		return
	}

	b.atk += (incrPos - b.lastGpos) * b.capacity

	if b.atk >= b.capacity {
		b.atk = b.capacity
	}

	b.lastGpos = incrPos
}
func (b *BtLimit) Take() bool {
	return b.getAvailableTks()
}
