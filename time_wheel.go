package alpaca

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

type TimeWheel struct {
	ticker   *time.Ticker
	tdura    time.Duration
	buckets  []*bucket
	bnum     int
	curPos   int
	run      bool
	stopChan chan bool
}

type bucket struct {
	mglist *list.List
	mu     sync.Mutex
}

type Tmsg struct {
	ntu int
	pos int
	amg *AlpaceMsg
}

const (
	maxBuketSize = 1 << 20
)

func NewTimeWheel(bucketNum int32) (*TimeWheel, error) {

	if bucketNum <= 0 {
		return nil, errors.New("Invalid bukectNum")
	}

	bNum := vBsize(bucketNum)

	tw := &TimeWheel{
		tdura:    time.Second,
		curPos:   0,
		bnum:     bNum,
		buckets:  make([]*bucket, bNum),
		stopChan: make(chan bool),
	}

	for i := 0; i < bNum; i++ {
		tw.buckets[i] = &bucket{mglist: list.New()}
	}

	return tw, nil
}

func (b *bucket) del(e *list.Element) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mglist.Remove(e)
}

func (b *bucket) add(t *Tmsg) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mglist.PushBack(t)
}

func (b *bucket) first() *list.Element {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mglist.Front()
}

func (b *bucket) len() int {
	return b.mglist.Len()
}

func (t *TimeWheel) addAmsg(amg *AlpaceMsg) {

	if !t.run {
		return
	}

	pos, turns := t.getPnturns(time.Duration(amg.kmsg.Delay) * time.Second)

	tk := &Tmsg{
		pos: pos,
		ntu: turns,
		amg: amg,
	}
	t.buckets[pos].add(tk)
}

func (t *TimeWheel) gettk() *bucket {
	return t.buckets[t.cpos()]
}

func (t *TimeWheel) getPnturns(delay time.Duration) (int, int) {

	diff := int(delay / t.tdura)

	turns := int(diff / t.bnum)

	pos := (t.curPos + diff) & (t.bnum - 1)

	return pos, turns
}

func (t *TimeWheel) start() <-chan time.Time {
	t.ticker = time.NewTicker(t.tdura)
	t.run = true
	return t.ticker.C
}

func (t *TimeWheel) stop() {
	t.stopChan <- true
	t.run = false
}

func (t *TimeWheel) cpos() int {
	return t.curPos
}

func (t *TimeWheel) inrcPos() {
	t.curPos = (t.curPos + 1) & (t.bnum - 1)
}

func (t *TimeWheel) state() bool {
	return t.run
}

func (t *TimeWheel) getBuckets() []*bucket {
	return t.buckets
}

func vBsize(tbnum int32) int {
	u := tbnum - 1
	u |= u >> 1
	u |= u >> 2
	u |= u >> 4
	u |= u >> 8
	u |= u >> 16
	if int(u+1) > maxBuketSize {
		return maxBuketSize
	}
	return int(u + 1)
}
