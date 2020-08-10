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

func NewTimeWheel(bucketNum int) (*TimeWheel, error) {

	if bucketNum <= 0 {
		return nil, errors.New("Invalid bukectNum")
	}

	tw := &TimeWheel{
		tdura:    time.Second,
		curPos:   0,
		bnum:     bucketNum,
		buckets:  make([]*bucket, bucketNum),
		stopChan: make(chan bool),
	}

	for i := 0; i < bucketNum; i++ {
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

	pos := (t.curPos + diff) % t.bnum

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
	t.curPos = (t.curPos + 1) % t.bnum
}
