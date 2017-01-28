package dlock

import (
	"sync"
	"time"

	"golang.org/x/net/context"
)

type ms_record struct {
	key        string
	value      string
	version    int
	changeTime time.Time
	ttl        time.Duration
	waitChs    []chan bool
}

type mem_storage struct {
	data      map[string]*ms_record
	lock      sync.Mutex
	closed    bool
	sweepTime time.Time
	sweepCh   chan bool
}

var cMaxTime = time.Unix(1<<59-1, 0)

func NewMemStorage() Storage {
	ms := &mem_storage{data: make(map[string]*ms_record), sweepCh: make(chan bool, 1)}

	go func() {
		for !ms.closed {
			sleepTimeout := ms.sweep()
			if sleepTimeout < 1 {
				sleepTimeout = 10
			}
			if sleepTimeout > time.Minute {
				sleepTimeout = time.Minute
			}

			select {
			case _, ok := <-ms.sweepCh:
				if !ok {
					return
				}
			case <-time.After(sleepTimeout):
			}
		}
	}()

	return ms
}

func (ms *mem_storage) sweep() time.Duration {
	now := time.Now()

	// if ms.sweepTime == cMaxTime OR it is before now, we will walk through
	// the list, otherwise go out of here...
	if cMaxTime.After(ms.sweepTime) && now.Before(ms.sweepTime) {
		return ms.sweepTime.Sub(now)
	}

	ms.lock.Lock()
	defer ms.lock.Unlock()

	if ms.closed {
		return 0
	}

	minTime := cMaxTime
	for k, msr := range ms.data {
		if msr.ttl == 0 {
			continue
		}

		expTime := msr.changeTime.Add(msr.ttl)

		if expTime.Before(now) {
			delete(ms.data, k)
			continue
		}

		if expTime.Before(minTime) {
			minTime = expTime
		}
	}

	ms.sweepTime = minTime
	return ms.sweepTime.Sub(now)
}

func (ms *mem_storage) Create(ctx context.Context, record *Record) (*Record, error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	if ms.closed {
		return nil, Error(DLErrClosed)
	}

	r, ok := ms.data[record.Key]
	if ok {
		return toRecord(r), Error(DLErrAlreadyExists)
	}

	r = to_ms_record(record)
	r.version = 1
	ms.saveRecord(r)
	return toRecord(r), nil
}

func (ms *mem_storage) saveRecord(msr *ms_record) {
	ms.data[msr.key] = msr

	expTime := msr.getExpTime()
	if ms.sweepTime.After(expTime) || ms.sweepTime.IsZero() {
		ms.sweepTime = expTime

		// Signal if not full yet...
		select {
		case ms.sweepCh <- true:
		default:
		}
	}
}

func (ms *mem_storage) Get(ctx context.Context, key string) (*Record, error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	if ms.closed {
		return nil, Error(DLErrClosed)
	}

	r, ok := ms.data[key]
	if !ok {
		return nil, Error(DLErrNotFound)
	}

	return toRecord(r), nil
}

func (ms *mem_storage) CasByVersion(ctx context.Context, record *Record) (*Record, error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	if ms.closed {
		return nil, Error(DLErrClosed)
	}

	r, ok := ms.data[record.Key]
	if !ok {
		return nil, Error(DLErrNotFound)
	}

	if r.version != record.Version {
		return toRecord(r), Error(DLErrWrongVersion)
	}

	r1 := to_ms_record(record)
	r1.version = r.version + 1
	ms.saveRecord(r1)
	r.notifyChans()
	return toRecord(r1), nil
}

func (ms *mem_storage) Delete(ctx context.Context, record *Record) (*Record, error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	if ms.closed {
		return nil, Error(DLErrClosed)
	}

	r, ok := ms.data[record.Key]
	if !ok {
		return nil, Error(DLErrNotFound)
	}

	if r.version != record.Version {
		return toRecord(r), Error(DLErrWrongVersion)
	}

	delete(ms.data, record.Key)

	r.notifyChans()
	return nil, nil
}

func (ms *mem_storage) WaitVersionChange(ctx context.Context, key string, version int, timeout time.Duration) (*Record, error) {
	ch, err := ms.newChan(key, version)
	if err != nil {
		r, _ := ms.Get(ctx, key)
		return r, err
	}

	// will do this after ms.dropChan() (see below), what guarantees no write after the call
	defer close(ch)

	err = nil
	select {
	case <-ctx.Done():
		err = DLErrClosed
	case <-ch:
	case <-time.After(timeout):
	}
	ms.dropChan(key, ch)

	if err != nil {
		return nil, err
	}

	return ms.Get(ctx, key)
}

func (ms *mem_storage) Close() {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	if ms.closed {
		return
	}

	for _, msr := range ms.data {
		msr.notifyChans()
	}

	ms.data = make(map[string]*ms_record)
	ms.sweepTime = cMaxTime

	close(ms.sweepCh)
	ms.closed = true
}

func (ms *mem_storage) newChan(key string, version int) (chan bool, error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	if ms.closed {
		return nil, Error(DLErrClosed)
	}

	r, ok := ms.data[key]
	if !ok {
		return nil, Error(DLErrNotFound)
	}

	if r.version != version {
		return nil, Error(DLErrWrongVersion)
	}

	ch := make(chan bool, 1)
	r.waitChs = append(r.waitChs, ch)
	return ch, nil
}

func (ms *mem_storage) dropChan(key string, ch chan bool) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	msr, _ := ms.data[key]
	if msr == nil {
		return
	}

	for i, c := range msr.waitChs {
		if c == ch {
			l := len(msr.waitChs)
			msr.waitChs[l-1], msr.waitChs[i] = msr.waitChs[i], msr.waitChs[l-1]
			msr.waitChs = msr.waitChs[:l-1]
			return
		}
	}
}

func (msr *ms_record) notifyChans() {
	for _, ch := range msr.waitChs {
		ch <- true
	}
	msr.waitChs = make([]chan bool, 0)
}

func (msr *ms_record) getExpTime() time.Time {
	if msr.ttl == 0 {
		return time.Unix(1<<60-1, 0)
	}
	return msr.changeTime.Add(msr.ttl)
}

func toRecord(r *ms_record) *Record {
	ttl := r.ttl - time.Now().Sub(r.changeTime)
	if r.ttl == 0 {
		ttl = 0
	} else if ttl <= 0 {
		// We will not provide 0 or a value <0 cause 0 means infinite, but
		// r.ttl > 0 means that we have a timeout...
		ttl = 1
	}
	return &Record{Key: r.key, Value: r.value, Version: r.version, Ttl: ttl}
}

func to_ms_record(r *Record) *ms_record {
	var ttl time.Duration = 0
	if r.Ttl > 0 {
		ttl = r.Ttl
	}
	return &ms_record{r.Key, r.Value, r.Version, time.Now(),
		ttl, make([]chan bool, 0)}
}
