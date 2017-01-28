package dlock

import (
	"testing"
	"time"

	"github.com/jrivets/gorivets"
	"golang.org/x/net/context"
)

func TestStartStop(t *testing.T) {
	ms := NewMemStorage().(*mem_storage)
	dlm := NewDLockManager(ms, time.Millisecond).(*dlock_manager)
	dlm.Start()

	<-time.After(time.Millisecond)
	id := cKeyLockerId + dlm.lockerId

	r, err := ms.Get(context.TODO(), id)
	if err != nil || r == nil {
		t.Fatal("At this moment we have to have a record to keep the instance alive")
	}
	if r.Ttl > time.Millisecond || r.Ttl < 1 {
		t.Fatal("Wrong Ttl: ", r)
	}

	dlm.Shutdown()

	if dlm.state != ST_STOPPED {
		t.Fatal("dlm state shoule be ST_STOPPED")
	}
}

func TestSimpleLocking(t *testing.T) {
	ms := NewMemStorage().(*mem_storage)
	dlm := NewDLockManager(ms, time.Second)

	dlm.Start()
	lock := dlm.GetLocker("val1")
	lock2 := dlm.GetLocker("val2")

	_, err := ms.Get(context.TODO(), "val1")
	if !CheckError(err, DLErrNotFound) {
		t.Fatal("Should be no val1 record")
	}

	lock.Lock()
	_, err = ms.Get(context.TODO(), "val1")
	if err != nil {
		t.Fatal("Should be val1 record")
	}
	_, err = ms.Get(context.TODO(), "val2")
	if !CheckError(err, DLErrNotFound) {
		t.Fatal("Should be no val2 record")
	}

	lock2.Lock()
	_, err = ms.Get(context.TODO(), "val2")
	if err != nil {
		t.Fatal("Should be val2 record")
	}
	lock.Unlock()
	lock2.Unlock()

	_, err = ms.Get(context.TODO(), "val1")
	if !CheckError(err, DLErrNotFound) {
		t.Fatal("Should be no val1 record")
	}
	_, err = ms.Get(context.TODO(), "val2")
	if !CheckError(err, DLErrNotFound) {
		t.Fatal("Should be no val2 record")
	}

	dlm.Shutdown()
}

func TestRelockPanic(t *testing.T) {
	ms := NewMemStorage().(*mem_storage)
	dlm := NewDLockManager(ms, time.Second)

	dlm.Start()
	lock := dlm.GetLocker("val1")

	lock.Lock()
	if gorivets.CheckPanic(func() { lock.Lock() }) == nil {
		t.Fatal("Should panicing")
	}

	lock.Unlock()
	if gorivets.CheckPanic(func() { lock.Unlock() }) == nil {
		t.Fatal("Should panicing")
	}

	dlm.Shutdown()
}

func TestConcurrentLocking(t *testing.T) {
	ms := NewMemStorage().(*mem_storage)
	dlm := NewDLockManager(ms, time.Second)

	dlm.Start()
	lock1 := dlm.GetLocker("val")
	lock2 := dlm.GetLocker("val")

	lock1.Lock()
	start := time.Now()

	go func() {
		<-time.After(time.Millisecond * 50)
		lock1.Unlock()
	}()

	lock2.Lock()
	if time.Now().Sub(start) <= time.Millisecond*50 {
		t.Fatal("Something goes wrong should be blocked at least 50ms")
	}

	lock2.Unlock()

	dlm.Shutdown()
}

func TestPanickingLocking(t *testing.T) {
	ms := NewMemStorage().(*mem_storage)
	dlm := NewDLockManager(ms, time.Second).(*dlock_manager)

	dlm.Start()
	lock1 := dlm.GetLocker("val")
	lock2 := dlm.GetLocker("val")

	lock1.Lock()
	start := time.Now()

	go func() {
		<-time.After(time.Millisecond * 50)
		dlm.Shutdown()
	}()

	err := gorivets.CheckPanic(func() { lock2.Lock() })
	if time.Now().Sub(start) <= time.Millisecond*50 {
		t.Fatal("Something goes wrong should be blocked at least 50ms")
	}

	if !CheckError(err.(error), DLErrClosed) {
		t.Fatal("Something goes wrong the lock should be cancelled: ", err)
	}
	t.Logf("Got error: %s", err)
}

func TestConcurrentLockingDistr(t *testing.T) {
	ms := NewMemStorage().(*mem_storage)
	dlm1 := NewDLockManager(ms, time.Hour)
	dlm2 := NewDLockManager(ms, time.Hour)

	dlm1.Start()
	dlm2.Start()
	lock1 := dlm1.GetLocker("val")
	lock2 := dlm2.GetLocker("val")

	lock1.Lock()
	start := time.Now()

	go func() {
		<-time.After(time.Millisecond * 50)
		lock1.Unlock()
	}()

	lock2.Lock()
	if time.Now().Sub(start) <= time.Millisecond*50 {
		t.Fatal("Something goes wrong should be blocked at least 50ms")
	}

	lock2.Unlock()

	dlm1.Shutdown()
	dlm2.Shutdown()
}
