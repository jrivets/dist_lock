package dlock

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/satori/go.uuid"
	"golang.org/x/net/context"
)

type (
	Record struct {
		Key     string
		Value   string
		Version int

		// Time To live in nanoseconds. This value indicates how long the record
		// is going to be kept in the storage. Value 0 shows that Ttl is not
		// limited
		Ttl time.Duration
	}

	Storage interface {
		// Creates a new one, or returns existing one with DLErrAlreadyExists
		// error
		Create(ctx context.Context, record *Record) (*Record, error)

		// Retrieves the record by its key. It will return nil and an error,
		// which will indicate the reason why the operation was not succesful.
		Get(ctx context.Context, key string) (*Record, error)

		// Compare and set the record value if the record stored version is
		// same as in the provided record. The record version will be updated
		// too.
		//
		// Returns updated stored record or stored record and error if the
		// operation was not successful
		CasByVersion(ctx context.Context, record *Record) (*Record, error)

		// Tries to delete the record. Operation can fail if stored record
		// version is different than existing one. This case the stored version
		// is returned as well as the appropriate error. If the record is deleted
		// both results are nil(!)
		Delete(ctx context.Context, record *Record) (*Record, error)

		// Waits for the record version change. The version param contans an
		// expected version.
		WaitVersionChange(ctx context.Context, key string, version int, timeout time.Duration) (*Record, error)
	}

	Error int

	DLockManager interface {
		// Starts the manager using the provided context. The DLockeManager
		// will use the context for creating Lockers and
		Start()
		GetLocker(name string) sync.Locker
		GetLockerWithCtx(ctx context.Context, name string) sync.Locker
		Shutdown()
	}
)

const (
	// Errors
	DLErrAlreadyExists Error = 1
	DLErrNotFound      Error = 2
	DLErrWrongVersion  Error = 3
	DLErrClosed        Error = 4
)

const (
	// dlock_manager states
	ST_STARTING = iota + 1
	ST_STARTED
	ST_STOPPED
)

const (
	// Reserved keys
	cKeyLockerId = "__locker_id__"

	// Default config settings
	cKeepAliveSec = 30
)

func CheckError(e error, expErr Error) bool {
	if e == nil {
		return false
	}
	err, ok := e.(Error)
	if !ok {
		return false
	}
	return err == expErr
}

func (e Error) Error() string {
	switch e {
	case DLErrAlreadyExists:
		return "Record with the key already exists"
	case DLErrNotFound:
		return "Record with the key is not found"
	case DLErrWrongVersion:
		return "Unexpected record version"
	case DLErrClosed:
		return "The Distributed Lock Manager is already closed."
	}
	return ""
}

func NewDLockManager(storage Storage, keepAlive time.Duration) DLockManager {
	dlm := &dlock_manager{storage: storage, state: ST_STARTING}
	dlm.cfgKeepAlive = keepAlive
	dlm.lockerId = uuid.NewV4().String()
	dlm.llocks = make(map[string]*local_lock)
	return dlm
}

// ================================ Details ===================================
type dlock struct {
	name   string
	dlm    *dlock_manager
	lock   sync.Mutex
	locked bool
	ctx    context.Context
}

func (dl *dlock) Lock() {
	dl.lock.Lock()
	defer dl.lock.Unlock()

	if dl.locked {
		panic("Incorrect distributed lock usage: already locked.")
	}

	dl.dlm.lockGlobal(dl.ctx, dl.name)
	dl.locked = true
}

func (dl *dlock) Unlock() {
	dl.lock.Lock()
	defer dl.lock.Unlock()

	if !dl.locked {
		panic("Incorrect distributed lock usage: An attempt to unlock not-locked lock")
	}

	dl.dlm.unlockGlobal(dl.ctx, dl.name)
	dl.locked = false
}

type dlock_manager struct {
	storage   Storage
	lock      sync.Mutex
	state     int
	lockerId  string
	llocks    map[string]*local_lock
	shtdwnCh  chan bool
	ctx       context.Context
	ctxCancel context.CancelFunc

	//Config settings

	// Sets the instance storage record keep-alive timeout
	cfgKeepAlive time.Duration
}

type local_lock struct {
	sigChannel chan bool
	counter    int
}

func (dlm *dlock_manager) Start() {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	if dlm.state != ST_STARTING {
		panic(errors.New("Wrong dlock_manager state=" + strconv.Itoa(dlm.state)))
	}

	dlm.ctx, dlm.ctxCancel = context.WithCancel(context.Background())

	dlm.state = ST_STARTED
	dlm.keepAlive()
	dlm.shtdwnCh = make(chan bool)

	go func() {
		for {
			select {
			case <-dlm.shtdwnCh:
			case <-time.After(dlm.cfgKeepAlive / 2):
				dlm.keepAlive()
			}
		}
	}()
}

func (dlm *dlock_manager) GetLocker(name string) sync.Locker {
	return dlm.GetLockerWithCtx(dlm.ctx, name)
}

func (dlm *dlock_manager) GetLockerWithCtx(ctx context.Context, name string) sync.Locker {
	if strings.HasPrefix(name, cKeyLockerId) {
		panic("Wrong name. Prefix=" + cKeyLockerId + " is reserved")
	}

	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	if dlm.state != ST_STARTED {
		panic(errors.New("Wrong dlock_manager state=" + strconv.Itoa(dlm.state)))
	}

	return &dlock{name: name, dlm: dlm, ctx: ctx}
}

func (dlm *dlock_manager) Shutdown() {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	if dlm.state != ST_STARTED {
		panic(errors.New("Wrong dlock_manager state=" + strconv.Itoa(dlm.state)))
	}

	// Lets all waiters know about finishing the game
	for _, ll := range dlm.llocks {
		close(ll.sigChannel)
	}

	dlm.llocks = make(map[string]*local_lock)

	dlm.state = ST_STOPPED
	close(dlm.shtdwnCh)

	r, _ := dlm.storage.Get(dlm.ctx, cKeyLockerId+dlm.lockerId)
	if r != nil {
		dlm.storage.Delete(dlm.ctx, r)
	}
	dlm.ctxCancel()
}

func (dlm *dlock_manager) keepAlive() {

	for {
		r := &Record{cKeyLockerId + dlm.lockerId, "alive", 0, dlm.cfgKeepAlive}
		r, err := dlm.storage.Create(dlm.ctx, r)
		if err == nil {
			return
		}

		if CheckError(err, DLErrAlreadyExists) {
			for {
				r.Ttl = dlm.cfgKeepAlive
				r, err = dlm.storage.CasByVersion(dlm.ctx, r)

				if err == nil {
					return
				}

				if CheckError(err, DLErrNotFound) {
					break
				}

				if CheckError(err, DLErrWrongVersion) {
					panic(err)
				}
			}
		}
	}
}

func (dlm *dlock_manager) lockGlobal(ctx context.Context, name string) {
	err := dlm.lockLocal(ctx, name)
	if err != nil {
		panic(err)
	}

	for {
		r, err := dlm.storage.Create(ctx, &Record{name, dlm.lockerId, 0, 0})

		if err == nil {
			return
		}

		if !CheckError(err, DLErrAlreadyExists) {
			dlm.unlockLocal(name)
			panic(err)
		}

		owner := r.Value
		if dlm.isLockerIdValid(owner) {
			r, err = dlm.storage.WaitVersionChange(ctx, name, r.Version, dlm.cfgKeepAlive)
			if CheckError(err, DLErrNotFound) {
				continue
			}
			if err != nil {
				dlm.unlockLocal(name)
				panic(err)
			}
			owner = r.Value
		} else {
			owner = ""
		}

		if owner == "" {
			r.Value = dlm.lockerId
			r, err = dlm.storage.CasByVersion(ctx, r)
			if err == nil {
				return
			}
		}
	}
}

func (dlm *dlock_manager) unlockGlobal(ctx context.Context, name string) {
	defer dlm.unlockLocal(name)
	r, err := dlm.storage.Get(ctx, name)

	for r != nil && !CheckError(err, DLErrNotFound) {
		if r.Value != dlm.lockerId {
			panic(errors.New("FATAL internal error: unlocking object which is locked by other locker. " +
				"expected lockerId=" + dlm.lockerId + ", but returned one is " + r.Value))
		}

		r, err = dlm.storage.Delete(ctx, r)
	}
}

func (dlm *dlock_manager) lockLocal(ctx context.Context, name string) error {
	ll, err := dlm.leaseLocalLock(name)
	if err != nil {
		return err
	}

	if ll.counter == 1 {
		return nil
	}

	err = nil
	select {
	case _, ok := <-ll.sigChannel:
		if !ok {
			err = error(DLErrClosed)
		}
	case <-ctx.Done():
		err = error(DLErrClosed)
	case <-dlm.shtdwnCh:
		err = error(DLErrClosed)
	}

	if err != nil {
		dlm.releaseLocalLock(name, ll)
	}

	return err
}

func (dlm *dlock_manager) leaseLocalLock(name string) (*local_lock, error) {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	if dlm.state != ST_STARTED {
		return nil, error(DLErrClosed)
	}

	ll := dlm.llocks[name]
	if ll == nil {
		ll = &local_lock{counter: 0, sigChannel: make(chan bool)}
		dlm.llocks[name] = ll
	}
	ll.counter++

	return ll, nil
}

func (dlm *dlock_manager) releaseLocalLock(name string, ll *local_lock) {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	dlm.unlockLocalUnsafe(name, ll, false)
}

func (dlm *dlock_manager) unlockLocal(name string) {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	ll := dlm.llocks[name]
	if ll == nil {
		return
	}

	dlm.unlockLocalUnsafe(name, ll, true)
}

func (dlm *dlock_manager) unlockLocalUnsafe(name string, ll *local_lock, signal bool) {
	ll.counter--
	if ll.counter == 0 {
		delete(dlm.llocks, name)
	}

	if signal && ll.counter > 0 {
		ll.sigChannel <- true
	}
}

func (dlm *dlock_manager) isLockerIdValid(lockerId string) bool {
	if dlm.lockerId == lockerId {
		return true
	}

	_, err := dlm.storage.Get(dlm.ctx, cKeyLockerId+lockerId)
	if CheckError(err, DLErrNotFound) {
		return false
	}
	return true
}
