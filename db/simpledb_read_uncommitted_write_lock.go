package db

import (
	"fmt"
	"sync"
)

type SimpleDBReadUncommittedWriteLock struct {
	data       map[int]int
	mu         sync.RWMutex
	nextTxnId  int64
	txnUndoOps map[int64][]func()

	// Row-level write locks (separate from mu)
	rowLocksMu   sync.Mutex             // protects rowLocks and txnHeldLocks
	rowLocks     map[int]*sync.Mutex    // key -> per-row mutex
	txnHeldLocks map[int64]map[int]bool // txnId -> set of locked keys
}

func NewSimpleDBReadUncommittedWriteLock() *SimpleDBReadUncommittedWriteLock {
	return &SimpleDBReadUncommittedWriteLock{
		data:         make(map[int]int),
		mu:           sync.RWMutex{},
		nextTxnId:    1,
		txnUndoOps:   make(map[int64][]func()),
		rowLocks:     make(map[int]*sync.Mutex),
		txnHeldLocks: make(map[int64]map[int]bool),
	}
}

func (d *SimpleDBReadUncommittedWriteLock) BeginTx(isolationLevel string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	txId := d.nextTxnId
	d.nextTxnId++
	d.txnUndoOps[txId] = make([]func(), 0)
	return txId, nil
}

// acquireRowLock acquires a row-level write lock, blocking if another txn holds it
func (d *SimpleDBReadUncommittedWriteLock) acquireRowLock(txId int64, key int) {
	d.rowLocksMu.Lock()
	if d.txnHeldLocks[txId] != nil && d.txnHeldLocks[txId][key] {
		d.rowLocksMu.Unlock()
		return // Already hold this lock
	}

	rowMu := d.rowLocks[key]
	if rowMu == nil {
		rowMu = &sync.Mutex{}
		d.rowLocks[key] = rowMu
	}
	d.rowLocksMu.Unlock()

	rowMu.Lock() // May block here

	d.rowLocksMu.Lock()
	if d.txnHeldLocks[txId] == nil {
		d.txnHeldLocks[txId] = make(map[int]bool)
	}
	d.txnHeldLocks[txId][key] = true
	d.rowLocksMu.Unlock()
}

// releaseRowLocks releases all row-level locks held by a transaction
func (d *SimpleDBReadUncommittedWriteLock) releaseRowLocks(txId int64) {
	d.rowLocksMu.Lock()
	defer d.rowLocksMu.Unlock()
	for key := range d.txnHeldLocks[txId] {
		d.rowLocks[key].Unlock()
	}
	delete(d.txnHeldLocks, txId)
}

func (d *SimpleDBReadUncommittedWriteLock) Set(txId int64, key int, value int) error {
	// Acquire row lock BEFORE d.mu to avoid deadlock:
	// If we held d.mu while blocking on a row lock, other txns couldn't commit
	// (commit needs d.mu), so the row lock would never be released.
	d.acquireRowLock(txId, key)

	d.mu.Lock()
	defer d.mu.Unlock()
	oldValue, ok := d.data[key]
	if ok {
		d.txnUndoOps[txId] = append(d.txnUndoOps[txId], func() {
			d.data[key] = oldValue
		})
	} else {
		d.txnUndoOps[txId] = append(d.txnUndoOps[txId], func() {
			delete(d.data, key)
		})
	}
	d.data[key] = value
	return nil
}

func (d *SimpleDBReadUncommittedWriteLock) Get(txId int64, key int) (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.data[key], nil
}

func (d *SimpleDBReadUncommittedWriteLock) Delete(txId int64, key int) error {
	// Acquire row lock BEFORE d.mu to avoid deadlock (see Set for explanation)
	d.acquireRowLock(txId, key)

	d.mu.Lock()
	defer d.mu.Unlock()
	oldValue, ok := d.data[key]
	if ok {
		d.txnUndoOps[txId] = append(d.txnUndoOps[txId], func() {
			d.data[key] = oldValue
		})
	}
	delete(d.data, key)
	return nil
}

func (d *SimpleDBReadUncommittedWriteLock) Commit(txId int64) error {
	// Release row locks BEFORE d.mu to allow blocked txns to proceed
	// before we hold d.mu (maintains consistent lock ordering with Set/Delete)
	d.releaseRowLocks(txId)

	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.txnUndoOps, txId)
	return nil
}

func (d *SimpleDBReadUncommittedWriteLock) Rollback(txId int64) error {
	// Release row locks BEFORE d.mu (see Commit for explanation)
	d.releaseRowLocks(txId)

	d.mu.Lock()
	defer d.mu.Unlock()
	for i := len(d.txnUndoOps[txId]) - 1; i >= 0; i-- {
		d.txnUndoOps[txId][i]()
	}
	delete(d.txnUndoOps, txId)
	return nil
}

func (d *SimpleDBReadUncommittedWriteLock) PrintState() {
	d.mu.RLock()
	defer d.mu.RUnlock()
	fmt.Println("--------------------------------")
	fmt.Println("Database State:")
	for key, value := range d.data {
		fmt.Printf("  %d: %d\n", key, value)
	}

	fmt.Println("Txn Undo Ops:")
	for txId, ops := range d.txnUndoOps {
		fmt.Printf("  Txn %d: %v\n", txId, ops)
	}
	fmt.Println("Next Txn ID:")
	fmt.Printf("  %d\n", d.nextTxnId)
	fmt.Println("--------------------------------")
}
