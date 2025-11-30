package db

import "sync"

type DatabaseReadUncommitted struct {
	data       map[string]string
	mu         sync.RWMutex
	nextTxnId  int64
	txnUndoOps map[int64][]func()
}

func NewDatabaseReadUncommitted() *DatabaseReadUncommitted {
	return &DatabaseReadUncommitted{
		data: make(map[string]string),
		mu:   sync.RWMutex{},
	}
}

func (d *DatabaseReadUncommitted) BeginTx(isolationLevel string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return 0, nil
}

func (d *DatabaseReadUncommitted) Set(txId int64, key string, value string) error {
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

func (d *DatabaseReadUncommitted) Get(txId int64, key string) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.data[key], nil
}

func (d *DatabaseReadUncommitted) Delete(txId int64, key string) error {
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

func (d *DatabaseReadUncommitted) Commit(txId int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.txnUndoOps, txId)
	return nil
}

func (d *DatabaseReadUncommitted) Rollback(txId int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// apply undo operations for this txn in reverse order
	for i := len(d.txnUndoOps[txId]) - 1; i >= 0; i-- {
		d.txnUndoOps[txId][i]()
	}
	delete(d.txnUndoOps, txId)
	return nil
}
