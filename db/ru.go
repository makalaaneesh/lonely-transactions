package db

import (
	"fmt"
	"sync"
)

type DatabaseReadUncommitted struct {
	data       map[int]int
	mu         sync.RWMutex
	nextTxnId  int64
	txnUndoOps map[int64][]func()
}

func NewDatabaseReadUncommitted() *DatabaseReadUncommitted {
	return &DatabaseReadUncommitted{
		data:       make(map[int]int),
		mu:         sync.RWMutex{},
		nextTxnId:  1,
		txnUndoOps: make(map[int64][]func()),
	}
}

func (d *DatabaseReadUncommitted) BeginTx(isolationLevel string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	txId := d.nextTxnId
	d.nextTxnId++

	d.txnUndoOps[txId] = make([]func(), 0)
	return txId, nil
}

func (d *DatabaseReadUncommitted) Set(txId int64, key int, value int) error {
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

func (d *DatabaseReadUncommitted) Get(txId int64, key int) (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.data[key], nil
}

func (d *DatabaseReadUncommitted) Delete(txId int64, key int) error {
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

func (d *DatabaseReadUncommitted) PrintState() {
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
