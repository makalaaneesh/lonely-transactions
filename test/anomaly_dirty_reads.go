package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirtyRead(t *testing.T, db Database) {
	exec := NewTxnsExecutor(db)

	// Transaction 1: Begin, write 1 = 100, signal barrier, then rollback
	txn1 := exec.NewTxn("txn1")
	txn1.BeginTx()
	txn1.Set(1, 100)
	txn1.Barrier("txn1_after_write") // Signal that write is complete
	txn1.WaitFor("txn2_after_read")  // Wait for the read to be complete
	txn1.Rollback()

	// Transaction 2: Wait for txn1's write, then read
	txn2 := exec.NewTxn("txn2")
	txn2.BeginTx()
	txn2.WaitFor("txn1_after_write") // Wait for txn1 to write
	txn2.PrintDbState()
	txn2.Get(1)                     // Should read the uncommitted value (dirty read)
	txn2.Barrier("txn2_after_read") // Signal that read is complete
	txn2.Commit()

	// Execute the scheduled operations
	results := exec.Execute(true)

	// Transaction 2's Get operation is at index 2 (BeginTx=0, WaitFor=1, Get=2)
	value := results.Get("txn2", 2)
	assert.Equal(t, 100, value) // Should read the dirty value before txn1 rolls back
}
