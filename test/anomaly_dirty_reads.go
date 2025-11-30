package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirtyRead(t *testing.T, db Database) {
	exec := NewTxnsExecutor(db)

	// Transaction 1: Begin, write 1 = 100, then rollback
	txn1 := exec.NewTxn("txn1")
	txn1.At(0).BeginTx()
	txn1.At(1).Set(1, 100)
	txn1.At(4).Rollback()

	// Transaction 2: Begin, read 1, commit
	txn2 := exec.NewTxn("txn2")
	txn2.At(2).BeginTx()
	txn2.At(3).Get(1)
	txn2.At(5).Commit()

	// Execute the scheduled operations
	results := exec.Execute(true)

	// Transaction 2 should have read the uncommitted value 100
	value := results.Get("txn2", 3)
	assert.Equal(t, 0, value)
}
