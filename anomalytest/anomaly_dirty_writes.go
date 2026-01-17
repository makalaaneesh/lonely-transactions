package test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// https://stackoverflow.com/a/66181531
// Similar to G0 in the hermitage documentation
// https://github.com/ept/hermitage/blob/master/postgres.md#read-committed-basic-requirements-g0-g1a-g1b-g1c
func TestDirtyWrite(t *testing.T, db Database) {
	exec := NewTxnsExecutor(db)

	// Initial state: both positions empty (value 0)
	// Key 1 = first_place, Key 2 = second_place

	// Transaction 1: Put racer1 in first, racer2 in second
	txn1 := exec.NewTxn("txn1")
	txn1.BeginTx()
	txn1.Set(1, 100) // first_place = racer1 (represented by 100)
	txn1.Barrier("txn1_wrote_first")
	// Use timeout-based wait: if T2's writes complete (no locking), we continue immediately.
	// If T2 is blocked on row lock (depending on implementation), we timeout and continue.
	txn1.WaitForWithTimeout("txn2_wrote_second", 1000*time.Millisecond)
	txn1.PrintDbState()
	txn1.Set(2, 200) // second_place = racer2 (dirty write over T2 in read-uncommitted!)
	txn1.Commit()
	txn1.Barrier("txn1_committed")

	// Transaction 2: Swap them - racer2 in first, racer1 in second
	txn2 := exec.NewTxn("txn2")
	txn2.BeginTx()
	txn2.WaitFor("txn1_wrote_first") // Wait for T1's first write
	txn2.PrintDbState()
	txn2.Set(1, 200) // first_place = racer2
	txn2.Set(2, 100) // second_place = racer1
	txn2.Barrier("txn2_wrote_second")
	txn2.WaitFor("txn1_committed")
	txn2.Commit()
	txn2.Barrier("txn2_committed")

	// read final state
	txn3 := exec.NewTxn("txn3")
	txn3.BeginTx()
	txn3.WaitFor("txn2_committed")
	txn3.WaitFor("txn1_committed")
	first := txn3.Get(1)
	second := txn3.Get(2)
	txn3.Commit()

	results := exec.Execute(true)

	firstValue := results.GetValue(first)
	secondValue := results.GetValue(second)

	// If dirty writes are prevented: should be either (100,200) or (200,100)
	// If dirty writes occur: might be (100,100) or (200,200) - INCONSISTENT!
	assert.NotEqual(t, firstValue, secondValue, "Both values should be different. firstValue: %d, secondValue: %d", firstValue, secondValue)
}
