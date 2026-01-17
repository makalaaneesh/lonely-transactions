package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// https://stackoverflow.com/a/66181531
func TestDirtyWrite(t *testing.T, db Database) {
	exec := NewTxnsExecutor(db)

	// Initial state: both positions empty (value 0)
	// Key 1 = first_place, Key 2 = second_place

	// Transaction 1: Put racer1 in first, racer2 in second
	txn1 := exec.NewTxn("txn1")
	txn1.BeginTx()
	txn1.Set(1, 100) // first_place = racer1 (represented by 100)
	txn1.Barrier("txn1_wrote_first")
	txn1.WaitFor("txn2_committed") // Allow T2 to interleave and write both first and second
	txn1.PrintDbState()
	txn1.Set(2, 200) // second_place = racer2 (dirty write over T2!)
	txn1.Commit()
	txn1.Barrier("txn1_committed")

	// Transaction 2: Swap them - racer2 in first, racer1 in second
	txn2 := exec.NewTxn("txn2")
	txn2.BeginTx()
	txn2.WaitFor("txn1_wrote_first") // Wait for T1's first write
	txn2.PrintDbState()
	txn2.Set(1, 200) // first_place = racer2 (dirty write over T1!)
	txn2.Set(2, 100) // second_place = racer1
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

// TestWriteCycleG0 tests that Write Cycles (G0) are prevented.
// This is the anomaly described at:
// https://github.com/ept/hermitage/blob/master/postgres.md#read-committed-basic-requirements-g0-g1a-g1b-g1c
//
// Setup: key 1 = 10, key 2 = 20
//
// The test simulates:
//
//	T1: begin
//	T2: begin
//	T1: update key 1 = 11
//	T2: update key 1 = 12 -- attemps to write key 1 after T1's write
//	T1: update key 2 = 21
//	T1: commit -- This unblocks T2
//	T2: update key 2 = 22
//	T2: commit
//	Final state: key 1 = 12, key 2 = 22
//
// With proper G0 prevention, T2's write to key 1 only completes after T1 commits,
// so T2's writes (12, 22) become the final values.
func TestWriteCycleG0(t *testing.T, db Database) {
	exec := NewTxnsExecutor(db)

	// Setup initial state: key 1 = 10, key 2 = 20
	setupTxn := exec.NewTxn("setup")
	setupTxn.BeginTx()
	setupTxn.Set(1, 10)
	setupTxn.Set(2, 20)
	setupTxn.Commit()

	// Transaction 1
	txn1 := exec.NewTxn("txn1")
	txn1.BeginTx()
	txn1.Set(1, 11)
	txn1.Barrier("txn1_wrote_key1")
	txn1.Set(2, 21)
	txn1.Commit() // This would unblock T2's write to key 1
	txn1.Barrier("txn1_committed")

	// Transaction 2
	txn2 := exec.NewTxn("txn2")
	txn2.BeginTx()
	txn2.WaitFor("txn1_wrote_key1")
	txn2.Set(1, 12) // T2 attempts to write key 1 after T1's write
	txn2.Set(2, 22)
	txn2.Commit()
	txn2.Barrier("txn2_committed")

	// Read final state
	txn3 := exec.NewTxn("txn3")
	txn3.BeginTx()
	txn3.WaitFor("txn2_committed")
	read1 := txn3.Get(1)
	read2 := txn3.Get(2)
	txn3.Commit()

	results := exec.Execute(true)

	value1 := results.GetValue(read1)
	value2 := results.GetValue(read2)

	// With proper G0 prevention (row locking), final state should be:
	// key 1 = 12 (T2's write, which happened after T1's commit)
	// key 2 = 22 (T2's write)
	assert.Equal(t, 12, value1, "key 1 should be 12 (T2's final write)")
	assert.Equal(t, 22, value2, "key 2 should be 22 (T2's final write)")
}
