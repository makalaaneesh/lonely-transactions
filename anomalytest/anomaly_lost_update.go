package anomalytest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLostUpdateIncrement demonstrates the lost update anomaly where concurrent increments
// can result in one update being lost.
// Classic scenario: Two transactions both read a counter, increment it, and write it back.
// With proper isolation: both increments should be applied (0 -> 1 -> 2)
// With lost update anomaly: second write overwrites first (0 -> 1 -> 1)
func TestLostUpdateIncrement(t *testing.T, db Database) {
	exec := NewTxnsExecutor(db)

	// Initial state: key 1 = 0

	// Transaction 1: Read counter, increment, write back
	txn1 := exec.NewTxn("txn1")
	txn1.BeginTx()
	read1 := txn1.Get(1) // Read counter (expecting 0)
	txn1.Barrier("txn1_read")
	txn1.WaitFor("txn2_read") // Wait for T2 to also read the old value
	// Compute and write incremented value based on what we read
	txn1.SetComputed(1, func() int {
		return exec.resultStore.GetValue(read1) + 1
	})
	txn1.Barrier("txn1_wrote")
	txn1.WaitFor("txn2_wrote") // Wait for T2 to write
	txn1.Commit()
	txn1.Barrier("txn1_committed")

	// Transaction 2: Read counter, increment, write back
	txn2 := exec.NewTxn("txn2")
	txn2.BeginTx()
	txn2.WaitFor("txn1_read")
	read2 := txn2.Get(1) // Read counter (expecting 0)
	txn2.Barrier("txn2_read")

	txn2.WaitFor("txn1_wrote") // Wait for T1 to write its increment
	txn2.PrintDbState()
	// Compute and write incremented value based on what we read
	txn2.SetComputed(1, func() int {
		return exec.resultStore.GetValue(read2) + 1
	})
	txn2.Barrier("txn2_wrote")
	txn2.Commit()
	txn2.Barrier("txn2_committed")

	// Transaction 3: Read final value
	txn3 := exec.NewTxn("txn3")
	txn3.BeginTx()
	txn3.WaitFor("txn1_committed")
	txn3.WaitFor("txn2_committed")
	finalRead := txn3.Get(1)
	txn3.Commit()

	results := exec.Execute(true)

	value1 := results.GetValue(read1)
	value2 := results.GetValue(read2)
	finalValue := results.GetValue(finalRead)

	// Both transactions read 0
	assert.Equal(t, 0, value1, "T1 should read 0")
	assert.Equal(t, 0, value2, "T2 should read 0")

	// If lost update occurs: final value = 1 (T2 overwrites T1's increment)
	// If proper isolation: final value = 2 (both increments applied)
	assert.Equal(t, 2, finalValue, "Final value should be 2 (both increments applied), but got %d (lost update!)", finalValue)
}
