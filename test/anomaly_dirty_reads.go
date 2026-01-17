package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Corresponds to G1a in the hermitage documentation
// https://github.com/ept/hermitage/blob/master/postgres.md#read-committed-basic-requirements-g0-g1a-g1b-g1c
func TestDirtyReadAbort_G1a(t *testing.T, db Database) {
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
	txn2Read := txn2.Get(1)         // Should read the uncommitted value (dirty read)
	txn2.Barrier("txn2_after_read") // Signal that read is complete
	txn2.Commit()

	// Execute the scheduled operations
	results := exec.Execute(true)

	// Use the GetResult reference to retrieve the value
	value := results.GetValue(txn2Read)
	assert.Equal(t, 0, value) // Should not read the dirty value written by txn1
}

// Corresponds to G1b in the hermitage documentation
// https://github.com/ept/hermitage/blob/master/postgres.md#read-committed-basic-requirements-g0-g1a-g1b-g1c
func TestDirtyReadCommit_G1b(t *testing.T, db Database) {
	exec := NewTxnsExecutor(db)

	// Transaction 1: Begin, write 1 = 100, signal barrier, then commit
	txn1 := exec.NewTxn("txn1")
	txn1.BeginTx()
	txn1.WaitFor("txn2_after_first_read")
	txn1.Set(1, 100)
	txn1.Barrier("txn1_after_write") // Signal that write is complete
	txn1.WaitFor("txn2_after_second_read")
	txn1.Commit()
	txn1.Barrier("txn1_after_commit")

	// Transaction2
	txn2 := exec.NewTxn("txn2")
	txn2.BeginTx()

	read1 := txn2.Get(1)
	txn2.Barrier("txn2_after_first_read")

	txn2.WaitFor("txn1_after_write")
	txn2.PrintDbState()
	read2 := txn2.Get(1)
	txn2.Barrier("txn2_after_second_read")

	txn2.WaitFor("txn1_after_commit")
	read3 := txn2.Get(1)
	txn2.Barrier("txn2_after_third_read")

	txn2.Commit()

	results := exec.Execute(true)

	value1 := results.GetValue(read1)
	value2 := results.GetValue(read2)
	value3 := results.GetValue(read3)

	assert.Equal(t, 0, value1)
	assert.Equal(t, 0, value2)
	assert.Equal(t, 100, value3)
}

// TestCircularInformationFlow_G1c tests that Circular Information Flow (G1c) is prevented.
// This anomaly occurs when two transactions each read the other's uncommitted writes,
// creating a circular dependency.
//
// Setup: key 1 = 10, key 2 = 20
//
// The test simulates:
//
//	T1: begin
//	T2: begin
//	T1: update key 1 = 11
//	T2: update key 2 = 22
//	T1: select key 2 -- Should read 20 (not T2's uncommitted 22)
//	T2: select key 1 -- Should read 10 (not T1's uncommitted 11)
//	T1: commit
//	T2: commit
//
// With proper G1c prevention (Read Committed or higher), neither transaction
// should see the other's uncommitted writes.
//
// https://github.com/ept/hermitage/blob/master/postgres.md#read-committed-basic-requirements-g0-g1a-g1b-g1c
func TestCircularInformationFlow_G1c(t *testing.T, db Database) {
	exec := NewTxnsExecutor(db)

	// Setup initial state: key 1 = 10, key 2 = 20
	setupTxn := exec.NewTxn("setup")
	setupTxn.BeginTx()
	setupTxn.Set(1, 10)
	setupTxn.Set(2, 20)
	setupTxn.Commit()
	setupTxn.Barrier("setup_complete")

	// Transaction 1
	txn1 := exec.NewTxn("txn1")
	txn1.WaitFor("setup_complete")
	txn1.BeginTx()
	txn1.Set(1, 11) // T1 writes key 1 = 11
	txn1.Barrier("txn1_wrote_key1")
	txn1.WaitFor("txn2_wrote_key2")
	txn1.PrintDbState()
	read1 := txn1.Get(2) // T1 reads key 2 - should see 20, not T2's uncommitted 22
	txn1.Barrier("txn1_read_key2")
	txn1.WaitFor("txn2_read_key1")
	txn1.Commit()
	txn1.Barrier("txn1_committed")

	// Transaction 2
	txn2 := exec.NewTxn("txn2")
	txn2.WaitFor("setup_complete")
	txn2.BeginTx()
	txn2.WaitFor("txn1_wrote_key1")
	txn2.Set(2, 22) // T2 writes key 2 = 22
	txn2.Barrier("txn2_wrote_key2")
	txn2.WaitFor("txn1_read_key2")
	txn2.PrintDbState()
	read2 := txn2.Get(1) // T2 reads key 1 - should see 10, not T1's uncommitted 11
	txn2.Barrier("txn2_read_key1")
	txn2.WaitFor("txn1_committed")
	txn2.Commit()
	txn2.Barrier("txn2_committed")

	// Read final state
	txn3 := exec.NewTxn("txn3")
	txn3.WaitFor("txn2_committed")
	txn3.BeginTx()
	finalRead1 := txn3.Get(1)
	finalRead2 := txn3.Get(2)
	txn3.Commit()

	results := exec.Execute(true)

	// T1's read of key 2: should be 20 (original value), not 22 (T2's uncommitted write)
	value1 := results.GetValue(read1)
	// T2's read of key 1: should be 10 (original value), not 11 (T1's uncommitted write)
	value2 := results.GetValue(read2)

	// Final state should be: key 1 = 11, key 2 = 22 (both committed)
	finalValue1 := results.GetValue(finalRead1)
	finalValue2 := results.GetValue(finalRead2)

	// With G1c prevention (Read Committed), neither transaction sees the other's uncommitted writes
	assert.Equal(t, 20, value1, "T1 should read original value 20 for key 2, not T2's uncommitted 22")
	assert.Equal(t, 10, value2, "T2 should read original value 10 for key 1, not T1's uncommitted 11")

	// Verify final committed state
	assert.Equal(t, 11, finalValue1, "Final key 1 should be 11 after T1's commit")
	assert.Equal(t, 22, finalValue2, "Final key 2 should be 22 after T2's commit")
}
