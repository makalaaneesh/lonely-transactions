package test

import (
	"testing"

	"github.com/makalaaneesh/lonely-transactions/db"
)

// TestDirtyReadWithBarriers demonstrates dirty read using barrier-based coordination
func TestDirtyReadWithBarriers(t *testing.T) {
	database := db.NewDatabaseReadUncommitted()
	exec := NewTxnsExecutor(database)

	// Transaction 1: Write a value, signal barrier, then rollback
	txn1 := exec.NewTxn("txn1")
	txn1.BeginTx()
	txn1.Set(1, 100)
	txn1.Barrier("txn1_after_write") // Signal that write is complete
	txn1.Rollback()

	// Transaction 2: Wait for txn1's write, then read
	txn2 := exec.NewTxn("txn2")
	txn2.BeginTx()
	txn2.WaitFor("txn1_after_write") // Wait for txn1 to write
	txn2Read := txn2.Get(1)           // Should read the uncommitted value (dirty read)
	txn2.Commit()

	results := exec.Execute(false)

	// Use the GetResult reference to retrieve the value
	value := results.GetValue(txn2Read)
	if value != 100 {
		t.Errorf("Expected txn2 to read dirty value 100, got %d", value)
	}
	t.Logf("Dirty read successful: txn2 read %d before txn1 rolled back", value)
}

// TestWriteWriteConflict demonstrates write-write conflict with blocking
func TestWriteWriteConflict(t *testing.T) {
	database := db.NewDatabaseReadUncommitted()
	exec := NewTxnsExecutor(database)

	// Transaction 1: Write to key 1, wait before committing
	txn1 := exec.NewTxn("txn1")
	txn1.BeginTx()
	txn1.Set(1, 100)
	txn1.Barrier("txn1_first_write_done")
	// Simulate some work before commit
	txn1.Commit()
	txn1.Barrier("txn1_committed")

	// Transaction 2: Wait for txn1's write, then try to write same key
	txn2 := exec.NewTxn("txn2")
	txn2.BeginTx()
	txn2.WaitFor("txn1_first_write_done")
	txn2.Set(1, 200) // This may block if locks are held
	txn2.Commit()

	results := exec.Execute(false)
	_ = results // Results would contain any Get operations

	t.Log("Write-write conflict test completed")
}

// TestComplexInterleaving demonstrates complex multi-transaction interleaving
func TestComplexInterleaving(t *testing.T) {
	database := db.NewDatabaseReadUncommitted()
	exec := NewTxnsExecutor(database)

	// Setup: Initialize some data
	setup := exec.NewTxn("setup")
	setup.BeginTx()
	setup.Set(1, 0)
	setup.Set(2, 0)
	setup.Commit()
	setup.Barrier("setup_done")

	// Transaction 1: Increment key 1
	txn1 := exec.NewTxn("txn1")
	txn1.WaitFor("setup_done")
	txn1.BeginTx()
	txn1Read := txn1.Get(1)
	txn1.Set(1, 10)
	txn1.Barrier("txn1_updated")
	txn1.Commit()

	// Transaction 2: Read key 1, then update key 2
	txn2 := exec.NewTxn("txn2")
	txn2.WaitFor("setup_done")
	txn2.BeginTx()
	txn2.WaitFor("txn1_updated") // Wait for txn1 to update
	txn2Read := txn2.Get(1)       // Should see txn1's uncommitted write (dirty read)
	txn2.Set(2, 20)
	txn2.Commit()

	results := exec.Execute(false)

	// Use GetResult references to retrieve values
	value1 := results.GetValue(txn1Read)
	if value1 != 0 {
		t.Errorf("Expected txn1 to read 0, got %d", value1)
	}

	value2 := results.GetValue(txn2Read)
	if value2 != 10 {
		t.Errorf("Expected txn2 to read 10 (dirty read), got %d", value2)
	}

	t.Logf("Complex interleaving test passed: txn1 read %d, txn2 read %d", value1, value2)
}

// TestLostUpdate demonstrates lost update anomaly
func TestLostUpdate(t *testing.T) {
	database := db.NewDatabaseReadUncommitted()
	exec := NewTxnsExecutor(database)

	// Setup: Initialize counter
	setup := exec.NewTxn("setup")
	setup.BeginTx()
	setup.Set(1, 0)
	setup.Commit()
	setup.Barrier("setup_initialized")

	// Transaction 1: Read and increment
	txn1 := exec.NewTxn("txn1")
	txn1.WaitFor("setup_initialized")
	txn1.BeginTx()
	txn1Read := txn1.Get(1)
	txn1.Barrier("txn1_read")
	txn1.Set(1, 1) // Increment from 0 to 1
	txn1.Commit()

	// Transaction 2: Also read and increment (creates lost update)
	txn2 := exec.NewTxn("txn2")
	txn2.WaitFor("setup_initialized")
	txn2.BeginTx()
	txn2.WaitFor("txn1_read") // Read after txn1 reads
	txn2Read := txn2.Get(1)    // Might read 0 or 1 depending on isolation
	txn2.Set(1, 1)             // Also sets to 1, potentially losing txn1's update
	txn2.Commit()

	results := exec.Execute(false)

	value1 := results.GetValue(txn1Read)
	value2 := results.GetValue(txn2Read)

	t.Logf("Lost update test: txn1 read %d, txn2 read %d", value1, value2)
	t.Log("Both transactions set counter to 1, demonstrating lost update")
}

// TestSequentialOperations tests simple sequential operations without barriers
func TestSequentialOperations(t *testing.T) {
	database := db.NewDatabaseReadUncommitted()
	exec := NewTxnsExecutor(database)

	txn1 := exec.NewTxn("txn1")
	txn1.BeginTx()
	txn1.Set(5, 500)
	txn1.PrintDbState() // Print state after set
	txn1Read := txn1.Get(5)
	txn1.Commit()

	results := exec.Execute(false)

	// Use GetResult reference to retrieve value
	value := results.GetValue(txn1Read)
	if value != 500 {
		t.Errorf("Expected to read 500, got %d", value)
	}
	t.Logf("Sequential operations test passed: read %d", value)
}

// TestRollbackAfterBarrier tests that rollback works correctly after signaling barriers
func TestRollbackAfterBarrier(t *testing.T) {
	database := db.NewDatabaseReadUncommitted()
	exec := NewTxnsExecutor(database)

	txn1 := exec.NewTxn("txn1")
	txn1.BeginTx()
	txn1.Set(10, 1000)
	txn1.Barrier("txn1_wrote_value")
	txn1.Rollback() // Undo the write
	txn1.Barrier("txn1_rolled_back")

	txn2 := exec.NewTxn("txn2")
	txn2.WaitFor("txn1_rolled_back") // Wait for rollback
	txn2.BeginTx()
	txn2Read := txn2.Get(10) // Should read 0 (default) since txn1 rolled back
	txn2.Commit()

	results := exec.Execute(false)

	// Use GetResult reference to retrieve value
	value := results.GetValue(txn2Read)
	if value != 0 {
		t.Errorf("Expected to read 0 after rollback, got %d", value)
	}
	t.Logf("Rollback test passed: read %d after rollback", value)
}

