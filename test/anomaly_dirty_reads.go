package test

import "testing"

func TestDirtyRead(t *testing.T, db Database) {
	exec := NewTxnsExecutor(db)

	// Transaction 1: Begin, write "x" = "dirty", then rollback
	txn1 := exec.NewTxn("txn1")
	txn1.At(0).BeginTx()
	txn1.At(1).Set("x", "dirty")
	txn1.At(4).Rollback()

	// Transaction 2: Begin, read "x", commit
	txn2 := exec.NewTxn("txn2")
	txn2.At(2).BeginTx()
	txn2.At(3).Get("x")
	txn2.At(5).Commit()

	// Execute the scheduled operations
	results := exec.Execute()

	// Transaction 2 should have read the uncommitted "dirty" value
	value := results.Get("txn2", 3)
	if value != "dirty" {
		t.Errorf("Expected txn2 to read 'dirty' at step 3, got '%s'", value)
	}
}
