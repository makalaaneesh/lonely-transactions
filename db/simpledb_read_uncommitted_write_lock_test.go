package db

import (
	"testing"

	"github.com/makalaaneesh/lonely-transactions/anomalytest"
)

func TestSimpleDBReadUncommittedWriteLockDirtyReadAbort(t *testing.T) {
	db := NewSimpleDBReadUncommittedWriteLock()
	anomalytest.TestDirtyReadAbort_G1a(t, db)
}

func TestSimpleDBReadUncommittedWriteLockDirtyReadCommit(t *testing.T) {
	db := NewSimpleDBReadUncommittedWriteLock()
	anomalytest.TestDirtyReadCommit_G1b(t, db)
}

func TestSimpleDBReadUncommittedWriteLockDirtyReadCircularInformationFlowG1c(t *testing.T) {
	db := NewSimpleDBReadUncommittedWriteLock()
	anomalytest.TestDirtyReadCircularInformationFlow_G1c(t, db)
}

func TestSimpleDBReadUncommittedWriteLockDirtyWrite(t *testing.T) {
	db := NewSimpleDBReadUncommittedWriteLock()
	anomalytest.TestDirtyWrite(t, db)
}
