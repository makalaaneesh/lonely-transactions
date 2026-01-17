package db

import (
	"testing"

	"github.com/makalaaneesh/lonely-transactions/anomalytest"
)

func TestSimpleDBReadUncommittedDirtyReadAbort(t *testing.T) {
	db := NewSimpleDBReadUncommitted()
	anomalytest.TestDirtyReadAbort_G1a(t, db)
}

func TestSimpleDBReadUncommittedDirtyReadCommit(t *testing.T) {
	db := NewSimpleDBReadUncommitted()
	anomalytest.TestDirtyReadCommit_G1b(t, db)
}

func TestSimpleDBReadUncommittedDirtyReadCircularInformationFlowG1c(t *testing.T) {
	db := NewSimpleDBReadUncommitted()
	anomalytest.TestDirtyReadCircularInformationFlow_G1c(t, db)
}

func TestSimpleDBReadUncommittedDirtyWrite(t *testing.T) {
	db := NewSimpleDBReadUncommitted()
	anomalytest.TestDirtyWrite(t, db)
}
