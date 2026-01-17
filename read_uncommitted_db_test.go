package main

import (
	"testing"

	"github.com/makalaaneesh/lonely-transactions/anomalytest"
	"github.com/makalaaneesh/lonely-transactions/db"
)

func TestReadUncommittedDirtyReadAbort(t *testing.T) {
	db := db.NewSimpleDBReadUncommitted()
	anomalytest.TestDirtyReadAbort_G1a(t, db)
}

func TestReadUncommittedDirtyReadCommit(t *testing.T) {
	db := db.NewSimpleDBReadUncommitted()
	anomalytest.TestDirtyReadCommit_G1b(t, db)
}

func TestReadUncommittedDirtyWrite(t *testing.T) {
	db := db.NewSimpleDBReadUncommitted()
	anomalytest.TestDirtyWrite(t, db)
}

func TestReadUncommittedLostUpdate(t *testing.T) {
	db := db.NewSimpleDBReadUncommitted()
	anomalytest.TestLostUpdateIncrement(t, db)
}

func TestReadUncommittedCircularInformationFlowG1c(t *testing.T) {
	db := db.NewSimpleDBReadUncommitted()
	anomalytest.TestDirtyReadCircularInformationFlow_G1c(t, db)
}
