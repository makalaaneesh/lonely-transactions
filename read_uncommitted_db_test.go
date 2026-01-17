package main

import (
	"testing"

	"github.com/makalaaneesh/lonely-transactions/db"
	"github.com/makalaaneesh/lonely-transactions/test"
)

func TestReadUncommittedDirtyReadAbort(t *testing.T) {
	db := db.NewDatabaseReadUncommitted()
	test.TestDirtyReadAbort_G1a(t, db)
}

func TestReadUncommittedDirtyReadCommit(t *testing.T) {
	db := db.NewDatabaseReadUncommitted()
	test.TestDirtyReadCommit_G1b(t, db)
}

func TestReadUncommittedDirtyWrite(t *testing.T) {
	db := db.NewDatabaseReadUncommitted()
	test.TestDirtyWrite(t, db)
}

func TestReadUncommittedLostUpdate(t *testing.T) {
	db := db.NewDatabaseReadUncommitted()
	test.TestLostUpdateIncrement(t, db)
}

func TestReadUncommittedCircularInformationFlowG1c(t *testing.T) {
	db := db.NewDatabaseReadUncommitted()
	test.TestDirtyReadCircularInformationFlow_G1c(t, db)
}
