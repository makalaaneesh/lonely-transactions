package main

import (
	"testing"

	"github.com/makalaaneesh/lonely-transactions/db"
	"github.com/makalaaneesh/lonely-transactions/test"
)

func TestReadUncommittedDirtyReadAbort(t *testing.T) {
	db := db.NewDatabaseReadUncommitted()
	test.TestDirtyReadAbort(t, db)
}

func TestReadUncommittedDirtyReadCommit(t *testing.T) {
	db := db.NewDatabaseReadUncommitted()
	test.TestDirtyReadCommit(t, db)
}
