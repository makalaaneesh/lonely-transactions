package main

import (
	"testing"

	"github.com/makalaaneesh/lonely-transactions/db"
	"github.com/makalaaneesh/lonely-transactions/test"
)

func TestReadUncommitted(t *testing.T) {
	db := db.NewDatabaseReadUncommitted()
	test.TestDirtyRead(t, db)
}
