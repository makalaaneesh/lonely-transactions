package test

type Database interface {
	BeginTx(isolationLevel string) (int64, error)
	Set(txId int64, key int, value int) error
	Get(txId int64, key int) (int, error)
	Delete(txId int64, key int) error
	Commit(txId int64) error
	Rollback(txId int64) error
	PrintState()
}
