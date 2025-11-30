package test

type Database interface {
	BeginTx(isolationLevel string) (int64, error)
	Set(txId int64, key string, value string) error
	Get(txId int64, key string) (string, error)
	Delete(txId int64, key string) error
	Commit(txId int64) error
	Rollback(txId int64) error
}
