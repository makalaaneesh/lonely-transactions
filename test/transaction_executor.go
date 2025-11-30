package test

import (
	"fmt"
	"sync"
)

// opKind represents the type of operation
type opKind int

const (
	opDatabase opKind = iota // Database operation (BeginTx, Set, Get, etc.)
	opBarrier                // Barrier - signals a named synchronization point
	opWaitFor                // WaitFor - waits for a named barrier
)

// GetResult is a reference to a Get operation's result
type GetResult struct {
	txnName string
	opIndex int
}

// operation represents a single operation in a transaction
type operation struct {
	kind        opKind
	fn          func() error // For database operations
	barrierName string       // For Barrier and WaitFor operations
	opIndex     int          // Index of this operation in the transaction
	description string       // Human-readable description for debug output
}

// TxnsExecutor coordinates the execution of multiple transactions with barrier-based synchronization
type TxnsExecutor struct {
	db          Database
	txns        map[string]*Txn
	barriers    map[string]chan struct{}
	resultStore *Results
	mu          sync.Mutex
}

// NewTxnsExecutor creates a new transaction executor
func NewTxnsExecutor(db Database) *TxnsExecutor {
	return &TxnsExecutor{
		db:          db,
		txns:        make(map[string]*Txn),
		barriers:    make(map[string]chan struct{}),
		resultStore: newResults(),
	}
}

// NewTxn creates a new transaction handle
func (e *TxnsExecutor) NewTxn(name string) *Txn {
	e.mu.Lock()
	defer e.mu.Unlock()
	txn := &Txn{
		name:       name,
		executor:   e,
		db:         e.db,
		operations: []operation{},
		opCounter:  0,
	}
	e.txns[name] = txn
	return txn
}

// Execute runs all scheduled transactions concurrently with barrier-based coordination
func (e *TxnsExecutor) Execute(debug bool) *Results {
	// Phase 1: Register all barriers
	e.registerBarriers()

	// Phase 2: Start transaction goroutines
	var wg sync.WaitGroup
	for _, txn := range e.txns {
		wg.Add(1)
		go func(t *Txn) {
			defer wg.Done()
			t.run(e.barriers, debug)
		}(txn)
	}

	// Phase 3: Wait for all transactions to complete
	wg.Wait()

	return e.resultStore
}

// registerBarriers scans all transactions and creates channels for all barrier names
func (e *TxnsExecutor) registerBarriers() {
	for _, txn := range e.txns {
		for _, op := range txn.operations {
			if op.kind == opBarrier {
				e.barriers[op.barrierName] = make(chan struct{})
			}
		}
	}
}

// Txn represents a transaction handle with direct operation methods
type Txn struct {
	name       string
	executor   *TxnsExecutor
	db         Database
	txnId      int64
	operations []operation
	opCounter  int
	mu         sync.Mutex
}

// run executes all operations for this transaction sequentially
func (t *Txn) run(barriers map[string]chan struct{}, debug bool) {
	for _, op := range t.operations {
		switch op.kind {
		case opDatabase:
			if debug {
				fmt.Printf("[%s] (%d) %s\n", t.name, op.opIndex, op.description)
			}
			if err := op.fn(); err != nil {
				fmt.Printf("Error in transaction %s at op %d: %v\n", t.name, op.opIndex, err)
			}
		case opBarrier:
			if debug {
				fmt.Printf("[%s] (%d) BARRIER %s\n", t.name, op.opIndex, op.barrierName)
			}
			close(barriers[op.barrierName])
		case opWaitFor:
			if debug {
				fmt.Printf("[%s] (%d) WAIT_FOR %s\n", t.name, op.opIndex, op.barrierName)
			}
			<-barriers[op.barrierName]
			if debug {
				fmt.Printf("[%s] (%d) UNBLOCKED from %s\n", t.name, op.opIndex, op.barrierName)
			}
		}
	}
}

// addOp adds an operation to the transaction's operation list
func (t *Txn) addOp(op operation) {
	t.mu.Lock()
	defer t.mu.Unlock()
	op.opIndex = t.opCounter
	t.opCounter++
	t.operations = append(t.operations, op)
}

// BeginTx schedules a BeginTx operation
func (t *Txn) BeginTx() {
	t.addOp(operation{
		kind:        opDatabase,
		description: "BEGIN_TX",
		fn: func() error {
			txnId, err := t.db.BeginTx("READ_UNCOMMITTED")
			if err != nil {
				return err
			}
			t.txnId = txnId
			return nil
		},
	})
}

// Set schedules a Set operation
func (t *Txn) Set(key, value int) {
	t.addOp(operation{
		kind:        opDatabase,
		description: fmt.Sprintf("SET %d = %d", key, value),
		fn: func() error {
			return t.db.Set(t.txnId, key, value)
		},
	})
}

// SetComputed schedules a Set operation with a value computed at execution time
func (t *Txn) SetComputed(key int, valueFn func() int) {
	t.addOp(operation{
		kind:        opDatabase,
		description: fmt.Sprintf("SET_COMPUTED %d = <computed>", key),
		fn: func() error {
			value := valueFn()
			return t.db.Set(t.txnId, key, value)
		},
	})
}

// Get schedules a Get operation and captures the result, returning a reference to retrieve it later
func (t *Txn) Get(key int) *GetResult {
	currentOpIndex := t.opCounter
	result := &GetResult{
		txnName: t.name,
		opIndex: currentOpIndex,
	}

	t.addOp(operation{
		kind:        opDatabase,
		description: fmt.Sprintf("GET %d", key),
		fn: func() error {
			value, err := t.db.Get(t.txnId, key)
			if err != nil {
				return err
			}
			// Store the result indexed by operation index
			t.executor.resultStore.store(t.name, currentOpIndex, value)
			return nil
		},
	})

	return result
}

// Delete schedules a Delete operation
func (t *Txn) Delete(key int) {
	t.addOp(operation{
		kind:        opDatabase,
		description: fmt.Sprintf("DELETE %d", key),
		fn: func() error {
			return t.db.Delete(t.txnId, key)
		},
	})
}

// Commit schedules a Commit operation
func (t *Txn) Commit() {
	t.addOp(operation{
		kind:        opDatabase,
		description: "COMMIT",
		fn: func() error {
			return t.db.Commit(t.txnId)
		},
	})
}

// Rollback schedules a Rollback operation
func (t *Txn) Rollback() {
	t.addOp(operation{
		kind:        opDatabase,
		description: "ROLLBACK",
		fn: func() error {
			return t.db.Rollback(t.txnId)
		},
	})
}

// Barrier creates a named synchronization point that other transactions can wait for
func (t *Txn) Barrier(name string) {
	t.addOp(operation{
		kind:        opBarrier,
		barrierName: name,
	})
}

// WaitFor waits for a named barrier to be signaled
func (t *Txn) WaitFor(barrierName string) {
	t.addOp(operation{
		kind:        opWaitFor,
		barrierName: barrierName,
	})
}

// PrintDbState schedules a database state print operation for debugging
func (t *Txn) PrintDbState() {
	t.addOp(operation{
		kind:        opDatabase,
		description: "PRINT_DB_STATE",
		fn: func() error {
			fmt.Printf("(%s) ", t.name)
			t.db.PrintState()
			return nil
		},
	})
}

// Results stores the results of Get operations indexed by transaction name and operation index
type Results struct {
	data map[string]map[int]int
	mu   sync.RWMutex
}

// newResults creates a new Results storage
func newResults() *Results {
	return &Results{
		data: make(map[string]map[int]int),
	}
}

// store saves a result for a specific transaction and operation index
func (r *Results) store(txnName string, opIndex int, value int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.data[txnName] == nil {
		r.data[txnName] = make(map[int]int)
	}
	r.data[txnName][opIndex] = value
}

// Get retrieves the result of a Get operation for a specific transaction and operation index
func (r *Results) Get(txnName string, opIndex int) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if txnData, ok := r.data[txnName]; ok {
		return txnData[opIndex]
	}
	return 0
}

// GetValue retrieves the value using a GetResult reference
func (r *Results) GetValue(ref *GetResult) int {
	return r.Get(ref.txnName, ref.opIndex)
}
