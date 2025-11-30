package test

import (
	"fmt"
	"sync"
)

// scheduledOp represents an operation scheduled at a specific step
type scheduledOp struct {
	step int
	fn   func() error
}

// TxnsExecutor coordinates the execution of multiple transactions with deterministic step-based scheduling
type TxnsExecutor struct {
	db          Database
	txns        map[string]*Txn
	resultStore *Results
	mu          sync.Mutex
}

// NewTxnsExecutor creates a new transaction executor
func NewTxnsExecutor(db Database) *TxnsExecutor {
	return &TxnsExecutor{
		db:          db,
		txns:        make(map[string]*Txn),
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
		operations: []scheduledOp{},
		stepSignal: make(chan int),
		done:       make(chan int),
	}
	e.txns[name] = txn
	return txn
}

// Execute runs all scheduled transactions concurrently with step-based coordination
func (e *TxnsExecutor) Execute(debug bool) *Results {
	// Find the maximum step
	maxStep := 0
	for _, txn := range e.txns {
		for _, op := range txn.operations {
			if op.step > maxStep {
				maxStep = op.step
			}
		}
	}

	// Start a goroutine for each transaction
	var wg sync.WaitGroup
	for _, txn := range e.txns {
		wg.Add(1)
		go func(t *Txn) {
			defer wg.Done()
			t.run()
		}(txn)
	}

	// Coordinate step-by-step execution
	for currentStep := 0; currentStep <= maxStep; currentStep++ {
		// Find transactions that have operations at this step
		txnsAtStep := []*Txn{}
		for _, txn := range e.txns {
			if txn.hasOpAtStep(currentStep) {
				txnsAtStep = append(txnsAtStep, txn)
			}
		}

		// Signal all of them simultaneously to start this step
		for _, txn := range txnsAtStep {
			txn.stepSignal <- currentStep
		}

		// Wait for all of them to complete this step
		for _, txn := range txnsAtStep {
			<-txn.done
		}
		if debug {
			fmt.Println("After step", currentStep)
			e.db.PrintState()
		}
	}

	// Close all step signal channels to stop goroutines
	for _, txn := range e.txns {
		close(txn.stepSignal)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	return e.resultStore
}

// Txn represents a transaction handle that only exposes the At() method
type Txn struct {
	name       string
	executor   *TxnsExecutor
	db         Database
	txnId      int64
	operations []scheduledOp
	stepSignal chan int
	done       chan int
}

// At returns a TxnStepExecutor for scheduling operations at the specified step
func (t *Txn) At(step int) *TxnStepExecutor {
	return &TxnStepExecutor{
		txn:  t,
		step: step,
	}
}

// hasOpAtStep checks if this transaction has any operation scheduled at the given step
func (t *Txn) hasOpAtStep(step int) bool {
	for _, op := range t.operations {
		if op.step == step {
			return true
		}
	}
	return false
}

// run is the goroutine function that executes operations for this transaction
func (t *Txn) run() {
	for step := range t.stepSignal {
		// Execute all operations scheduled for this step
		for _, op := range t.operations {
			if op.step == step {
				if err := op.fn(); err != nil {
					// Store error (for now just print, could be improved)
					fmt.Printf("Error in transaction %s at step %d: %v\n", t.name, step, err)
				}
			}
		}
		// Signal completion of this step
		t.done <- step
	}
}

// scheduleOp adds an operation to be executed at the current step
func (t *Txn) scheduleOp(step int, fn func() error) {
	t.operations = append(t.operations, scheduledOp{
		step: step,
		fn:   fn,
	})
}

// TxnStepExecutor provides methods to schedule database operations at a specific step
type TxnStepExecutor struct {
	txn  *Txn
	step int
}

// BeginTx schedules a BeginTx operation at this step
func (s *TxnStepExecutor) BeginTx() {
	s.txn.scheduleOp(s.step, func() error {
		txnId, err := s.txn.db.BeginTx("READ_UNCOMMITTED")
		if err != nil {
			return err
		}
		s.txn.txnId = txnId
		return nil
	})
}

// Set schedules a Set operation at this step
func (s *TxnStepExecutor) Set(key, value int) {
	s.txn.scheduleOp(s.step, func() error {
		return s.txn.db.Set(s.txn.txnId, key, value)
	})
}

// Get schedules a Get operation at this step and captures the result
func (s *TxnStepExecutor) Get(key int) {
	s.txn.scheduleOp(s.step, func() error {
		value, err := s.txn.db.Get(s.txn.txnId, key)
		if err != nil {
			return err
		}
		// Store the result
		s.txn.executor.resultStore.store(s.txn.name, s.step, value)
		return nil
	})
}

// Delete schedules a Delete operation at this step
func (s *TxnStepExecutor) Delete(key int) {
	s.txn.scheduleOp(s.step, func() error {
		return s.txn.db.Delete(s.txn.txnId, key)
	})
}

// Commit schedules a Commit operation at this step
func (s *TxnStepExecutor) Commit() {
	s.txn.scheduleOp(s.step, func() error {
		return s.txn.db.Commit(s.txn.txnId)
	})
}

// Rollback schedules a Rollback operation at this step
func (s *TxnStepExecutor) Rollback() {
	s.txn.scheduleOp(s.step, func() error {
		return s.txn.db.Rollback(s.txn.txnId)
	})
}

// Results stores the results of Get operations indexed by transaction name and step
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

// store saves a result for a specific transaction and step
func (r *Results) store(txnName string, step int, value int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.data[txnName] == nil {
		r.data[txnName] = make(map[int]int)
	}
	r.data[txnName][step] = value
}

// Get retrieves the result of a Get operation for a specific transaction and step
func (r *Results) Get(txnName string, step int) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if txnData, ok := r.data[txnName]; ok {
		return txnData[step]
	}
	return 0
}
