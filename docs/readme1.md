# Database Isolation Level Testing Framework

A Go-based framework for testing database isolation levels by simulating transaction anomalies with controlled interleaving.

## Project Structure

- `db/` - Database implementations at different isolation levels
- `anomalytest/` - Transaction executor and anomaly test cases
  - `transaction_executor.go` - Barrier-based transaction coordination
  - `anomaly_dirty_reads.go` - Dirty read test scenarios
  - `anomaly_dirty_writes.go` - Dirty write test scenarios
  - `anomaly_lost_update.go` - Lost update test scenarios

## The Dirty Writes Testing Problem

Testing dirty writes is tricky because of how real databases handle concurrent writes.

### The Scenario

```
T1: Set(1, 100)           -- T1 writes to key 1
T2: Set(1, 200)           -- T2 tries to write to key 1 (dirty write over T1's uncommitted data)
T2: Set(2, 100)
T1: Set(2, 200)           -- T1 tries to write to key 2 (dirty write over T2's uncommitted data)
T1: Commit
T2: Commit
```

### The Problem

In PostgreSQL (or any database with row-level locking), this leads to **deadlock**:
- T2's `Set(1, 200)` blocks because T1 holds a write lock on key 1
- T1's `Set(2, 200)` blocks because T2 holds a write lock on key 2
- Neither can proceed → deadlock

You can't force the interleaving that would exhibit dirty writes because the locking **prevents** it.

## Solution: WaitForWithTimeout

The `WaitForWithTimeout` barrier allows tests to handle blocking behavior gracefully:

```go
// Instead of blocking forever waiting for T2's writes:
txn1.WaitFor("txn2_wrote_second")

// Use a timeout - if T2 is blocked, continue after timeout:
txn1.WaitForWithTimeout("txn2_wrote_second", 100*time.Millisecond)
```

### Behavior by Isolation Level

| Implementation | What Happens |
|----------------|--------------|
| **No locking** | T2's writes complete immediately → barrier signals → T1 continues → dirty write occurs |
| **With write locks** | T2 blocks on T1's lock → timeout expires → T1 commits → T2 unblocks → no dirty write |

## Key Insight: Real Databases Prevent Dirty Writes

Even at **read uncommitted**, most real databases (PostgreSQL, SQL Server, MySQL/InnoDB) prevent dirty writes using exclusive write locks.

The "uncommitted" in "read uncommitted" refers to **read behavior** (allowing dirty reads), not write behavior.

From the SQL standard and Berenson et al.'s "A Critique of ANSI SQL Isolation Levels":
- **P0 (Dirty Write)** must be prevented at ALL isolation levels
- Only **P1 (Dirty Read)** is allowed at read uncommitted

## Isolation Level Anomaly Matrix

| Isolation Level | Dirty Writes (G0) | Dirty Reads (G1) | Lost Updates | Non-Repeatable Reads | Phantom Reads |
|-----------------|-------------------|------------------|--------------|----------------------|---------------|
| Read Uncommitted | Prevented | Allowed | Allowed | Allowed | Allowed |
| Read Committed | Prevented | Prevented | Allowed | Allowed | Allowed |
| Repeatable Read | Prevented | Prevented | Prevented | Prevented | Allowed |
| Serializable | Prevented | Prevented | Prevented | Prevented | Prevented |

## Two Implementation Strategy

For educational purposes, maintain two implementations:

### 1. Naive Implementation (`simpledb_read_uncommitted.go`)
- No write locks
- Allows dirty writes
- Useful for **demonstrating** the anomaly

### 2. Realistic Implementation (`simpledb_read_uncommitted_write_lock.go`)
- Includes write locks held until commit/rollback
- Prevents dirty writes (like real databases)
- Models realistic database behavior

### Test Results

| Test | Naive Implementation | Realistic Implementation |
|------|---------------------|-------------------------|
| Dirty Writes Test | FAILS (anomaly occurs) | PASSES (anomaly prevented) |
| Dirty Reads Test | FAILS (anomaly occurs) | FAILS (anomaly occurs) |

Both implementations allow dirty reads (that's what read uncommitted means), but only the naive one allows dirty writes.

## References

- [Hermitage: Testing Transaction Isolation Levels](https://github.com/ept/hermitage)
- [A Critique of ANSI SQL Isolation Levels](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf)
- [Stack Overflow: Dirty Writes Explanation](https://stackoverflow.com/a/66181531)

Next Steps:
- Implement read committed.