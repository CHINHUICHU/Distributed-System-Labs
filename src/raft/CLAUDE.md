# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all Raft tests
go test

# Run tests for a specific part
go test -run 3A    # Leader election
go test -run 3B    # Log replication
go test -run 3C    # Persistence
go test -run 3D    # Log compaction / snapshots

# Run with race detector (recommended during development)
go test -race -run 3A

# Run a single named test
go test -run TestInitialElection3A -v

# Run repeatedly to check for flakiness
for i in {1..5}; do go test -run 3D; done
```

The `test-script.sh` script runs `go test -run 2D` in a loop (outdated naming — use `3D` instead).

## Architecture

This is a Go implementation of the Raft consensus protocol for MIT 6.5840 (Distributed Systems). The implementation is split across several files:

### Core struct (`raft.go`)
- `Raft` struct holds all peer state: `currentTerm`, `votedFor`, `log`, `commitIndex`, `lastApplied`, `nextIndex[]`, `matchIndex[]`, `role`, `latestSnapshot`, `lastIncludedIndex`, `lastIncludedTerm`
- `Make()` initializes the peer and starts three background goroutines: `ticker()` (election timeout), `applier()` (sends committed entries to `applyCh`), `checkCommitIndex()` (leader advances commit index)
- `seen map[interface{}]int` tracks command → raft-index for deduplication

### File responsibilities
| File | Responsibility |
|------|---------------|
| `raft.go` | Raft struct, Make, applier, checkCommitIndex, ticker |
| `state.go` | Role enum, Kill/killed, GetState, index conversion helpers |
| `election.go` | RequestVote RPC + handler, startElection |
| `append_entries.go` | AppendEntries RPC args/reply + handler |
| `agreement.go` | Start(), reachAgreement(), appendLogRoutine (leader replication loop) |
| `snapshot.go` | Snapshot(), InstallSnapshot RPC + handler |
| `persist.go` | persist() and readPersist() using labgob encoder |
| `util.go` | Debug flag, DPrintf, Timestamp() |
| `config.go` | Test harness (course-provided, do not modify) |
| `persister.go` | Persister object (course-provided, do not modify) |

### Log indexing
The log is compacted via snapshots. Two index spaces exist:
- **Raft index**: globally monotonic, starts at 1 (`RaftStartIndex`)
- **Log (slice) index**: position within `rf.log[]`

Conversion helpers in `state.go`:
- `raftToLogIndex(raftIdx) = raftIdx - lastIncludedIndex`
- `logToRaftIndex(logIdx) = logIdx + lastIncludedIndex`

`rf.log[0]` is always a sentinel/dummy entry. After a snapshot at raft-index N, `rf.log[0]` represents the snapshot boundary, and `lastIncludedIndex = N`.

### Concurrency model
- Single mutex `rf.mu` protects all state. `persist()` must be called while holding the lock (it doesn't acquire it internally).
- RPCs are sent in goroutines. The caller uses a channel + `select` with `time.After(RpcTimeout)` to avoid blocking indefinitely.
- Leader per-peer replication runs in `appendLogRoutine(peer, term)` goroutines spawned from `reachAgreement()`.
- `nextIndex[]` and `matchIndex[]` are nil when the peer is not leader; code checks for nil before use.

### Timing constants (`raft.go`)
- `CheckInterval = 10ms` — polling interval for background loops
- `AppendInterval = 10ms` — heartbeat send interval
- `RpcTimeout = 150ms` — timeout waiting for an RPC reply
- `RpcInterval = 20ms` — delay between sending RPCs to successive peers
- Election timeout: random 300–500ms (`ticker()`)
