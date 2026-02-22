# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all kvraft tests (from src/kvraft/)
go test

# Run Part A tests (no snapshots)
go test -run 3A

# Run Part B tests (with snapshots)
go test -run 3B

# Run with race detector (always use during development)
go test -race -run 3A

# Run a single named test
go test -run TestBasic3A -v

# Run repeatedly to check for flakiness
for i in {1..5}; do go test -run 3A; done
```

Note: test functions use `3A`/`3B` suffixes (the lab was renumbered from 3 to 4, but the test file was not updated). The spec references `4A`/`4B` but the actual `-run` filter must match test function names like `TestBasic3A`.

## Architecture

This is MIT 6.5840 Lab 4: a fault-tolerant key/value storage service layered on top of the Raft implementation in `../raft/`.

### Layers

```
Clerk (client.go)
    ↓  RPC: Get / PutAppend
KVServer (server.go)
    ↓  raft.Start() / applyCh
Raft  (../raft/)
    ↓  persister.Save() / ReadSnapshot()
Persister
```

### Files to implement

| File | Role |
|------|------|
| `client.go` | `Clerk` — sends RPCs to kvservers, retries on failure, remembers last known leader |
| `server.go` | `KVServer` — submits ops to Raft via `Start()`, applies committed ops from `applyCh`, deduplicates client requests |
| `common.go` | Shared RPC arg/reply structs (`PutAppendArgs`, `GetArgs`, etc.) and error constants |
| `config.go` | Test harness (course-provided, do not modify) |
| `test_test.go` | Tests — `GenericTest` drives most scenarios with flags for unreliable net, crashes, partitions, and snapshots |

### Key design requirements

**Duplicate detection**: Clerks may retry RPCs (e.g., after a leader change), so the server must deduplicate. Each client should carry a unique ID and a sequence number; the server tracks the highest sequence number seen per client. Since a client makes only one call at a time, a new request from a client implies the previous one was received — the server can free old duplicate state.

**Leader detection**: `KVServer.Get`/`PutAppend` call `rf.Start()`. If the server is not the leader, `Start()` returns `isLeader=false` and the handler replies `ErrWrongLeader`. The Clerk retries a different server. The Clerk should remember the last successful leader to avoid scanning all servers every RPC.

**Waiting for commitment**: After `Start()` returns an index, the handler must wait until an `ApplyMsg` with that index arrives on `applyCh` (or detect that leadership was lost). Use a per-index channel or condition variable; wake it from the applier goroutine. Watch for the wrong command appearing at the expected index (meaning a new leader overwrote it) — return `ErrWrongLeader` so the Clerk retries.

**Snapshots (Part B)**: When `persister.RaftStateSize()` approaches `maxraftstate`, call `rf.Snapshot(index, data)` where `data` is a GOB-encoded snapshot of the KV map plus the duplicate-detection table. On restart, restore state from `persister.ReadSnapshot()`. Also handle `ApplyMsg.SnapshotValid == true` from the applier when Raft installs a remote snapshot.

### Raft API surface used by kvraft

- `raft.Make(servers, me, persister, applyCh)` — create peer
- `rf.Start(command)` → `(index, term, isLeader)` — submit a log entry
- `rf.GetState()` → `(term, isLeader)`
- `rf.Snapshot(index, snapshot []byte)` — trim log and save snapshot
- `persister.RaftStateSize()` — check current Raft state size
- `persister.ReadSnapshot()` — read latest snapshot on startup
- `applyCh` delivers `ApplyMsg` with either `CommandValid=true` (new commit) or `SnapshotValid=true` (snapshot installed by Raft)

### Raft timing constants (relevant for test timing)

Defined in `../raft/raft.go`: heartbeat interval `AppendInterval = 50ms`. The speed test requires >3 ops per heartbeat interval, so the KV layer must not add unnecessary latency (avoid sleeping in handlers).
