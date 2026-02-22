# MIT 6.5840 Distributed Systems

Implemented Raft consensus and a fault-tolerant distributed KV store in Go with linearizable semantics.

## Part 1: Raft (`src/raft/`)

Raft is a consensus algorithm that replicates a log across a cluster of servers. This implementation covers:

- **Leader election**: servers use randomized timeouts to elect a leader via majority vote. Terms prevent stale leaders from causing split-brain.
- **Log replication**: the leader appends entries and replicates them to followers via `AppendEntries` RPCs. An entry is committed once a majority acknowledges it.
- **Persistence**: `currentTerm`, `votedFor`, and the log are persisted to disk before responding to RPCs, so a crashed server can rejoin without violating safety.
- **Log compaction (snapshots)**: the state machine can snapshot its state at a log index; Raft discards preceding entries and sends the snapshot to lagging followers via `InstallSnapshot`.

## Part 2: Fault-tolerant KV store (`src/kvraft/`)

A replicated key/value service layered on top of Raft. Clients call `Get`, `Put`, and `Append`; the service remains available as long as a majority of servers are alive.

- **Linearizability**: every operation appears to execute instantaneously at some point between its invocation and response, even under concurrent clients and server failures.
- **Duplicate detection**: clerks retry RPCs on failure. Each request carries a `(clientId, seqNum)` pair; servers track the latest sequence number per client and skip re-execution of already-applied operations.
- **Snapshotting**: when the Raft log grows too large, the KV server snapshots the key/value map and the duplicate-detection table, then asks Raft to discard the prefix.

## Running the tests

```bash
# Raft
cd src/raft && go test -race

# KV store (Part A: no snapshots, Part B: with snapshots)
cd src/kvraft && go test -race -run 3A
cd src/kvraft && go test -race -run 3B
```
