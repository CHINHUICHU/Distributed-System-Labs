package raft

import (
	"time"
)

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.role == Leader && rf.nextIndex != nil && rf.matchIndex != nil && !rf.killed()
	if !isLeader {
		return index, term, isLeader
	}

	defer rf.persist()

	term = rf.currentTerm
	entry := Entry{
		Term:    term,
		Command: command,
	}

	if raftIndex, ok := rf.seen[command]; ok {
		logIndex := rf.raftToLogIndex(raftIndex)
		rf.log[logIndex] = entry
		index = raftIndex
	} else {
		rf.log = append(rf.log, entry)
		index = rf.logToRaftIndex(len(rf.log) - 1)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.seen[entry.Command] = index
	}

	return index, term, isLeader
}

func (rf *Raft) reachAgreement() {
	rf.mu.Lock()
	start := rf.currentTerm
	rf.mu.Unlock()

	for !rf.killed() {
		for peer := range rf.peers {
			rf.mu.Lock()
			if peer != rf.me && rf.nextIndex != nil && rf.matchIndex != nil && rf.role == Leader && start == rf.currentTerm {
				rf.mu.Unlock()
				go rf.appendLogRoutine(peer, start)
				time.Sleep(RpcInterval)
			} else if rf.role != Leader || rf.currentTerm != start {
				rf.mu.Unlock()
				return
			} else {
				rf.mu.Unlock() // peer == rf.me, skip
			}
		}
	}
}

// appendLogRoutine replicates entries to peer for the given leader term.
// It runs in a loop: sends AppendEntries (or delegates to installSnapshotForPeer
// if the peer is behind the snapshot), adjusts nextIndex on rejection, and
// exits when the peer catches up, the leader steps down, or the term changes.
func (rf *Raft) appendLogRoutine(peer int, term int) {
	for {
		rf.mu.Lock()
		if rf.role != Leader || rf.nextIndex == nil || rf.matchIndex == nil || rf.killed() || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		// Clamp nextIndex[peer] so it never points past the end of the log.
		if next := rf.logToRaftIndex(len(rf.log)); rf.nextIndex[peer] > next {
			rf.nextIndex[peer] = next
		}

		nextLogIndex := rf.raftToLogIndex(rf.nextIndex[peer])
		if nextLogIndex < 0 {
			// peer is behind our snapshot; send the snapshot instead.
			rf.mu.Unlock()
			rf.installSnapshotForPeer(peer, term)
			return
		}

		// Build the AppendEntries arguments.
		prevLogTerm := 0
		if nextLogIndex-1 >= 0 {
			prevLogTerm = rf.log[nextLogIndex-1].Term
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[peer] - 1,
			PrevLogTerm:  prevLogTerm,
			Entries:      rf.log[nextLogIndex:],
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		rf.mu.Unlock()

		ok := sendRPC(func() { rf.sendAppendEntries(peer, args, reply) }, RpcTimeout)
		if !ok {
			time.Sleep(RpcInterval)
			continue
		}

		rf.mu.Lock()
		if rf.role != Leader || rf.nextIndex == nil || rf.matchIndex == nil {
			rf.mu.Unlock()
			return
		}
		// Discard stale replies (our state already advanced past this RPC).
		if rf.currentTerm != args.Term || rf.nextIndex[peer] != args.PrevLogIndex+1 {
			rf.mu.Unlock()
			time.Sleep(RpcInterval)
			continue
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			matchIdx := args.PrevLogIndex + len(args.Entries)
			if matchIdx > rf.matchIndex[peer] {
				rf.matchIndex[peer] = matchIdx
				rf.nextIndex[peer] = matchIdx + 1
			}
			rf.mu.Unlock()
			return
		}

		// Follower rejected the entry: fast-back nextIndex using conflict info.
		newNext := -1
		for j, e := range rf.log {
			if e.Term == reply.ConflictTerm {
				newNext = rf.logToRaftIndex(j) + 1
			}
		}
		if newNext == -1 {
			newNext = reply.ConflictIndex
		}
		if newNext < rf.nextIndex[peer] {
			rf.nextIndex[peer] = newNext
		} else {
			rf.nextIndex[peer]--
		}
		rf.mu.Unlock()
		time.Sleep(RpcInterval)
	}
}

// installSnapshotForPeer sends the current snapshot to peer, retrying on timeout.
// Returns once the peer acknowledges, rejects with a higher term, or this
// server is no longer the leader for term.
func (rf *Raft) installSnapshotForPeer(peer, term int) {
	for {
		rf.mu.Lock()
		if rf.role != Leader || rf.nextIndex == nil || rf.matchIndex == nil || rf.killed() || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.latestSnapshot,
		}
		rf.mu.Unlock()

		reply := &InstallSnapshotReply{}
		ok := sendRPC(func() { rf.sendInstallSnapshot(peer, args, reply) }, RpcTimeout*2)
		if !ok {
			time.Sleep(RpcInterval)
			continue
		}

		rf.mu.Lock()
		isStale := rf.currentTerm != args.Term || rf.role != Leader || rf.lastIncludedIndex != args.LastIncludedIndex
		if !isStale {
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				rf.persist()
			} else if rf.lastIncludedIndex > rf.matchIndex[peer] {
				rf.nextIndex[peer] = rf.lastIncludedIndex + 1
				rf.matchIndex[peer] = rf.lastIncludedIndex
			}
		}
		rf.mu.Unlock()
		return
	}
}
