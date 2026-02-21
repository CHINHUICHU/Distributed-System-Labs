package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// AE RPC step1: process term
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else if rf.role == Candidate {
		// Same term but we're a candidate — a valid leader exists, step down.
		rf.role = Follower
		rf.nextIndex = nil
		rf.matchIndex = nil
	}

	reply.Term = rf.currentTerm

	lastIdx := args.PrevLogIndex + len(args.Entries)

	// Early exit: all entries in this RPC are already covered by our snapshot.
	if lastIdx < rf.lastIncludedIndex {
		return
	}

	rf.lastContact = time.Now()

	// The leader may not know how far ahead our snapshot is. When the RPC's
	// entry range overlaps with our snapshot, we trim the already-snapshotted
	// portion so the rest of the handler only sees entries we don't yet have.
	//
	// Case 1: the RPC ends exactly at our snapshot boundary.
	// Treat the final entry as the new prevLog so the conflict check below
	// verifies agreement at the boundary.
	if lastIdx == rf.lastIncludedIndex && rf.lastIncludedIndex > 0 {
		args.PrevLogIndex = lastIdx
		args.PrevLogTerm = args.Entries[len(args.Entries)-1].Term
		args.Entries = args.Entries[len(args.Entries)-1:]
	} else if args.PrevLogIndex < rf.lastIncludedIndex && lastIdx > rf.lastIncludedIndex {
		// Case 2: the RPC spans across our snapshot boundary.
		// Skip entries already in the snapshot; start from lastIncludedIndex+1.
		newPrev := rf.lastIncludedIndex - args.PrevLogIndex - 1
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = args.Entries[newPrev].Term
		args.Entries = args.Entries[newPrev+1:]
	}

	// process RPC
	// AE RPC step 2: check if entry match at prevLogIndex and prevLogTerm
	if lastRaftIndex := rf.logToRaftIndex(len(rf.log) - 1); args.PrevLogIndex > lastRaftIndex {
		reply.ConflictIndex = lastRaftIndex + 1
		return
	} else if args.PrevLogIndex > rf.logToRaftIndex(0) {
		logIndex := rf.raftToLogIndex(args.PrevLogIndex)
		if e := rf.log[logIndex]; e.Term != args.PrevLogTerm {
			reply.ConflictTerm = e.Term
			for i, entry := range rf.log {
				if entry.Term == e.Term {
					reply.ConflictIndex = rf.logToRaftIndex(i)
					return
				}
			}
		}
	}

	newEntries := args.Entries
	match := 0

	// AE RPC step 3: truncate follower's if conflicting
	for i := 0; i < len(args.Entries); i++ {
		logIdx := rf.raftToLogIndex(args.PrevLogIndex + i + 1)
		if logIdx >= 0 && logIdx < len(rf.log) {
			if e := rf.log[logIdx]; e.Term != args.Entries[i].Term || e.Command != args.Entries[i].Command {
				for i := logIdx; i < len(rf.log); i++ {
					delete(rf.seen, rf.log[i].Command)
				}
				rf.log = rf.log[:logIdx]
				break
			} else {
				match++
			}
		}
	}

	newEntries = newEntries[match:]

	// AE RPC step 4: append new entries, deduplicating via the seen map.
	// seen[cmd] holds the raft index where cmd was previously appended.
	// If we've already recorded this command at a live log position, update
	// its term in place rather than creating a duplicate entry.
	for _, e := range newEntries {
		if prevIdx, ok := rf.seen[e.Command]; ok && prevIdx > 0 && prevIdx < rf.logToRaftIndex(len(rf.log)) &&
			rf.log[rf.raftToLogIndex(prevIdx)].Command == e.Command {
			entry := Entry{
				Term:    reply.Term,
				Command: e.Command,
			}
			rf.log[rf.raftToLogIndex(prevIdx)] = entry
		} else {
			rf.log = append(rf.log, e)
			rf.seen[e.Command] = rf.logToRaftIndex(len(rf.log) - 1)
		}
	}

	// AE PRC step 5: check commit index
	if ci := rf.commitIndex; args.LeaderCommit > ci {
		ci = args.LeaderCommit
		lastIdx := rf.logToRaftIndex(len(rf.log) - 1)
		if lastIdx < ci {
			ci = lastIdx
		}
		rf.commitIndex = ci
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
