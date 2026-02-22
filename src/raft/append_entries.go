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

	rf.lastContact = time.Now()

	// All entries in this RPC are already covered by our snapshot: accept
	// trivially so the leader does not backtrack nextIndex, and update
	// commitIndex from LeaderCommit before returning.
	if lastIdx <= rf.lastIncludedIndex {
		if args.LeaderCommit > rf.commitIndex {
			if last := rf.logToRaftIndex(len(rf.log) - 1); args.LeaderCommit < last {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = last
			}
		}
		reply.Success = true
		return
	}

	// RPC spans the snapshot boundary: skip the already-snapshotted prefix
	// and rebase PrevLog onto lastIncludedIndex.
	if args.PrevLogIndex < rf.lastIncludedIndex {
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

	// AE RPC step 4: append new entries.
	// Followers replicate the log exactly as the leader dictates — no
	// deduplication here. The seen map is updated so Snapshot() can clean up.
	for _, e := range newEntries {
		rf.log = append(rf.log, e)
		rf.seen[e.Command] = rf.logToRaftIndex(len(rf.log) - 1)
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
