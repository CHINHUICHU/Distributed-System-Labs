package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
const (
	CheckInterval  = 10 * time.Millisecond
	RpcTimeout     = 150 * time.Millisecond
	AppendInterval = 10 * time.Millisecond
	RaftStartIndex = 1
	RpcInterval    = 20 * time.Millisecond
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	role        Role
	lastContact time.Time

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int

	log         []Entry
	commitIndex int
	lastApplied int

	applych chan ApplyMsg

	nextIndex  []int
	matchIndex []int
	seen       map[interface{}]int

	// all the log "before" this index has been snapshoted
	// the first index of rf.log will be snapshotBefore
	// snapshotBefore    int
	latestSnapshot    []byte
	lastIncludedTerm  int
	lastIncludedIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		timeout := time.Duration(300+rand.Int63()%200) * time.Millisecond
		time.Sleep(timeout)

		rf.mu.Lock()
		timedOut := rf.role != Leader && time.Since(rf.lastContact) >= timeout
		rf.mu.Unlock()

		if timedOut {
			go rf.startElection()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		votedFor:    -1,
		lastContact: time.Now(),
		applych:     applyCh,
		log:         make([]Entry, 0),
		seen:        make(map[interface{}]int),
	}

	rf.log = append(rf.log, Entry{})

	// Your initialization code here (2A, 2B, 2C).
	/*
		create a background goroutine that will kick off leader election periodically
		by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	*/
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.latestSnapshot = persister.ReadSnapshot()
	rf.lastApplied = rf.lastIncludedIndex

	for i, e := range rf.log {
		rf.seen[e.Command] = rf.logToRaftIndex(i)
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	// only leader run the routines

	go rf.checkCommitIndex()

	return rf
}

func (rf *Raft) applier() {
	for !rf.killed() {
		applyEntries := make([]ApplyMsg, 0)
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			logIdx := rf.raftToLogIndex(rf.lastApplied)
			valid := logIdx >= 0 && logIdx < len(rf.log) && rf.log[logIdx].Command != nil
			if !valid {
				break
			}
			e := rf.log[logIdx]
			am := ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: rf.lastApplied,
			}
			applyEntries = append(applyEntries, am)
		}
		rf.mu.Unlock()
		for _, am := range applyEntries {
			rf.applych <- am
		}
		time.Sleep(CheckInterval)
	}
}

func (rf *Raft) checkCommitIndex() {
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.killed() && rf.role == Leader && rf.matchIndex != nil && len(rf.log) > 0 {
			for i := rf.logToRaftIndex(len(rf.log) - 1); i > rf.commitIndex; i-- {
				count := 0
				for p := range rf.peers {
					if rf.matchIndex[p] >= i {
						count++
					}
				}
				if count > len(rf.peers)/2 {
					logIdx := rf.raftToLogIndex(i)
					valid := logIdx >= 0 && logIdx < len(rf.log)
					if valid && rf.log[logIdx].Term == rf.currentTerm {
						rf.commitIndex = i
					}
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(CheckInterval)
	}
}
