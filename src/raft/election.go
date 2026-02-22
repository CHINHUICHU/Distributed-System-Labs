package raft

import (
	"sync"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	LastLogIdx  int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	// Your code here (2A, 2B)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// check if candidate's log is more up-to-date
	logLen := len(rf.log)
	isUpToDate := true
	if logLen > 0 {
		lastEntry := rf.log[logLen-1]
		isUpToDate = args.LastLogTerm > lastEntry.Term || (args.LastLogTerm == lastEntry.Term && args.LastLogIdx >= rf.logToRaftIndex(logLen-1))
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastContact = time.Now()
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.role == Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	rf.role = Candidate
	rf.currentTerm++
	rf.lastContact = time.Now()
	rf.votedFor = rf.me
	rf.persist()
	term := rf.currentTerm
	rf.mu.Unlock()

	var (
		mu    sync.Mutex
		votes = 1 // self-vote
		once  sync.Once
	)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			lastRaftIdx := -1
			lastLogTerm := 0
			if ll := len(rf.log); ll > 0 {
				lastRaftIdx = rf.logToRaftIndex(ll - 1)
				lastLogTerm = rf.log[ll-1].Term
			}
			args := &RequestVoteArgs{
				Term:        term,
				CandidateId: rf.me,
				LastLogIdx:  lastRaftIdx,
				LastLogTerm: lastLogTerm,
			}
			rf.mu.Unlock()

			reply := &RequestVoteReply{}
			if !sendRPC(func() { rf.sendRequestVote(peer, args, reply) }, RpcTimeout) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentTerm != term || rf.role != Candidate {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				rf.persist()
				return
			}
			if reply.VoteGranted {
				mu.Lock()
				votes++
				won := votes > len(rf.peers)/2
				mu.Unlock()

				if won {
					once.Do(func() {
						rf.becomeLeader()
						go rf.reachAgreement()
					})
				}
			}
		}(peer)
	}
}
