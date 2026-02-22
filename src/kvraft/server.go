package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type     string // "Get", "Put", "Append"
	Key      string
	Value    string
	ClientId int64
	SeqNum   int
}

type notifyMsg struct {
	err      Err
	value    string
	clientId int64
	seqNum   int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	persister   *raft.Persister
	lastApplied int

	store     map[string]string
	lastSeq   map[int64]int
	lastReply map[int64]string
	notifyCh  map[int]chan notifyMsg
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	// Fast path: already applied (handles retries of committed ops)
	if kv.lastSeq[args.ClientId] >= args.SeqNum {
		reply.Err = OK
		reply.Value = kv.lastReply[args.ClientId]
		kv.mu.Unlock()
		return
	}

	op := Op{Type: "Get", Key: args.Key, ClientId: args.ClientId, SeqNum: args.SeqNum}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan notifyMsg, 1)
	kv.notifyCh[index] = ch
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		if msg.clientId == args.ClientId && msg.seqNum == args.SeqNum {
			reply.Err = msg.err
			reply.Value = msg.value
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// Fast path: already applied (handles retries of committed ops)
	if kv.lastSeq[args.ClientId] >= args.SeqNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, SeqNum: args.SeqNum}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan notifyMsg, 1)
	kv.notifyCh[index] = ch
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		if msg.clientId == args.ClientId && msg.seqNum == args.SeqNum {
			reply.Err = msg.err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.SnapshotValid {
			kv.mu.Lock()
			kv.installSnapshot(msg.Snapshot, msg.SnapshotIndex)
			kv.mu.Unlock()
			continue
		}

		if !msg.CommandValid {
			continue
		}

		op := msg.Command.(Op)
		index := msg.CommandIndex

		kv.mu.Lock()

		var result notifyMsg
		result.clientId = op.ClientId
		result.seqNum = op.SeqNum

		if kv.lastSeq[op.ClientId] >= op.SeqNum {
			// duplicate: return cached result
			result.err = OK
			result.value = kv.lastReply[op.ClientId]
		} else {
			switch op.Type {
			case "Put":
				kv.store[op.Key] = op.Value
				result.err = OK
			case "Append":
				kv.store[op.Key] += op.Value
				result.err = OK
			case "Get":
				v, ok := kv.store[op.Key]
				if ok {
					result.err = OK
					result.value = v
				} else {
					result.err = ErrNoKey
				}
			}
			kv.lastSeq[op.ClientId] = op.SeqNum
			kv.lastReply[op.ClientId] = result.value
		}

		if ch, ok := kv.notifyCh[index]; ok {
			ch <- result
			delete(kv.notifyCh, index)
		}

		kv.lastApplied = index
		if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.takeSnapshot(index)
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.store)
	e.Encode(kv.lastSeq)
	e.Encode(kv.lastReply)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) installSnapshot(data []byte, index int) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var store map[string]string
	var lastSeq map[int64]int
	var lastReply map[int64]string
	if d.Decode(&store) != nil || d.Decode(&lastSeq) != nil || d.Decode(&lastReply) != nil {
		log.Fatalf("kvserver %d: snapshot decode failed", kv.me)
	}
	kv.store = store
	kv.lastSeq = lastSeq
	kv.lastReply = lastReply
	kv.lastApplied = index
	for idx, ch := range kv.notifyCh {
		if idx <= index {
			ch <- notifyMsg{err: ErrWrongLeader}
			delete(kv.notifyCh, idx)
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.store = make(map[string]string)
	kv.lastSeq = make(map[int64]int)
	kv.lastReply = make(map[int64]string)
	kv.notifyCh = make(map[int]chan notifyMsg)

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()

	return kv
}
