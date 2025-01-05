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
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	RequestID int
	ClientID  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister

	kvMap                   map[string]string
	clientDoneRequestNumMap map[int64]int
	//clientRequestValueMap   map[int64]string

	//appliedOps []Op
}

//func (kv *KVServer) Contains(targetOp Op) bool {
//	for _, op := range kv.appliedOps {
//		if op == targetOp {
//			return true
//		}
//	}
//	return false
//}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if args.RequestID <= kv.clientDoneRequestNumMap[args.ClientID] {
		reply.Err = OK
		reply.Value = kv.kvMap[args.Key]
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		Value:     "",
		RequestID: args.RequestID,
		ClientID:  args.ClientID,
	}

	err := kv.attemptCommitOp(op)
	kv.mu.Lock()
	if err == OK {
		reply.Value = kv.kvMap[args.Key]
	}
	reply.Err = err
	kv.mu.Unlock()

	//if err == OK {
	//DPrintf("[server %v] kvmap %v, clientDoneRequestNumMap %v, clientRequestValueMap %v", kv.me, kv.kvMap, kv.clientDoneRequestNumMap, kv.clientRequestValueMap)
	//}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if args.RequestID <= kv.clientDoneRequestNumMap[args.ClientID] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	op := Op{
		Type:      args.Type,
		Key:       args.Key,
		Value:     args.Value,
		RequestID: args.RequestID,
		ClientID:  args.ClientID,
	}

	err := kv.attemptCommitOp(op)
	reply.Err = err
	//if err == OK {
	//DPrintf("[server %v] kvmap %v, clientDoneRequestNumMap %v, clientRequestValueMap %v", kv.me, kv.kvMap, kv.clientDoneRequestNumMap, kv.clientRequestValueMap)
	//}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if args.RequestID <= kv.clientDoneRequestNumMap[args.ClientID] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	op := Op{
		Type:      args.Type,
		Key:       args.Key,
		Value:     args.Value,
		RequestID: args.RequestID,
		ClientID:  args.ClientID,
	}

	err := kv.attemptCommitOp(op)
	reply.Err = err

	//DPrintf("Append %v reply %v err %v", args, reply, err)
	//if err == OK {
	//	DPrintf("[server %v] kvmap %v, clientDoneRequestNumMap %v, clientRequestValueMap %v", kv.me, kv.kvMap, kv.clientDoneRequestNumMap, kv.clientRequestValueMap)
	//}
}

func (kv *KVServer) attemptCommitOp(op Op) Err {
	_, term, _ := kv.rf.Start(op)

	startTime := time.Now()

	for {
		kv.mu.Lock()
		if kv.killed() || time.Now().Sub(startTime) > 1*time.Second {
			kv.mu.Unlock()
			return ErrWrongLeader
		}

		currTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != currTerm {
			kv.mu.Unlock()
			return ErrWrongLeader
		}

		if op.RequestID <= kv.clientDoneRequestNumMap[op.ClientID] {
			kv.mu.Unlock()
			break
		}

		kv.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
	}
	return OK
}

func (kv *KVServer) checkCommits() {
	for kv.killed() == false {
		appliedMsg := <-kv.applyCh

		DPrintf("[server %v] mivige message %v", kv.me, appliedMsg)

		if appliedMsg.CommandValid {
			commitedOp := appliedMsg.Command.(Op)

			kv.mu.Lock()
			requestID, ok := kv.clientDoneRequestNumMap[commitedOp.ClientID]
			if !ok || requestID < commitedOp.RequestID {
				//if !kv.Contains(commitedOp) {
				//kv.appliedOps = append(kv.appliedOps, commitedOp)

				DPrintf("[server %v] appliedMsg %v commitedOp %v", kv.me, appliedMsg, commitedOp)

				kv.clientDoneRequestNumMap[commitedOp.ClientID] = commitedOp.RequestID
				//kv.clientRequestValueMap[commitedOp.ClientID] = commitedOp.Value

				if commitedOp.Type == "Put" {
					kv.kvMap[commitedOp.Key] = commitedOp.Value
				} else if commitedOp.Type == "Append" {
					kv.kvMap[commitedOp.Key] = kv.kvMap[commitedOp.Key] + commitedOp.Value
				}

			}
			kv.mu.Unlock()

			kv.controlRaftStateSize(appliedMsg.CommandIndex)

		} else if appliedMsg.SnapshotValid {
			kv.readFromSnapshot(appliedMsg.Snapshot)
			DPrintf("[server %v] wavikitxeee", kv.me)
		}

		time.Sleep(1 * time.Millisecond)
		DPrintf("[server %v] gavigvidzeee", kv.me)
	}
}

func (kv *KVServer) readFromSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvMap map[string]string
	var clientDoneRequestNumMap map[int64]int

	if d.Decode(&kvMap) != nil || d.Decode(&clientDoneRequestNumMap) != nil {
		panic("Error decoding persisted state")
	} else {
		//kv.mu.Lock()

		DPrintf("[server %v] readFromSnapshot", kv.me)
		kv.kvMap = kvMap
		kv.clientDoneRequestNumMap = clientDoneRequestNumMap
		DPrintf("[server %v] readFromSnapshot kvMap %v clientDoneRequestNumMap %v", kv.me, kv.kvMap, kv.clientDoneRequestNumMap)

		//kv.mu.Unlock()
	}
}

func (kv *KVServer) controlRaftStateSize(index int) {
	//for kv.killed() == false {
	kv.mu.Lock()
	DPrintf("[server %v] controlRaftStateSize index=%v size %v, maxSize %v", kv.me, index, kv.persister.RaftStateSize(), kv.maxraftstate)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.kvMap)
	e.Encode(kv.clientDoneRequestNumMap)

	state := w.Bytes()

	if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate-len(state) {
		kv.rf.Snapshot(index, state)
	}
	kv.mu.Unlock()

	//	time.Sleep(1 * time.Millisecond)
	//}
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
	// Your code here, if desired.
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
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.clientDoneRequestNumMap = make(map[int64]int)
	//kv.clientRequestValueMap = make(map[int64]string)
	//kv.appliedOps = make([]Op, 0)

	kv.readFromSnapshot(persister.ReadSnapshot())

	go kv.checkCommits()
	//go kv.controlRaftStateSize()
	return kv
}
