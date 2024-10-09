package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvMap                   map[string]string
	clientDoneRequestNumMap map[int64]int
	clientRequestValueMap   map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.kvMap[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clientDoneRequestNumMap[args.ClientID] != args.RequestID {
		kv.kvMap[args.Key] = args.Value
		kv.clientDoneRequestNumMap[args.ClientID] = args.RequestID
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clientDoneRequestNumMap[args.ClientID] == args.RequestID {
		reply.Value = kv.clientRequestValueMap[args.ClientID]
	} else {
		value, ok := kv.kvMap[args.Key]
		if ok {
			reply.Value = value
			kv.kvMap[args.Key] = value + args.Value
		} else {
			reply.Value = ""
			kv.kvMap[args.Key] = args.Value
		}
		kv.clientDoneRequestNumMap[args.ClientID] = args.RequestID
		kv.clientRequestValueMap[args.ClientID] = reply.Value
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.clientDoneRequestNumMap = make(map[int64]int)
	kv.clientRequestValueMap = make(map[int64]string)
	return kv
}
