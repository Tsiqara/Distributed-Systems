package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID       int64
	requestCounter int
	lastLeader     int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand()
	ck.requestCounter = 1
	ck.lastLeader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	var reply GetReply
	numServers := len(ck.servers)
	serverIdx := ck.lastLeader
	for serverIdx < numServers {
		reply = GetReply{}
		args := GetArgs{key, ck.requestCounter, ck.clientID}
		ok := ck.servers[serverIdx].Call("KVServer.Get", &args, &reply)
		//DPrintf("[server %v] Call Get %v Reply %v", serverIdx, args, reply)
		if ok && reply.Err != ErrWrongLeader {
			ck.lastLeader = serverIdx
			break
		}
		serverIdx++
		serverIdx %= numServers
	}
	ck.requestCounter++
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//var reply PutAppendReply
	numServers := len(ck.servers)
	serverIdx := ck.lastLeader
	for serverIdx < numServers {
		args := PutAppendArgs{key, value, op, ck.requestCounter, ck.clientID}
		reply := PutAppendReply{}
		ok := ck.servers[serverIdx].Call("KVServer."+op, &args, &reply)
		//DPrintf("[server %v] Call %v args %v reply %v ", serverIdx, op, args, reply)
		if ok && reply.Err != ErrWrongLeader {
			ck.lastLeader = serverIdx
			break
		}
		serverIdx++
		serverIdx %= numServers
	}
	ck.requestCounter++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
