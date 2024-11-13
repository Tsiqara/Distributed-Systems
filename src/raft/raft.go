package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
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
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state string

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	lastElectionTime  time.Time
	lastHeartBeatTime time.Time

	votesNum int

	applyCh chan ApplyMsg
}

const minElectionTimeout int64 = 400
const minHeartBeatTimeout int64 = 200
const ElectionTimeoutRange int64 = 200
const HeartBeatTimeoutRange int64 = 100

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == "leader"
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		return
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		return
	}
	err = e.Encode(rf.log)
	if err != nil {
		return
	}
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		fmt.Printf("failed to decode from persister")
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()

	DPrintf("server %v received request vote for term %v from %v", rf.me, args.Term, args.CandidateId)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("server %v's term %v is more then args' %v", rf.me, rf.currentTerm, args.Term)
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votesNum = 0
		rf.votedFor = -1
		rf.persist()
		DPrintf("server %v's term %v is less then args' %v converted to follower", rf.me, rf.currentTerm, args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogTerm := 0
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log)) {
			reply.VoteGranted = true
			rf.lastHeartBeatTime = time.Now()
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.persist()
			DPrintf("server %v vote for %v is %v ", rf.me, args.CandidateId, reply.VoteGranted)
			rf.mu.Unlock()
			return
		}

		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm || rf.killed() {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("server %v refuse appendEntries from %v", rf.me, args.LeaderId)
		rf.mu.Unlock()
		return
	}

	if args.Term >= rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.state = "follower"
		rf.votesNum = 0
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
		rf.lastHeartBeatTime = time.Now()

		if args.Entries == nil {
			if args.LeaderCommit > rf.commitIndex {
				DPrintf("leader %v commit index %v", args.LeaderId, args.LeaderCommit)
				DPrintf("server %v updated commit index from %v to %v", rf.me, rf.commitIndex, min(args.LeaderCommit, len(rf.log)-1))
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			}

			DPrintf("server %v accept heartbeat from %v", rf.me, args.LeaderId)
			rf.mu.Unlock()
			return
		}

		if args.PrevLogIndex >= 0 && (len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.XLen = len(rf.log) - 1
			reply.XTerm = -1
			reply.XIndex = -1
			if len(rf.log) > args.PrevLogIndex {
				reply.XTerm = rf.log[args.PrevLogIndex].Term
				for i := 0; i <= args.PrevLogIndex; i++ {
					if rf.log[i].Term == reply.XTerm {
						reply.XIndex = i
						break
					}
				}
			}

			DPrintf("[server %v] log %v, prevLogIndex %v", rf.me, rf.log, args.PrevLogIndex)
			if len(rf.log) > args.PrevLogIndex {
				DPrintf("my term %v, leader term %v", rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
			}
			DPrintf("[server %v] doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm of %v",
				rf.me, args.LeaderId)
			DPrintf("[server %v] XTerm %v XIndex %v XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
			rf.mu.Unlock()
			return
		}

		//	there are log entries to append
		if args.PrevLogIndex <= len(rf.log)-1 {
			DPrintf("server %v log %v  prevlogInd %v", rf.me, rf.log, args.PrevLogTerm)
			for i := 0; i < len(args.Entries); i++ {
				if args.PrevLogIndex+i+1 < len(rf.log) && rf.log[args.PrevLogIndex+i+1].Term == args.Entries[i].Term {
					DPrintf("server %v rf.log[args.PrevLogIndex+i+1].Term %v = args.Entries[i].Term %v", rf.me, rf.log[args.PrevLogIndex+i+1].Term, args.Entries[i].Term)
					continue
				}
				if args.PrevLogIndex+i+1 < len(rf.log) {
					DPrintf("server %v rf.log[args.PrevLogIndex+i+1].Term %v != args.Entries[i].Term %v", rf.me, rf.log[args.PrevLogIndex+i+1].Term, args.Entries[i].Term)
				}
				DPrintf("server %v truncate log from log %v to %v", rf.me, rf.log, rf.log[:args.PrevLogIndex+i+1])
				rf.log = rf.log[:args.PrevLogIndex+i+1]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				DPrintf("server %v append entries %v log became %v", rf.me, args.Entries[i:], rf.log)
				break
			}

			DPrintf("server %v appended %v entries from %v", rf.me, len(args.Entries), args.LeaderId)
			DPrintf("[server %v]  %v", rf.me, rf.log)
		}

		if args.LeaderCommit > rf.commitIndex {
			DPrintf("leader %v commit index %v", args.LeaderId, args.LeaderCommit)
			DPrintf("server %v updated commit index from %v to %v", rf.me, rf.commitIndex, min(args.LeaderCommit, len(rf.log)-1))
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}

		rf.mu.Unlock()
		return
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()

	if rf.state != "leader" || rf.killed() {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}

	logToAppend := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, logToAppend)
	rf.persist()
	term = rf.currentTerm
	index = len(rf.log) - 1
	DPrintf("%v received command %v", rf.me, command)
	rf.mu.Unlock()

	DPrintf("index %v, term %v, isLeader %v", index, term, isLeader)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	DPrintf("server %v is being killed", rf.me)
	DPrintf("server %v commitInd %v  appliedInd %v log %v", rf.me, rf.commitIndex, rf.lastApplied, rf.log)
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		DPrintf("%v is %v", rf.me, rf.state)
		if rf.state == "leader" {
			DPrintf("leader %v heartbeatTicker tick", rf.me)
			rf.mu.Unlock()
			//go rf.sendAppendEntries()
		} else if rf.state == "follower" {
			rf.votesNum = 0
			rf.votedFor = -1
			rf.persist()
			if time.Now().Sub(rf.lastHeartBeatTime).Milliseconds() > (minHeartBeatTimeout + rand.Int63()%HeartBeatTimeoutRange) {
				rf.mu.Unlock()
				go rf.startElection()
			} else {
				rf.mu.Unlock()
			}
		} else if rf.state == "candidate" {
			if rf.votesNum > len(rf.peers)/2 {
				DPrintf("server %v won the election for term %v with %v votes", rf.me, rf.currentTerm, rf.votesNum)
				rf.mu.Unlock()
				rf.becomeLeader()
			} else if time.Now().Sub(rf.lastElectionTime).Milliseconds() > (minElectionTimeout + rand.Int63()%ElectionTimeoutRange) {
				DPrintf("server %v has %v votes but timed out", rf.me, rf.votesNum)
				rf.mu.Unlock()
				go rf.startElection()
			} else {
				DPrintf("server %v has %v votes but is waiting", rf.me, rf.votesNum)
				rf.mu.Unlock()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) heartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == "leader" {
			rf.mu.Unlock()
			DPrintf("server %v is sending heartBeat", rf.me)
			for {
				rf.mu.Lock()
				if rf.state != "leader" {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				go rf.sendAppendEntries()
				time.Sleep(time.Duration(35) * time.Millisecond)
			}
		} else {
			rf.mu.Unlock()
			ms := 10
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

func (rf *Raft) sendAppendEntries() {
	rf.mu.Lock()

	if len(rf.peers) == 0 {
		rf.mu.Unlock()
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == 0 {
			rf.mu.Unlock()
		}
		if rf.killed() {
			//rf.mu.Unlock()
			return
		}

		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			hbArgs := AppendEntriesArgs{
				Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: 0, PrevLogTerm: 0, LeaderCommit: rf.commitIndex,
			}

			if rf.nextIndex[server] >= 0 && len(rf.log) >= rf.nextIndex[server] {
				entries := make([]LogEntry, len(rf.log)-rf.nextIndex[server])
				copy(entries, rf.log[rf.nextIndex[server]:])
				hbArgs.Entries = entries
				DPrintf("[server %v] send entries %v to server %v matchind %v nextIndex %v", rf.me, entries, server, rf.matchIndex, rf.nextIndex)
				hbArgs.PrevLogIndex = rf.nextIndex[server] - 1
				if hbArgs.PrevLogIndex >= 0 {
					hbArgs.PrevLogTerm = rf.log[hbArgs.PrevLogIndex].Term
				}
			}

			if rf.state == "leader" && rf.currentTerm == hbArgs.Term {
				rf.mu.Unlock()
				hbReply := AppendEntriesReply{}
				DPrintf("server %v send appendEntries to peer %v", rf.me, server)
				ok := rf.peers[server].Call("Raft.AppendEntries", &hbArgs, &hbReply)
				rf.mu.Lock()

				//if rf.state != "leader" || rf.currentTerm != hbArgs.Term {
				//	DPrintf("Server %v is no longer leader", rf.me)
				//	rf.state = "follower"
				//	rf.votesNum = 0
				//	rf.mu.Unlock()
				//	return
				//}

				if ok && hbReply.Term > rf.currentTerm {
					DPrintf("Server %v incorrect term is no longer leader", rf.me)
					rf.currentTerm = hbReply.Term
					rf.votedFor = -1
					rf.persist()
					rf.state = "follower"
					rf.votesNum = 0
				}
				if !hbReply.Success && hbReply.Term <= rf.currentTerm {
					if rf.nextIndex[server] == 0 {
						DPrintf("Server %v next index for %v is 0", rf.me, server)
					} else {
						DPrintf("Server %v failed to append, decrement nextIndex for %v", rf.me, server)
						if hbReply.XIndex == -1 {
							DPrintf("server %v log is too short, decrement nextInd from %v to %v", server, rf.nextIndex[server], hbReply.XLen)
							rf.nextIndex[server] = hbReply.XLen
						} else {
							index := -1
							for i := len(rf.log) - 1; i >= 0; i-- {
								if rf.log[i].Term == hbReply.XTerm {
									index = i
									break
								}
							}
							if index == -1 {
								DPrintf("server %v is leader and doesn't have XTerm %v", rf.me, hbReply.XTerm)
								DPrintf("server %v decrement nextInd from %v to %v", server, rf.nextIndex[server], hbReply.XIndex)
								rf.nextIndex[server] = hbReply.XIndex
							} else {
								DPrintf("server %v is leader and has XTerm %v", rf.me, hbReply.XTerm)
								DPrintf("server %v decrement nextInd from %v to %v", server, rf.nextIndex[server], index)
								rf.nextIndex[server] = index
							}
						}
					}
					//if rf.nextIndex[server] > 0 {
					//	rf.nextIndex[server]--
					//}
					rf.mu.Unlock()
					return
				}
				if hbReply.Success && len(hbArgs.Entries) > 0 {
					DPrintf("Server %v success append entries for %v", rf.me, server)
					DPrintf("[server %v]  %v", rf.me, rf.log)
					rf.matchIndex[server] = hbArgs.PrevLogIndex + len(hbArgs.Entries)
					go rf.updateCommitIndex()
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					DPrintf("[server %v]  match %v", rf.me, rf.matchIndex)
					DPrintf("[server %v]  next %v", rf.me, rf.nextIndex)
					DPrintf("[server %v]  commitIndex %v", rf.me, rf.commitIndex)
				}
				rf.mu.Unlock()
			} else {
				DPrintf("Server %v is no longer leader", rf.me)
				rf.state = "follower"
				rf.votesNum = 0
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
			}
		}(i)

	}
}
func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	if rf.state == "leader" {
		rf.matchIndex[rf.me] = len(rf.log) - 1
		for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
			if rf.log[i].Term == rf.currentTerm {
				count := 0
				for j := 0; j < len(rf.peers); j++ {
					if rf.matchIndex[j] >= i {
						count++
					}
				}
				if count > len(rf.peers)/2 {
					DPrintf("Server %v updating commit index from %v to %v", rf.me, rf.commitIndex, i)
					rf.commitIndex = i
					break
				}
			}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) apply() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied && rf.commitIndex <= len(rf.log)-1 {
			DPrintf("server %v commitInd %v  appliedInd %v log %v", rf.me, rf.commitIndex, rf.lastApplied, rf.log)
			entry := make([]LogEntry, rf.commitIndex-rf.lastApplied)
			copy(entry, rf.log[rf.lastApplied+1:rf.commitIndex+1])
			appInd := rf.lastApplied
			oldCommitInd := rf.commitIndex
			rf.mu.Unlock()
			DPrintf("Server %v applying %v ", rf.me, entry)
			for key, val := range entry {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      val.Command,
					CommandIndex: key + appInd + 1,
				}
				DPrintf("Server %v applying %v message ", rf.me, msg)
				rf.applyCh <- msg
			}
			rf.mu.Lock()
			rf.lastApplied = oldCommitInd
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("server %v starting election", rf.me)
	rf.state = "candidate"
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesNum = 1
	rf.persist()
	rf.lastElectionTime = time.Now()
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	voteArgs := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log), LastLogTerm: lastLogTerm}
	if len(rf.log) == 0 {
		rf.mu.Unlock()
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == 0 {
			rf.mu.Unlock()
		}

		if i == rf.me {
			continue
		}

		if rf.killed() {
			//rf.mu.Unlock()
			return
		}

		DPrintf("server %v send request vote to %v", rf.me, i)
		go func(server int) {
			rf.mu.Lock()
			if rf.state == "candidate" && rf.currentTerm == voteArgs.Term {
				rf.mu.Unlock()
				voteReply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &voteArgs, &voteReply)
				if ok {
					rf.mu.Lock()
					if voteReply.VoteGranted {
						rf.votesNum++
						if rf.votesNum > len(rf.peers)/2 {
							DPrintf("server %v won the election for term %v with %v votes prematurely", rf.me, rf.currentTerm, rf.votesNum)
							rf.mu.Unlock()
							go rf.becomeLeader()
							return
						}
					} else if voteReply.Term > rf.currentTerm {
						rf.currentTerm = voteReply.Term
						rf.votedFor = -1
						rf.persist()
						rf.state = "follower"
						rf.votesNum = 0
					}
					rf.mu.Unlock()
				}
			} else {
				DPrintf("Server %v is no longer candidate", rf.me)
				rf.state = "follower"
				rf.votesNum = 0
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.state = "leader"
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) - 1
		rf.matchIndex[i] = 1
	}

	rf.matchIndex[rf.me] = len(rf.log) - 1
	DPrintf("[server %v] became leader nextInd %v matchInd %v", rf.me, rf.nextIndex, rf.matchIndex)
	rf.mu.Unlock()
	//rf.sendAppendEntries()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = "follower"
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastElectionTime = time.Now()
	rf.lastHeartBeatTime = time.Now()
	rf.votesNum = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartBeat()

	go rf.apply()

	return rf
}
