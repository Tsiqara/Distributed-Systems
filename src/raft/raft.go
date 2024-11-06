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
}

const minElectionTimeout int64 = 400
const minHeartBeatTimeout int64 = 400
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("server %v refuse appendEntries from %v", rf.me, args.LeaderId)
		rf.mu.Unlock()
		return
	}

	if args.Term >= rf.currentTerm {
		DPrintf("server %v accept appendEntries from %v", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.state = "follower"
		rf.votesNum = 0
		rf.currentTerm = args.Term
		rf.lastHeartBeatTime = time.Now()
		rf.mu.Unlock()
	}
	//} else if args.Term == rf.currentTerm {
	//	DPrintf("server %v accept appendEntries from %v", rf.me, args.LeaderId)
	//	reply.Term = rf.currentTerm
	//	reply.Success = true
	//	rf.lastHeartBeatTime = time.Now()
	//	rf.mu.Unlock()
	//}

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
	DPrintf("server %v is being killed", rf.me)
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
			//go rf.sendHeartBeats()
		} else if rf.state == "follower" {
			rf.votesNum = 0
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
		DPrintf("avige lock tick heart")
		if rf.state == "leader" {
			rf.mu.Unlock()
			DPrintf("davde lock tick hb l")
			for {
				rf.mu.Lock()
				DPrintf("avige lock tick heart for")
				if rf.state != "leader" {
					rf.mu.Unlock()
					DPrintf("davde lock tick hb not l")
					break
				}
				rf.mu.Unlock()
				DPrintf("davde lock tick hb before go")
				go rf.sendHeartBeats()
				time.Sleep(time.Duration(100) * time.Millisecond)
			}
		} else {
			rf.mu.Unlock()
			DPrintf("davde lock tick hb lelse")
			ms := 10
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

func (rf *Raft) sendHeartBeats() {
	rf.mu.Lock()
	hbArgs := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
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

		// todo yvelas tavisi args

		//rf.mu.Unlock()
		go func(server int) {
			rf.mu.Lock()
			if rf.state == "leader" && rf.currentTerm == hbArgs.Term {
				rf.mu.Unlock()
				hbReply := AppendEntriesReply{}
				DPrintf("server %v send appendEntries to peer %v", rf.me, server)
				ok := rf.peers[server].Call("Raft.AppendEntries", &hbArgs, &hbReply)
				rf.mu.Lock()
				if ok && hbReply.Term > rf.currentTerm {
					rf.currentTerm = hbReply.Term
					rf.state = "follower"
					rf.votesNum = 0
				}
				rf.mu.Unlock()
			} else {
				DPrintf("Server %v is no longer leader", rf.me)
				rf.state = "follower"
				rf.votesNum = 0
				rf.mu.Unlock()
			}
		}(i)

	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("server %v starting election", rf.me)
	rf.state = "candidate"
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesNum = 1
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
						rf.state = "follower"
						rf.votesNum = 0
					}
					rf.mu.Unlock()
				}
			} else {
				DPrintf("Server %v is no longer candidate", rf.me)
				rf.state = "follower"
				rf.votesNum = 0
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.state = "leader"
	rf.mu.Unlock()
	//rf.sendHeartBeats()
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartBeat()

	return rf
}
