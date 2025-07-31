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
	"sync"
	"sync/atomic"
	// "fmt"

//	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
	"time"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
const (
    Follower = iota
    Candidate
    Leader
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
    votedFor    int
    state       int
	lastHeartbeat time.Time  // 最近一次收到有效Leader心跳的时间
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term     int // 领导人的任期
	LeaderId int // 领导人的 ID，以便于跟随者重定向 client
	// 后面的日志复制字段（2B再用）
	// PrevLogIndex int
	// PrevLogTerm  int
	// Entries      []LogEntry
	// LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  // 当前节点的 term，用于 leader 发现自己落后
	Success bool // 总是 true（在 2A 里）
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// 收到心跳，说明对方是有效 leader
	rf.currentTerm = args.Term
	rf.state = Follower
	// 这里可以重置选举计时器（后面我们加）
	rf.lastHeartbeat = time.Now()

	reply.Success = true
}


type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // 候选人的任期号
    CandidateId int // 请求投票的候选人ID
    // 2B以后可加日志信息字段
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前节点的任期号，用于候选人更新自己的任期
    VoteGranted bool // 是否同意投票
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm
    reply.VoteGranted = false

    if args.Term < rf.currentTerm {
        // 请求任期落后，拒绝投票
		// fmt.Printf("[Node %d] Reject vote for %d: term too old (current %d)\n", rf.me, args.CandidateId, rf.currentTerm)
        return
    }

    // 如果请求任期大于当前任期，更新任期并转换为跟随者
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.state = Follower
        rf.votedFor = -1
    }

    // 2A阶段忽略日志匹配判断，简化处理
    if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
        rf.votedFor = args.CandidateId
        reply.VoteGranted = true
		// fmt.Printf("[Node %d] Grant vote to %d for term %d\n", rf.me, args.CandidateId, args.Term)
        // 这里可以重置选举超时计时器，防止发起新选举（后面加）
    }else {
		// fmt.Printf("[Node %d] Reject vote for %d: already voted for %d\n", rf.me, args.CandidateId, rf.votedFor)
	}

    reply.Term = rf.currentTerm
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		 rf.mu.Lock()
        // 计算从最后收到心跳经过的时间
        timeSinceLastHeartbeat := time.Since(rf.lastHeartbeat)

        // 选举超时时间固定，或者随机但这里用变量保持一致
        electionTimeout := 300*time.Millisecond + time.Duration(rand.Intn(200))*time.Millisecond

        if rf.state != Leader && timeSinceLastHeartbeat >= electionTimeout {
            rf.startElection()
        }
        rf.mu.Unlock()

        // 休眠一个较小时间片，避免长时间阻塞
        time.Sleep(20 * time.Millisecond)
    }
}

func (rf *Raft) startElection() {
    rf.state = Candidate
    rf.currentTerm++
    rf.votedFor = rf.me
    votesGranted := 1  // 先给自己投票

    var mu sync.Mutex  // 保护 votesGranted 变量
    majority := len(rf.peers)/2 + 1

	// 打印启动选举
    // fmt.Printf("[Node %d] start election for term %d\n", rf.me, rf.currentTerm)

    for i := range rf.peers {
        if i == rf.me {
            continue
        }
        go func(server int) {
			rf.mu.Lock()
            args := RequestVoteArgs{
                Term:        rf.currentTerm,
                CandidateId: rf.me,
            }
			rf.mu.Unlock()
            var reply RequestVoteReply
            if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				if reply.VoteGranted {
					mu.Lock()
					votesGranted++
					if votesGranted >= majority {
						rf.mu.Lock()
						if rf.state == Candidate {
							rf.state = Leader
							// fmt.Printf("[Node %d] Become Leader for term %d with votes %d\n", rf.me, rf.currentTerm, votesGranted)
							go rf.leaderSendHeartbeats()
						}
						rf.mu.Unlock()
					}
					mu.Unlock()
				}
			}
        }(i)
    }
}

func (rf *Raft) leaderSendHeartbeats() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     term,
				LeaderId: rf.me,
				// 心跳不带日志（2A阶段）
			}
			rf.mu.Unlock()

			var reply AppendEntriesReply
			ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
				}
			}
		}(peer)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 初始化状态
	rf.votedFor = -1
	rf.state = Follower
	rf.lastHeartbeat = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
