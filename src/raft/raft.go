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
	"bytes"
	// "fmt"
	"sync"
	"sync/atomic"

	"math/rand"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
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

const (
	HeartBeatTimeOut = 101
	ElectTimeOutBase = 450
)

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

	heartTimer *time.Timer // 心跳定时器

	log         []LogEntry  // 日志数组，从下标0开始，每个包含command和term
	commitIndex int         // 当前已知被提交的最大日志条目索引（初始为0，单调递增）
	lastApplied int         // 最后被应用到状态机的日志条目的索引（初始为0，单调递增）

	nextIndex   []int       // 对于每个服务器，要发送的下一条日志索引（初始化为 leader 的 last log index + 1）
	matchIndex  []int       // 对于每个服务器，已知的最大匹配日志索引（即该 follower 上已复制的日志的最高索引）

	lastIncludedIndex int // 快照中包含的最后一条日志的 index
	lastIncludedTerm  int // 快照中包含的最后一条日志的 term

	applyCh chan ApplyMsg
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

func (rf *Raft) ResetHeartTimer(timeStamp int) {
	rf.heartTimer.Reset(time.Duration(timeStamp) * time.Millisecond)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)

    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)

	e.Encode(rf.lastIncludedIndex)
    e.Encode(rf.lastIncludedTerm)

    data := w.Bytes()
    rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		// 持久化数据为空，说明是第一次启动，初始化哨兵日志
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = []LogEntry{{Term: 0}} // 加哨兵日志
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = 0
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		// 解码失败，防御性处理，初始化哨兵日志
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = []LogEntry{{Term: 0}} // 加哨兵日志
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = 0
		return
	}

	// 解码成功，恢复持久化的状态和日志
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = logs
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	// 防御性检查：如果持久化日志为空，补充哨兵日志
	// if len(rf.log) == 0 {
	// 	rf.log = []LogEntry{{Term: 0}}
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	if lastIncludedIndex < rf.lastIncludedIndex {
		// fmt.Printf("[Node %d] CondInstallSnapshot: lastIncludedIndex %d < lastIncludedIndex %d, ignoring snapshot\n", rf.me, lastIncludedIndex, rf.lastIncludedIndex)
		return false
	}

	// 先保存旧的 lastIncludedIndex（修剪日志时要用）
	oldLastIncludedIndex := rf.lastIncludedIndex

	// 更新 snapshot 元数据
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex

	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	// 修剪日志：跳过 snapshot 覆盖的部分（包括 lastIncludedIndex 这一项）
	if len(rf.log) > 0 {
		var offset int
		if oldLastIncludedIndex == 0 {
			offset = lastIncludedIndex - oldLastIncludedIndex + 1
		} else {
			offset = lastIncludedIndex - oldLastIncludedIndex
		}
		if offset < len(rf.log) {
			rf.log = rf.log[offset:]
		} else {
			rf.log = []LogEntry{}
		}
	}

	rf.persistWithSnapshot(snapshot)

	// fmt.Printf("CondInstallSnapshot: true\n")

	return true
}

func (rf *Raft) getLastLogIndex() int {
	var lastLogIndex int
	if rf.lastIncludedIndex == 0 {
		lastLogIndex = len(rf.log) - 1
	} else {
		lastLogIndex = rf.lastIncludedIndex + len(rf.log)
	}
    return lastLogIndex
}

func (rf *Raft) indexToOffset(index int) int {
    return index - rf.lastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.commitIndex < index || index <= rf.lastIncludedIndex {
        return
    }

    oldLastIncludedIndex := rf.lastIncludedIndex

    // 计算新 snapshot 的 lastIncludedTerm
    term := rf.getTermAtIndex(index)

    // 先裁剪日志
	var offset int
	if oldLastIncludedIndex == 0 {
		offset = index + 1
	} else {
		offset = index - oldLastIncludedIndex
	}
    if offset < len(rf.log) {
        rf.log = rf.log[offset:]
    } else {
        rf.log = []LogEntry{}
    }

    // 再更新快照索引和任期
    rf.lastIncludedIndex = index
    rf.lastIncludedTerm = term

	if rf.lastApplied < index {
		rf.lastApplied = index
	}

    // 持久化快照和状态
    rf.persistWithSnapshot(snapshot)

}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    e.Encode(rf.lastIncludedIndex)
    e.Encode(rf.lastIncludedTerm)
    data := w.Bytes()
    rf.persister.SaveStateAndSnapshot(data, snapshot)
}

func (rf *Raft) RaftStateSize() int {
    return rf.persister.RaftStateSize()
}

func (rf *Raft) getTermAtIndex(index int) int {
    if index == rf.lastIncludedIndex {
        return rf.lastIncludedTerm
    }
	if rf.lastIncludedIndex == 0 {
		return rf.log[index].Term
	}
    return rf.log[index - rf.lastIncludedIndex - 1].Term
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type LogEntry struct {
    Command interface{}
    Term    int
}

type AppendEntriesArgs struct {
	Term     int // 领导人的任期
	LeaderId int // 领导人的 ID，以便于跟随者重定向 client
	// 后面的日志复制字段（2B再用）
	PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry  // 本次要追加的日志项，可以为空（即心跳）
    LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  // 当前节点的 term，用于 leader 发现自己落后
	Success bool // 总是 true（在 2A 里）
	ConflictIndex int // 新增为了快速回退
	ConflictTerm  int // 同上
	Len           int // 新增为了快速回退
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm
    reply.Success = false

	reply.ConflictTerm = -1
    reply.ConflictIndex = -1

	// fmt.Printf("[Follower %d] AppendEntries: ConflictTerm %d, ConflictIndex %d, currentTerm %d, log=%+v\n", rf.me, reply.ConflictTerm, reply.ConflictIndex, rf.currentTerm, rf.log)

	if rf.lastIncludedIndex == 0 {
		reply.Len = len(rf.log)
	} else {
		reply.Len = len(rf.log) + rf.lastIncludedIndex + 1

	}
	
    // 1. 任期检查：如果leader任期 < 当前任期，拒绝
    if args.Term < rf.currentTerm {
        return
    }

    // 2. 如果收到比当前任期大的任期，更新自己状态为Follower
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.state = Follower
        rf.votedFor = -1
		// 持久化
		rf.persist()
    }

    // 3. 重置心跳时间戳
    rf.lastHeartbeat = time.Now()

	// 处理快照边界的 PrevLogIndex
	if args.PrevLogIndex == rf.lastIncludedIndex {
		// 比较快照最后一条日志的任期
		if args.PrevLogTerm != rf.lastIncludedTerm {
			reply.ConflictTerm = rf.lastIncludedTerm
			reply.ConflictIndex = rf.lastIncludedIndex + 1
			return
		}
		// 匹配成功，继续下面操作
	} else if args.PrevLogIndex < rf.lastIncludedIndex {
		// PrevLogIndex 小于快照索引，说明Leader日志太老或者不匹配，拒绝
		// reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	} else {
		// PrevLogIndex > lastIncludedIndex，转换为本地日志索引
		var localPrevIndex int
		if rf.lastIncludedIndex == 0 {
			localPrevIndex = args.PrevLogIndex
		} else {
			localPrevIndex = args.PrevLogIndex - rf.lastIncludedIndex - 1
		}
		
		if localPrevIndex < 0 || localPrevIndex >= len(rf.log) {
			// 日志不匹配或不存在，告诉Leader本地日志长度
			// reply.Len = len(rf.log) + rf.lastIncludedIndex + 1
			return
		}
		// fmt.Printf("[Follower %d] AppendEntries: PrevLogIndex %d, PrevLogTerm %d, localPrevIndex %d, localPrevTerm %d\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, localPrevIndex, rf.log[localPrevIndex].Term)
		if rf.log[localPrevIndex].Term != args.PrevLogTerm {
			// 任期冲突，寻找冲突任期第一次出现的位置
			conflictTerm := rf.log[localPrevIndex].Term
			reply.ConflictTerm = conflictTerm
			i := localPrevIndex
			for i >= 0 && rf.log[i].Term == conflictTerm {
				i--
			}
			if i == -1 {
				// 如果冲突任期在日志开头，说明没有匹配的日志
				reply.ConflictIndex = rf.lastIncludedIndex + 1 // 这里返回快照索引+1
				return
			}
			if rf.lastIncludedIndex == 0 {
				reply.ConflictIndex = i + 1
			} else {
				reply.ConflictIndex = i + 1 + rf.lastIncludedIndex + 1 // 转换为本地日志索引
			}
			return
		}
	}

	// 到这里说明 PrevLogIndex 匹配成功，开始处理冲突日志和追加新日志

	// 计算日志开始追加的位置（相对于rf.log的本地索引）
	var startIndex int
	if rf.lastIncludedIndex == 0 {
		startIndex = args.PrevLogIndex + 1
	} else {
		startIndex = args.PrevLogIndex - rf.lastIncludedIndex
	}

    // 删除冲突日志（如果有）
	if len(args.Entries) > 0 {
		if startIndex < len(rf.log) {
			rf.log = rf.log[:startIndex]
		}
		// 追加新日志
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
	}

    // 6. 更新 commitIndex，取 leader 传来的 min(leaderCommit, 最后一条日志索引)
    if args.LeaderCommit > rf.commitIndex {
		var lastIndex int
		if rf.lastIncludedIndex == 0 {
			lastIndex = len(rf.log) - 1
		} else {
			// 计算本地日志最后索引
			lastIndex = rf.lastIncludedIndex + len(rf.log)
		}
        if args.LeaderCommit < lastIndex {
            rf.commitIndex = args.LeaderCommit
        } else {
            rf.commitIndex = lastIndex
        }
    } else {
		// 如果 LeaderCommit 小于当前 commitIndex，说明已经过期
		rf.commitIndex = args.LeaderCommit
	}
	// fmt.Printf("[Follower %d] AppendEntries: rf.commitIndex=%d, rf.lastIncludedIndex=%d, rf.log.len=%d, [leader %d].LeaderCommit=%d\n", rf.me, rf.commitIndex, rf.lastIncludedIndex, len(rf.log), args.LeaderId, args.LeaderCommit)
	// fmt.Printf("[Follower %d] AppendEntries success=%v, commitIndex=%v\n", rf.me, reply.Success, rf.commitIndex)

    reply.Success = true

	// fmt.Printf("[Follower %d] AppendEntries success=%v, log=%v\n", rf.me, reply.Success, rf.log)
}


type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // 候选人的任期号
    CandidateId int // 请求投票的候选人ID
    // 2B以后可加日志信息字段
	LastLogIndex int
	LastLogTerm  int
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
		return
	}

	if args.Term > rf.currentTerm {
		// fmt.Printf("[Node %d] RequestVote: CandidateId %d term %d > current term %d, switching to Follower\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1

		rf.persist() // 持久化状态
	}

	// 判断 candidate 日志是否至少和自己一样新
	// 计算本地日志最后索引和对应任期
	var lastLogIndex int
	if rf.lastIncludedIndex == 0 {
		lastLogIndex = len(rf.log) - 1
	} else {
		lastLogIndex = rf.lastIncludedIndex + len(rf.log)
	}
    lastLogTerm := rf.getTermAtIndex(lastLogIndex)

	upToDate := false
	if args.LastLogTerm > lastLogTerm {
		upToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		upToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now() // 重置心跳计时，防止过早发起选举
		rf.state = Follower
		rf.persist() // 持久化投票状态
	}
	reply.Term = rf.currentTerm
}

type InstallSnapshotArgs struct {
    Term              int
    LeaderId          int
    LastIncludedIndex int
    LastIncludedTerm  int
    Data              []byte
}

type InstallSnapshotReply struct {
    Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

	//fmt.Printf("[Follower %d] receive InstallSnapshot from %d, args=%+v  rf.lastIncludedIndex=%d currentTerm=%d\n", rf.me, args.LeaderId, args, rf.lastIncludedIndex, rf.currentTerm)

    reply.Term = rf.currentTerm

    if args.Term < rf.currentTerm {
        return
    }

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1
        rf.state = Follower
        rf.persist()
    }

    rf.lastHeartbeat = time.Now()

    if args.LastIncludedIndex <= rf.lastIncludedIndex {
        return
    }

    // 更新状态（内部会调用 persistWithSnapshot）
	// rf.mu.Unlock()
	success := rf.CondInstallSnapshot(args.LastIncludedTerm, args.LastIncludedIndex, args.Data)
    // fmt.Printf("[Node %d] CondInstallSnapshot returned %v\n", rf.me, success)
	if !success {
		return
	}
	// fmt.Printf(">>>>>> REACHED InstallSnapshot log print: index=%d term=%d\n", args.LastIncludedIndex, args.LastIncludedTerm)

    rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

// PreVoteArgs and PreVoteReply are used for the pre-vote phase in Raft,
// which is an optimization to reduce unnecessary elections.
// They are not part of the original Raft paper but can be used to improve election safety
// by allowing nodes to pre-vote before starting a full election
// This is useful in scenarios where a node might be unsure about its state or the state of the cluster,
// and it can help prevent split votes and unnecessary elections.
type PreVoteArgs struct {
    Term        int
    CandidateId int
    LastLogIndex int
    LastLogTerm  int
}

type PreVoteReply struct {
    Term        int
    VoteGranted bool
}

func (rf *Raft) PreVote(args *PreVoteArgs, reply *PreVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 拒绝过期的 term
	if args.Term < rf.currentTerm {
		return
	}

	// 判断 candidate 的日志是否至少和自己一样新
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	upToDate := false
	if args.LastLogTerm > lastLogTerm {
		upToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		upToDate = true
	}

	if upToDate {
		reply.VoteGranted = true
	}
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

func (rf *Raft) sendPreVote(server int, args *PreVoteArgs, reply *PreVoteReply) bool {
    ok := rf.peers[server].Call("Raft.PreVote", args, reply)
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
	rf.mu.Lock()
	// defer rf.mu.Unlock()

	defer func() {
		// DPrintf("server %v Start 释放锁mu", rf.me)
		rf.ResetHeartTimer(10)
		rf.mu.Unlock()
	}()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// 追加新日志条目
	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})

	rf.persist()

	var index int
	var term int
	if rf.lastIncludedIndex == 0 {
		index = len(rf.log) - 1 // 如果没有快照，直接使用日志长度作为索引
	} else {
		// 如果有快照，计算本地日志索引
		index = rf.lastIncludedIndex + len(rf.log)
	}
	term = rf.currentTerm

	// fmt.Printf("[Leader %d] Start() appended command %+v at index %d\n", rf.me, command, index)

	// 追加日志后，立刻异步向所有Follower复制日志（触发一次发送）
	// go rf.broadcastAppendEntries()

	return index, term, true
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go rf.sendAppendEntriesTo(peer, term)
	}
}

func (rf *Raft) sendAppendEntriesTo(peer int, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	nextIndex := rf.nextIndex[peer]

	// fmt.Printf("[Leader %d] sendAppendEntriesTo peer %d, nextIndex=%d, currentTerm=%d lastIncludedIndex=%d\n", rf.me, peer, nextIndex, term, rf.lastIncludedIndex)

	// 如果 follower 的 nextIndex <= lastIncludedIndex，说明 follower 需要安装快照
	if nextIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		// fmt.Printf("[Leader %d] sendInstallSnapshotTo peer %d, nextIndex=%d, lastIncludedIndex=%d\n", rf.me, peer, nextIndex, rf.lastIncludedIndex)
		rf.sendInstallSnapshotTo(peer)
		return
	}

	// 计算 prevLogIndex 和 prevLogTerm，注意索引转换
	prevLogIndex := nextIndex - 1
	var prevLogTerm int
	if prevLogIndex == rf.lastIncludedIndex {
		prevLogTerm = rf.lastIncludedTerm
	} else {
		// 本地日志索引
		var localPrevLogIndex int
		if rf.lastIncludedIndex == 0 {
			localPrevLogIndex = prevLogIndex
		} else {
			localPrevLogIndex = prevLogIndex - rf.lastIncludedIndex - 1
		}
		if localPrevLogIndex >= 0 && localPrevLogIndex < len(rf.log) {
			prevLogTerm = rf.log[localPrevLogIndex].Term
		} else {
			// 这种情况理论上不应该发生，日志索引越界，直接返回
			rf.mu.Unlock()
			return
		}
	}

	// 取出要发送的日志条目切片，索引转换
	var localNextIndex int
	var entries []LogEntry
	if rf.lastIncludedIndex == 0 {
		localNextIndex = nextIndex
	} else {
		localNextIndex = nextIndex - rf.lastIncludedIndex - 1
	}
	if localNextIndex < len(rf.log) {
		temp := rf.log[localNextIndex:]
		entries = make([]LogEntry, len(temp))
		copy(entries, temp)
	} else {
		entries = []LogEntry{}
	}

	leaderCommit := rf.commitIndex

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
	// fmt.Printf("[Leader %d] sendAppendEntriesTo peer %d, ok=%v, args=%+v, reply=%+v\n", rf.me, peer, ok, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		// fmt.Printf("[leader %d] AppendEntries: peer %d term %d > current term %d, switching to Follower\n", rf.me, peer, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()  // 持久化 currentTerm 和 votedFor
		return
	}

	if rf.state != Leader || term != rf.currentTerm {
		return
	}

	if reply.Success {
		// 更新 matchIndex 和 nextIndex
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		// fmt.Printf("last success to peer %d next [Leader %d] sendAppendEntriesTo peer %d, matchIndex=%v\n", peer, rf.me, peer, rf.matchIndex[peer])

		// 计算 commitIndex，只提交被大多数复制的日志
		// fmt.Printf("[Leader %d] commitIndex %d\n", rf.me, rf.commitIndex)
		// fmt.Printf("len(rf.log) = %d\n", len(rf.log))
		var N int
		if rf.lastIncludedIndex == 0 {
			N = len(rf.log) - 1
		} else{
			N = rf.lastIncludedIndex + len(rf.log)
		}
		for N > rf.commitIndex {
			count := 1 // Leader自己算
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			// fmt.Printf("rf.log=%+v, N=%d\n", rf.log, N)
			// fmt.Printf("N=%d, rf.currentTerm=%d, rf.getLog)
			// fmt.Printf("rf.term=%d, N.term=%d, rf.commitIndex=%d, count=%d\n", rf.currentTerm, rf.getLogTerm(N), rf.commitIndex, count)
			if count >= len(rf.peers)/2 + 1 && rf.getLogTerm(N) == rf.currentTerm {
				rf.commitIndex = N
				break
			}
			N--
		}
		// fmt.Printf("[Leader %d] Updated commitIndex to %d after sending to peer %d\n", rf.me, rf.commitIndex, peer)
	} else {
		// 复制失败，说明日志不匹配，使用 conflict 回退策略
		if reply.ConflictTerm != -1 {
			i := rf.nextIndex[peer] - 1
			for i > rf.lastIncludedIndex && rf.getLogTerm(i) > reply.ConflictTerm {
				i--
			}
			if rf.getLogTerm(i) == reply.ConflictTerm {
				// 之前PrevLogIndex发生冲突位置时, Follower的Term自己也有
				rf.nextIndex[peer] = i + 1
			} else {
				// 之前PrevLogIndex发生冲突位置时, Follower的Term自己没有
				rf.nextIndex[peer] = reply.ConflictIndex
			}
		} else {
			rf.nextIndex[peer] = reply.Len
			if rf.nextIndex[peer] > rf.lastIncludedIndex + len(rf.log) {
				rf.nextIndex[peer] = rf.lastIncludedIndex + len(rf.log)
			}
		}
		// fmt.Printf("last fail to peer %d next [Leader %d] sendAppendEntriesTo peer %d, nextIndex=%v\n", peer, rf.me, peer, rf.nextIndex[peer])
	}
}

func (rf *Raft) getLogTerm(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	var localIndex int
	if rf.lastIncludedIndex == 0 {
		localIndex = index // 因为哨兵日志的存在，
	} else {
		localIndex = index - rf.lastIncludedIndex - 1
	}
	if localIndex < 0 || localIndex >= len(rf.log) {
		return -1
	}
	return rf.log[localIndex].Term
}

func (rf *Raft) sendInstallSnapshotTo(peer int) {
	rf.mu.Lock()

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex:  rf.lastIncludedIndex,
		LastIncludedTerm:   rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}

	rf.mu.Unlock()

	var reply InstallSnapshotReply
	// fmt.Printf("Raft.InstallSnapshot start\n")
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)
	// fmt.Printf("[Leader %d] sendInstallSnapshotTo peer %d, ok=%v, args=%+v, reply=%+v\n", rf.me, peer, ok, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
    defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.lastHeartbeat = time.Now() // 重置心跳时间
		rf.persist()
	}

	// 快照成功安装，更新 follower 的索引
	rf.matchIndex[peer] = args.LastIncludedIndex
	rf.nextIndex[peer] = args.LastIncludedIndex + 1
}

func (rf *Raft) applyLogs(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++

			if rf.lastApplied <= rf.lastIncludedIndex {
				continue // 被快照覆盖的，不再 apply
			}
			var entry LogEntry
			// fmt.Printf("[Node %d] Applying log at index %d commitindex %d log=%+v\n", rf.me, rf.lastApplied, rf.commitIndex, rf.log)
			if rf.lastIncludedIndex == 0 {
				entry = rf.log[rf.lastApplied]
			} else {
				entry = rf.log[rf.lastApplied - rf.lastIncludedIndex - 1]
			}

			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}

			rf.mu.Unlock()
			applyCh <- msg
			// fmt.Printf("[Node %d] Applied log at index %d: %+v\n", rf.me, rf.lastApplied, entry)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
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
            // 进行预投票，成功才真正发起选举
			// fmt.Printf("follower %d start election\n", rf.me)
            rf.startElection()
        }
        rf.mu.Unlock()

        // 休眠一个较小时间片，避免长时间阻塞
        time.Sleep(20 * time.Millisecond)
    }
}

func (rf *Raft) startPreVote() bool {
    term := rf.currentTerm
    lastLogIndex := len(rf.log) - 1
    lastLogTerm := 0
    if lastLogIndex >= 0 {
        lastLogTerm = rf.log[lastLogIndex].Term
    }

    votes := 1
    majority := len(rf.peers)/2 + 1
    var mu sync.Mutex
    var wg sync.WaitGroup

    for i := range rf.peers {
        if i == rf.me {
            continue
        }
        wg.Add(1)
        go func(peer int) {
            defer wg.Done()
            args := &PreVoteArgs{
                Term:         term,  // 预投票用当前term，不自增
                CandidateId:  rf.me,
                LastLogIndex: lastLogIndex,
                LastLogTerm:  lastLogTerm,
            }
            reply := &PreVoteReply{}
            if rf.sendPreVote(peer, args, reply) {
                mu.Lock()
                if reply.VoteGranted {
                    votes++
                }
                mu.Unlock()
            }
        }(i)
    }

    wg.Wait()

    return votes >= majority
}

func (rf *Raft) startElection() {
    rf.state = Candidate
    rf.currentTerm++
    rf.votedFor = rf.me
	votesGranted := 1
	rf.lastHeartbeat = time.Now() // 重置心跳时间
	rf.persist()

	var lastLogIndex int
    if len(rf.log) == 0 {
		if rf.lastIncludedIndex == 0 {
			lastLogIndex = len(rf.log) - 1
		} else {
			lastLogIndex = rf.lastIncludedIndex
		}
    } else {
		if rf.lastIncludedIndex == 0 {
			lastLogIndex = len(rf.log) - 1
		} else {
			lastLogIndex = rf.lastIncludedIndex + len(rf.log)
		}
    }
    lastLogTerm := rf.getTermAtIndex(lastLogIndex)

	// lastLogIndex := len(rf.log) - 1
	// lastLogTerm := rf.log[lastLogIndex].Term
    // votesGranted := 1  // 先给自己投票
	// rf.lastHeartbeat = time.Now() // 重置心跳时间
	// rf.persist()

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
				LastLogIndex: lastLogIndex,
    			LastLogTerm:  lastLogTerm,
            }
			rf.mu.Unlock()
            var reply RequestVoteReply
            if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()

				if args.Term != rf.currentTerm {
					// 易错点: 函数调用的间隙被修改了
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()  // 持久化 currentTerm 和 votedFor
					rf.lastHeartbeat = time.Now()
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
							for i := range rf.peers {
								// rf.nextIndex[i] = len(rf.log)
								if rf.lastIncludedIndex == 0 {
									rf.nextIndex[i] = len(rf.log) // 设置为日志末尾
								} else {
									rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.log) + 1 // 设置为日志末尾的下一个索引
								}
								// rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.log)
								rf.matchIndex[i] = rf.lastIncludedIndex
							}
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
	for {
		<-rf.heartTimer.C
		rf.mu.Lock()
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		rf.mu.Unlock()

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			// 统一走 sendAppendEntriesTo，处理日志或空心跳
			go rf.sendAppendEntriesTo(peer, term)
		}

		// time.Sleep(100 * time.Millisecond)
		rf.ResetHeartTimer(HeartBeatTimeOut)
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

	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	// 初始化状态
	rf.votedFor = -1
	rf.state = Follower
	rf.lastHeartbeat = time.Now()

	rf.heartTimer = time.NewTimer(0)

	// rf.log = append(rf.log, LogEntry{Term: 0}) // 初始有一个哨兵日志
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// **这里加上初始化nextIndex和matchIndex**
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		if rf.lastIncludedIndex == 0 {
			rf.nextIndex[i] = len(rf.log) // 设置为日志末尾的下一个索引
		} else {
			rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.log) + 1 // 设置为日志末尾的下一个索引
		}
		rf.matchIndex[i] = 0
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	// **新建应用日志的协程，负责提交日志给状态机**
	go rf.applyLogs(rf.applyCh)

	return rf
}


// 在 Raft 里加
func (rf *Raft) GetLastLogTerm() int {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.getLastLogTermUnlocked()
}

func (rf *Raft) getLastLogTermUnlocked() int {
    if len(rf.log) > 0 {
        return rf.log[len(rf.log)-1].Term
    }
    return rf.lastIncludedTerm
}