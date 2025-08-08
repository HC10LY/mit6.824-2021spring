package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
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
	Type  string // "Put", "Append", "Get"
	Key   string
	Value string

	ClientId  int64 // Clerk 唯一 ID
	RequestId int   // 请求序号
}

type CommandResult struct {
	Err       Err
	Value     string // 仅 Get 有返回值
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore     map[string]string          // key-value 存储
	lastApplied map[int64]int              // 记录每个 ClientId 最后执行的 RequestId，防止重复
	// notifyChans map[int]chan CommandResult // 用于同步等待 apply 完成，key 是日志索引
	notifyChans sync.Map // map[int]chan CommandResult，改为sync.Map

	snapshotThreshold int // 保存 maxraftstate，方便使用

	// 读取快照后恢复状态需要用的字段
	lastAppliedIndex int // 记录已应用到的最大日志索引
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan CommandResult, 1)
	kv.notifyChans.Store(index, ch) // 存储到 sync.Map

	select {
	case res := <-ch:
		if res.ClientId == args.ClientId && res.RequestId == args.RequestId {
			reply.Err = res.Err
			reply.Value = res.Value
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(150 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan CommandResult, 1)
	kv.notifyChans.Store(index, ch) // 存储到 sync.Map

	select {
	case res := <-ch:
		if res.ClientId == args.ClientId && res.RequestId == args.RequestId {
			reply.Err = res.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(150 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			cmd := msg.Command.(Op)

			kv.mu.Lock()

			// 幂等检查：是否已经执行过
			lastReqId, ok := kv.lastApplied[cmd.ClientId]
			if !ok || cmd.RequestId > lastReqId {
				// 应用操作
				switch cmd.Type {
				case "Put":
					kv.kvStore[cmd.Key] = cmd.Value
				case "Append":
					kv.kvStore[cmd.Key] += cmd.Value
				case "Get":
					// Get 不修改状态机
				}
				kv.lastApplied[cmd.ClientId] = cmd.RequestId
			}

			// 准备结果
			var res CommandResult
			if cmd.Type == "Get" {
				val, exists := kv.kvStore[cmd.Key]
				if !exists {
					val = ""
				}
				res = CommandResult{Err: OK, Value: val, ClientId: cmd.ClientId, RequestId: cmd.RequestId}
			} else {
				res = CommandResult{Err: OK, ClientId: cmd.ClientId, RequestId: cmd.RequestId}
			}

			kv.lastAppliedIndex = msg.CommandIndex

			kv.mu.Unlock()

			if chVal, ok := kv.notifyChans.Load(msg.CommandIndex); ok {
				ch := chVal.(chan CommandResult)
				select {
				case ch <- res:
				default:
				}
				close(ch)
				kv.notifyChans.Delete(msg.CommandIndex)
			}

			// 判断是否需要 snapshot
            if kv.maxraftstate != -1 && kv.rf.RaftStateSize() > kv.maxraftstate {
                kv.doSnapshot(kv.lastAppliedIndex)
            }

			/*
			// 唤醒等待这个日志条目的 RPC handler
			if ch, ok := kv.notifyChans[msg.CommandIndex]; ok {
				select {
				case ch <- res:
				default:
				}
				close(ch)
				delete(kv.notifyChans, msg.CommandIndex)
			}
			kv.mu.Unlock()
			*/
		} else if msg.SnapshotValid {
            kv.mu.Lock()
            kv.readSnapshot(msg.Snapshot)
            kv.lastAppliedIndex = msg.SnapshotIndex
            kv.mu.Unlock()
        }
	}
}

func (kv *KVServer) doSnapshot(lastIncludedIndex int) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)

    e.Encode(kv.kvStore)
    e.Encode(kv.lastApplied)
	e.Encode(kv.lastAppliedIndex)
    data := w.Bytes()

    kv.rf.Snapshot(lastIncludedIndex, data)
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
    if snapshot == nil || len(snapshot) < 1 {
        return
    }

    r := bytes.NewBuffer(snapshot)
    d := labgob.NewDecoder(r)

    var kvStore map[string]string
	var lastApplied map[int64]int
	var lastAppliedIndex int

	if d.Decode(&kvStore) != nil ||
	   d.Decode(&lastApplied) != nil ||
	   d.Decode(&lastAppliedIndex) != nil {
		return
	}

    kv.kvStore = kvStore
    kv.lastApplied = lastApplied
    kv.lastAppliedIndex = lastAppliedIndex
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.lastApplied = make(map[int64]int)
	// kv.notifyChans = make(map[int]chan CommandResult)

	snapshot := persister.ReadSnapshot()
    if len(snapshot) > 0 {
        kv.readSnapshot(snapshot)
    }

	// 启动 goroutine 监听 Raft applyCh
	go kv.applier()

	return kv
}
