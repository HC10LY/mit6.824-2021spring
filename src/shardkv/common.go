package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoChange    = "ErrNoChange"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64 // 客户端唯一 ID（去重用）
	SeqId    int   // 客户端请求序号（去重用）
	// 下面两个在 ShardKV 中尤其重要
	Shard int // 这个 key 属于哪个 shard
	ConfigNum int // 当前请求是基于哪个配置发的
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId    int
	Shard    int
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}
