package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	// "fmt"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	clientId int64 // 唯一客户端 ID
    seqId    int   // 每个请求递增
	leaderIds map[int]int   // gid -> 该组上次成功的leader服务器索引
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seqId = 0
	ck.leaderIds = make(map[int]int)
	ck.config = ck.sm.Query(-1) // 拿最新配置
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.seqId++
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	ck.mu.Unlock()

	for {
		// fmt.Printf("client Get called key: %+v\n", args.Key)
		ck.mu.Lock()
		config := deepCopyConfig(ck.config)
		ck.mu.Unlock()
		shard := key2shard(key)
		gid := config.Shards[shard]
		if servers, ok := config.Groups[gid]; ok {
			ck.mu.Lock()
			leader := ck.leaderIds[gid]
			ck.mu.Unlock()
			if leader >= len(servers) {
				leader = 0
			}

			for tried := 0; tried < len(servers); tried++ {
				args.Shard = shard
				args.ConfigNum = config.Num
				// fmt.Printf("client Get called with args: %+v\n", args)
				srv := ck.make_end(servers[leader])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)

				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					// 成功，更新leaderId
					ck.mu.Lock()
					ck.leaderIds[gid] = leader
					ck.mu.Unlock()
					return reply.Value
				}

				if ok && reply.Err == ErrWrongGroup {
					// 当前组不负责这个shard，退出尝试刷新配置
					// fmt.Printf("client Get: wrong group for key %s, gid %d\n", key, gid)
					break
				}

				// fmt.Printf("client Get: failed to call server %s, reply: %+v\n", servers[leader], reply)

				// 失败或者非leader，试下一个服务器
				leader = (leader + 1) % len(servers)
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		newCfg := ck.sm.Query(-1)
		ck.mu.Lock()
		ck.config = deepCopyConfig(newCfg)
		// fmt.Printf("client Get: updated config to %+v\n", ck.config)
		ck.mu.Unlock()
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.seqId++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	ck.mu.Unlock()


	for {
		ck.mu.Lock()
		config := ck.config
		ck.mu.Unlock()

		shard := key2shard(key)
		gid := config.Shards[shard]
		if servers, ok := config.Groups[gid]; ok {
			ck.mu.Lock()
			leader := ck.leaderIds[gid]
			ck.mu.Unlock()
			if leader >= len(servers) {
				leader = 0
			}

			for tried := 0; tried < len(servers); tried++ {
				args.Shard = shard
				args.ConfigNum = config.Num
				srv := ck.make_end(servers[leader])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)

				if ok && reply.Err == OK {
					// 成功，更新leaderId
					ck.mu.Lock()
					ck.leaderIds[gid] = leader
					ck.mu.Unlock()
					return
				}

				if ok && reply.Err == ErrWrongGroup {
					// 当前组不负责这个shard，退出尝试刷新配置
					break
				}

				// 失败或者非leader，试下一个服务器
				leader = (leader + 1) % len(servers)
			}
		}
		time.Sleep(100 * time.Millisecond)
		newCfg := ck.sm.Query(-1)
		ck.mu.Lock()
		ck.config = deepCopyConfig(newCfg)
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}