package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int
    clientId  int64
    requestId int
    mu sync.Mutex
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
	// Your code here.
	ck.clientId = nrand()
    ck.leaderId = 0
    ck.requestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
    ck.mu.Lock()
	ck.requestId++
    args := &QueryArgs{
        Num:       num,
        ClientId:  ck.clientId,
        RequestId: ck.requestId,
    }
    ck.mu.Unlock()

	for {
        ck.mu.Lock()
        srv := ck.servers[ck.leaderId]
        ck.mu.Unlock()
        var reply QueryReply
        ok := srv.Call("ShardCtrler.Query", args, &reply)
        if ok && !reply.WrongLeader && reply.Err == OK {
            return reply.Config
        }
        ck.mu.Lock()
        ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        ck.mu.Unlock()
        time.Sleep(50 * time.Millisecond)
    }
}

func (ck *Clerk) Join(servers map[int][]string) {
    ck.mu.Lock()
	ck.requestId++
    args := &JoinArgs{
        Servers:   servers,
        ClientId:  ck.clientId,
        RequestId: ck.requestId,
    }
    ck.mu.Unlock()

	for {
        ck.mu.Lock()
        srv := ck.servers[ck.leaderId]
        ck.mu.Unlock()
        var reply JoinReply
        ok := srv.Call("ShardCtrler.Join", args, &reply)
        if ok && !reply.WrongLeader && reply.Err == OK {
            return
        }
        ck.mu.Lock()
        ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        ck.mu.Unlock()
        time.Sleep(50 * time.Millisecond)
    }
}

func (ck *Clerk) Leave(gids []int) {
    ck.mu.Lock()
	ck.requestId++
    args := &LeaveArgs{
        GIDs:      gids,
        ClientId:  ck.clientId,
        RequestId: ck.requestId,
    }
    ck.mu.Unlock()

	for {
        ck.mu.Lock()
        srv := ck.servers[ck.leaderId]
        ck.mu.Unlock()
        var reply LeaveReply
        ok := srv.Call("ShardCtrler.Leave", args, &reply)
        if ok && !reply.WrongLeader && reply.Err == OK {
            return
        }
        ck.mu.Lock()
        ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        ck.mu.Unlock()
        time.Sleep(50 * time.Millisecond)
    }
}

func (ck *Clerk) Move(shard int, gid int) {
    ck.mu.Lock()
	ck.requestId++
    args := &MoveArgs{
        Shard:     shard,
        GID:       gid,
        ClientId:  ck.clientId,
        RequestId: ck.requestId,
    }
    ck.mu.Unlock()

	for {
        ck.mu.Lock()
        srv := ck.servers[ck.leaderId]
        ck.mu.Unlock()
        var reply MoveReply
        ok := srv.Call("ShardCtrler.Move", args, &reply)
        if ok && !reply.WrongLeader && reply.Err == OK {
            return
        }
        ck.mu.Lock()
        ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        ck.mu.Unlock()
        time.Sleep(50 * time.Millisecond)
    }
}
