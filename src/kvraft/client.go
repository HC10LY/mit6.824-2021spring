package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	clientId  int64
    requestId int
    leaderId  int // 最后已知的 leader 优化查找
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
	ck.clientId = nrand()
    ck.requestId = 1
    ck.leaderId = 0
    return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	reqId := ck.requestId
	ck.requestId++
	ck.mu.Unlock()

	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: reqId,
	}

	for {
		server := ck.leaderId
		for i := 0; i < len(ck.servers); i++ {
			si := (server + i) % len(ck.servers)
			var reply GetReply
			ok := ck.servers[si].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				ck.leaderId = si
				return reply.Value
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	reqId := ck.requestId
	ck.requestId++
	ck.mu.Unlock()

	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: reqId,
	}

	for {
		server := ck.leaderId
		for i := 0; i < len(ck.servers); i++ {
			si := (server + i) % len(ck.servers)
			var reply PutAppendReply
			ok := ck.servers[si].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				ck.leaderId = si
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
