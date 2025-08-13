package shardkv

import (
	"bytes"
	// "fmt"
	// "fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

// import "fmt"



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string // "Get", "Put", "Append", "ConfigUpdate", "ShardData"
    Key       string
    Value     string
    ClientId  int64
    SeqId     int
    Shard     int
    ConfigNum int
    NewStatus int

    // 迁移相关
    Data      map[string]string // Shard 数据（用于迁移）
    LastSeq   map[int64]int     // 对应 shard 客户端最大序号（去重）
    ShardId   int
    NewConfigNum int
}

type CommandResult struct {
	Err       Err
	Value     string // 仅 Get 有返回值
	ClientId  int64
	RequestId int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck          *shardctrler.Clerk

    config       shardctrler.Config // 当前配置
	nextConfig 	 shardctrler.Config // 下一配置
    lastApplied  int

	notifyChans sync.Map

    kvStore      map[string]string       // 具体KV数据，按分片统一管理
    clientSeq    map[int64]int           // clientId -> 已处理最大SeqId，去重用
    shardStatus  [shardctrler.NShards]int // 0=NotOwned, 1=Serving, 2=Pulling, 3=BePulling

    dead       int32
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
    // 判断当前是否负责该 shard（简化示范，实际要更严格判断）
    shard := args.Shard
    if kv.shardStatus[shard] != 1 || args.ConfigNum != kv.config.Num { // 1 = Serving
        // fmt.Printf("Get called for shard %d, but %d of gid %d not in Serving state state is %d or not owned by this config=%+v\n", shard, kv.me, kv.gid, kv.shardStatus[shard], kv.config)
        kv.mu.Unlock()
        reply.Err = ErrWrongGroup
        return
    }
    kv.mu.Unlock()

    op := Op{
        Type:      "Get",
        Key:       args.Key,
        ClientId:  args.ClientId,
        SeqId:     args.SeqId,
        Shard:     args.Shard,
        ConfigNum: args.ConfigNum,
    }

    index, _, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    ch := make(chan CommandResult, 1)
    kv.notifyChans.Store(index, ch)

    select {
    case res := <-ch:
        if res.ClientId == args.ClientId && res.RequestId == args.SeqId {
            reply.Err = res.Err
            reply.Value = res.Value
        } else {
            reply.Err = ErrWrongLeader
        }
    case <-time.After(300 * time.Millisecond):
        reply.Err = ErrWrongLeader
    }
	// fmt.Printf("Get called with args: %+v, reply: %+v\n", args, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// fmt.Printf("PutAppend called with args.num %d\n", args.ConfigNum)
	kv.mu.Lock()
    shard := args.Shard
    if kv.shardStatus[shard] != 1 || args.ConfigNum != kv.config.Num {
        kv.mu.Unlock()
        reply.Err = ErrWrongGroup
		// fmt.Printf("PutAppend called for shard %d, but %d of gid %d not in Serving state state is %+v or not owned by this config=%+v\n", shard, kv.me, kv.gid, kv.shardStatus, kv.config)
        return
    }
    kv.mu.Unlock()

    op := Op{
        Type:      args.Op,
        Key:       args.Key,
        Value:     args.Value,
        ClientId:  args.ClientId,
        SeqId:     args.SeqId,
        Shard:     args.Shard,
        ConfigNum: args.ConfigNum,
    }

	// fmt.Printf("PutAppend op: %+v\n", op)
    index, _, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

	ch := make(chan CommandResult, 1)
    kv.notifyChans.Store(index, ch)

    select {
    case res := <-ch:
        if res.ClientId == args.ClientId && res.RequestId == args.SeqId {
            reply.Err = res.Err
        } else {
            reply.Err = ErrWrongLeader
        }
    case <-time.After(300 * time.Millisecond):
        reply.Err = ErrWrongLeader
    }
}

func (kv *ShardKV) allShardsIdle() bool {
    for _, status := range kv.shardStatus {
        if status != 1 && status != 0 {
            return false
        }
    }
    return true
}

func (kv *ShardKV) configPoller() {
    for {
        time.Sleep(100 * time.Millisecond) // 轮询间隔
        kv.mu.Lock()
        if kv.killed() {
            kv.mu.Unlock()
            return
        }

        _, isLeader := kv.rf.GetState()
        if !isLeader {
            kv.mu.Unlock()
            continue
        }
        
        // 如果有分片还在迁移/GC，就跳过本轮
        if !kv.allShardsIdle() {
            kv.mu.Unlock()
            continue
        }

        nextConfigNum := kv.config.Num + 1

        // 查询 shardctrler 最新配置
        newConfig := kv.mck.Query(nextConfigNum)

        // fmt.Printf("gid %d configPoller checking for next config %d, current pendingConfigNum %d\n", kv.gid, nextConfigNum, kv.pendingConfigNum)
        
		if kv.nextConfig.Num >= newConfig.Num || newConfig.Num <= kv.config.Num {
            kv.mu.Unlock()
            // fmt.Printf("gid %d not need send config update for %d\n", kv.gid, nextConfigNum)
            continue
        }

        // fmt.Printf("gid %d configPoller found new config: %+v\n", kv.gid, newConfig)

        kv.mu.Unlock()
        
        if newConfig.Num == nextConfigNum {
            // 通过 Raft 提交配置变更命令，触发内部应用
            op := Op{
                Type:      "ConfigUpdate",
                NewConfigNum: newConfig.Num,
            }
			// fmt.Printf("gid %d Submitting config update op: %+v\n", kv.gid, op)
            _, _, isLeader := kv.rf.Start(op)
            if isLeader {
                // fmt.Printf("gid %d configPoller submitted config update for %d, now pendingConfigNum %d  kv.confignum %d\n", kv.gid, newConfig.Num, kv.pendingConfigNum, kv.config.Num)
            }
        }
    }
}

type FetchShardArgs struct {
    Shard     int
    ConfigNum int
}

type FetchShardReply struct {
    Err       Err
    Data      map[string]string
    LastSeq   map[int64]int
}

func (kv *ShardKV) FetchShardData(args *FetchShardArgs, reply *FetchShardReply) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    _, isLeader := kv.rf.GetState()
    if !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    lastConfig := kv.mck.Query(args.ConfigNum)

    if lastConfig.Num != args.ConfigNum || lastConfig.Shards[args.Shard] != kv.gid {
        reply.Err = ErrWrongGroup
        return
    }

    // 判断是否负责该 shard
    if kv.shardStatus[args.Shard] != 3 || args.ConfigNum != kv.config.Num {
		// fmt.Printf("FetchShardData called for shard %d, but not in BePulling state state is %d or not owned by this server.gid=%d  config=%+v\n", args.Shard, kv.shardStatus[args.Shard], kv.gid, kv.config)
        reply.Err = ErrWrongGroup
        return
    }

    data := make(map[string]string)
    for k, v := range kv.kvStore {
        if key2shard(k) == args.Shard {
            data[k] = v
        }
    }

    lastSeqCopy := make(map[int64]int)
    for clientId, seq := range kv.clientSeq {
        lastSeqCopy[clientId] = seq
    }

    reply.Err = OK
    reply.Data = data
    reply.LastSeq = lastSeqCopy
}

func (kv *ShardKV) pollPullShards() {
    for {
        time.Sleep(100 * time.Millisecond)

        kv.mu.Lock()
        if kv.killed() {
            kv.mu.Unlock()
            return
        }

        _, isLeader := kv.rf.GetState()
        if !isLeader {
            kv.mu.Unlock()
            continue
        }

        localConfigNum := kv.config.Num
        shardList := []int{}
        for shard, status := range kv.shardStatus {
            if status == 2 {
                shardList = append(shardList, shard)
            }
        }
        kv.mu.Unlock()

        if len(shardList) == 0 {
            continue
        }

        // RPC 查询旧配置（无锁）
        oldConfig := kv.mck.Query(localConfigNum)

        for _, shard := range shardList {
            oldGid := oldConfig.Shards[shard]
            servers, ok := oldConfig.Groups[oldGid]
            if !ok || len(servers) == 0 {
                op := Op{
                    Type:         "ShardNoSource",
                    ShardId:      shard,
                    NewConfigNum: localConfigNum,
                }
                kv.rf.Start(op)
                continue
            }

            args := &FetchShardArgs{
                Shard:     shard,
                ConfigNum: localConfigNum,
            }

            for _, server := range servers {
                srv := kv.make_end(server)
                var reply FetchShardReply
                if ok := srv.Call("ShardKV.FetchShardData", args, &reply); ok && reply.Err == OK {
                    lastSeqCopy := make(map[int64]int)
                    for k, v := range reply.LastSeq {
                        lastSeqCopy[k] = v
                    }
                    dataCopy := make(map[string]string)
                    for k, v := range reply.Data {
                        dataCopy[k] = v
                    }

                    op := Op{
                        Type:         "ShardData",
                        ShardId:      shard,
                        Data:         dataCopy,
                        LastSeq:      lastSeqCopy,
                        NewConfigNum: localConfigNum,
                    }
                    kv.rf.Start(op)
                    break
                }
            }
        }
    }
}

type ShardStatusArgs struct {
    Shard    int
    ConfigNum int // 查询的配置版本
}

type ShardStatusReply struct {
    Status int // shard 状态，比如 0=NotOwned,1=Serving,2=Pulling,3=BePulling
}

func (kv *ShardKV) FetchShardStatus(args *ShardStatusArgs, reply *ShardStatusReply) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    _, isLeader := kv.rf.GetState()
    if !isLeader {
        reply.Status = 0 // 或者专门设计一个错误字段，告诉调用者不是leader
        return
    }

    shard := args.Shard
    if shard < 0 || shard >= len(kv.shardStatus) {
        reply.Status = 0
        return
    }

    if args.ConfigNum < kv.config.Num {
        reply.Status = 1
        return
    }

    if args.ConfigNum == kv.config.Num {
        reply.Status = kv.shardStatus[shard]
        return
    }

    // reply.Status = kv.shardStatus[shard]
}

func (kv *ShardKV) pollBePullShards() {
    for {
        time.Sleep(100 * time.Millisecond)

        kv.mu.Lock()
        if kv.killed() {
            kv.mu.Unlock()
            return
        }
        _, isLeader := kv.rf.GetState()
        if !isLeader {
            kv.mu.Unlock()
            continue
        }

        // 拷贝本地状态，减少锁持有时间
        localConfigNum := kv.config.Num
        shardStatusCopy := make(map[int]int)
        for k, v := range kv.shardStatus {
            shardStatusCopy[k] = v
        }
        kv.mu.Unlock()

        for shard, status := range shardStatusCopy {
            if status != 3 { // BePulling
                continue
            }

            // RPC 调用 shardmaster，不持锁
            nextConfig := kv.mck.Query(localConfigNum + 1)
            if nextConfig.Num != localConfigNum+1 {
                continue // 不影响其他 shard
            }

            newGid := nextConfig.Shards[shard]
            if newGid == kv.gid {
                continue
            }

            // 检查新组是否已 Serving
            servers := nextConfig.Groups[newGid]
            received := false
            for _, server := range servers {
                srv := kv.make_end(server)
                var reply ShardStatusReply
                args := &ShardStatusArgs{Shard: shard, ConfigNum: localConfigNum}
                ok := srv.Call("ShardKV.FetchShardStatus", args, &reply)
                if ok && reply.Status == 1 {
                    received = true
                    break
                }
            }

            if received {
                kv.mu.Lock()
                // 再次确认状态没被其他人改
                if kv.shardStatus[shard] == 3 && kv.config.Num == localConfigNum {
                    op := Op{
                        Type:         "ShardGone",
                        ShardId:      shard,
                        NewConfigNum: kv.config.Num,
                    }
                    kv.rf.Start(op)
                }
                kv.mu.Unlock()
            }
        }
    }
}

/*
type AckShardArgs struct {
    Shard     int
    ConfigNum int
	Gid	      int // 发送方组ID
}

type AckShardReply struct {
    Err Err
}

func (kv *ShardKV) AckShardReceived(args *AckShardArgs, reply *AckShardReply) {
    kv.mu.Lock()

    lastconfig := kv.mck.Query(args.ConfigNum)
    if lastconfig.Num != args.ConfigNum || lastconfig.Shards[args.Shard] != kv.gid {
        reply.Err = ErrWrongGroup
        kv.mu.Unlock()
        return
    }

    _, isLeader := kv.rf.GetState()
    if !isLeader {
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    }

    if args.Gid == kv.gid {
        reply.Err = OK
        return
    }

    // fmt.Printf("gid %d AckShardReceived called for shard %d, config %d local gid %d shardstatus %+v config.num %d  config.shard %+v\n", args.Gid, args.Shard, args.ConfigNum, kv.gid, kv.shardStatus, kv.config.Num, lastconfig.Shards)

    if kv.shardStatus[args.Shard] == 3 && args.ConfigNum == kv.config.Num {
        op := Op{
            Type: "AckShard",
            ShardId: args.Shard,
            NewConfigNum: args.ConfigNum,
        }
        index, _, isLeader := kv.rf.Start(op)
        if !isLeader {
            reply.Err = ErrWrongLeader
            kv.mu.Unlock()
            return
        }

        ch := make(chan CommandResult, 1)
        kv.notifyChans.Store(index, ch)
        kv.mu.Unlock()

        select {
        case res := <-ch:
            kv.mu.Lock()
            reply.Err = res.Err
            kv.mu.Unlock()
        case <-time.After(time.Second):
            kv.mu.Lock()
            // reply.Err = ErrTimeout
            kv.mu.Unlock()
        }

        // kv.notifyChans.Delete(index)
        return
    }

    if kv.config.Num > args.ConfigNum {
        reply.Err = OK
    }

    // reply.Err = OK
    kv.mu.Unlock()
}

func (kv *ShardKV) sendShardReceiveAck(shard int, oldConfigNum int, oldGid int) bool {
    args := &AckShardArgs{
        Shard:      shard,
        ConfigNum:  oldConfigNum,
        Gid:        kv.gid,       // 发送方自己的组ID
    }
    // 查询旧配置的服务器列表
    oldConfig := kv.mck.Query(oldConfigNum)
    if oldConfig.Num != oldConfigNum {
        return false
    }
    servers, ok := oldConfig.Groups[oldGid]
    if !ok || len(servers) == 0 {
        // 没有旧服务器可通知，直接返回
        return true
    }

    timeout := time.After(5 * time.Second) // 超时时间，可根据需要调整
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-timeout:
            // 超时，返回失败
            return false
        case <-ticker.C:
            if kv.killed() {
                return false
            }
            for _, server := range servers {
                srv := kv.make_end(server)
                var reply AckShardReply
                ok := srv.Call("ShardKV.AckShardReceived", args, &reply)
                if ok && reply.Err == OK {
                    return true // 收到确认，成功返回
                }
            }
        }
    }
}
*/

func (kv *ShardKV) configAdvancer() {
    for {
        time.Sleep(100 * time.Millisecond)

        kv.mu.Lock()
        if kv.killed() {
            kv.mu.Unlock()
            return
        }

        _, isLeader := kv.rf.GetState()
        if !isLeader {
            kv.mu.Unlock()
            continue
        }

        // fmt.Printf("gid %d Checking if can advance config from %d to %d\n", kv.gid, kv.config.Num, kv.nextConfig.Num)

        if kv.nextConfig.Num == kv.config.Num + 1 {
            canAdvance := true
            for _, status := range kv.shardStatus {
                if status == 2 || status == 3 {
                    // fmt.Printf("gid %d Cannot advance config from %d to %d: shard status %d is not ready shards %+v\n", kv.gid, kv.config.Num, kv.nextConfig.Num, status, kv.shardStatus)
                    canAdvance = false
                    break
                }
            }
            if canAdvance {
                op := Op{
                    Type:      "ConfigAdvance",
                    ConfigNum: kv.nextConfig.Num,
                }
                _, _, isLeader := kv.rf.Start(op)
                if !isLeader {
                    // leader 可能变了，稍后重试
                }
            }
        }
        kv.mu.Unlock()
    }
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) doSnapshot(lastApplied int) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.clientSeq)
	e.Encode(kv.config)
    e.Encode(kv.nextConfig)
	e.Encode(kv.shardStatus)
	e.Encode(lastApplied)

	data := w.Bytes()

	// fmt.Printf("Server %d (GID %d) creating snapshot at index %d, data size %d bytes\n", kv.me, kv.gid, lastApplied, len(data))

    kv.rf.Snapshot(lastApplied, data)
}

func (kv *ShardKV) readSnapshot(snapshot []byte) bool {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    if snapshot == nil || len(snapshot) < 1 {
        return false // 无快照或空快照
    }

    r := bytes.NewBuffer(snapshot)
    d := labgob.NewDecoder(r)

    var kvStore map[string]string
	var clientSeq map[int64]int
	var config shardctrler.Config
    var nextConfig shardctrler.Config
	var shardStatus [shardctrler.NShards]int
	var lastApplied int



	if d.Decode(&kvStore) != nil ||
		d.Decode(&clientSeq) != nil ||
		d.Decode(&config) != nil ||
        d.Decode(&nextConfig) != nil ||
		d.Decode(&shardStatus) != nil ||
		d.Decode(&lastApplied) != nil {
		return false // 解码失败
	}

    kv.kvStore = kvStore
	kv.clientSeq = clientSeq
	kv.config = config
    kv.nextConfig = nextConfig      // 恢复 nextConfig
	kv.shardStatus = shardStatus
	kv.lastApplied = lastApplied

	// fmt.Printf("Server %d (GID %d) restored snapshot at index %d, data size %d bytes\n", kv.me, kv.gid, lastApplied, len(snapshot))

	return true
}

/*
func (kv *ShardKV) asyncSendShardReceiveAck(shard int, configNum int) {
    for {
        kv.mu.Lock()
        _, isLeader := kv.rf.GetState()
        if kv.killed() || !isLeader || kv.config.Num != configNum || kv.shardStatus[shard] != 2 {
            // 不满足条件，退出重试循环
            kv.mu.Unlock()
            return
        }
        oldConfig := kv.mck.Query(configNum)
        oldGid := oldConfig.Shards[shard]
        // fmt.Printf("gid %d sendShardReceiveAck confignum %d called for shard %d, config %+v\n", kv.gid, configNum, shard, oldConfig)
        kv.mu.Unlock()

        success := kv.sendShardReceiveAck(shard, configNum, oldGid)
        if success {
            // 成功后提交一个 Raft 命令更新状态为 Serving (1)
            op := Op{
                Type:       "ShardReceiveAcked",
                ShardId:    shard,
                NewConfigNum: configNum,
            }
            index, _, isLeader := kv.rf.Start(op)
            if !isLeader {
                // 不是Leader了，退出
                return
            }

            // 等待该命令被应用
            ch := make(chan CommandResult, 1)
            kv.notifyChans.Store(index, ch)

            select {
            case <-ch:
                // 更新成功，退出循环
                kv.notifyChans.Delete(index)
                return
            case <-time.After(3 * time.Second):
                // 超时重试
            }
        } else {
            // 失败重试，sleep 等待下一次尝试
            time.Sleep(500 * time.Millisecond)
        }
    }
}
*/

func (kv *ShardKV) applyLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.mu.Lock()
			// 快照时可能回退，不应用老日志
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}

			var res Err = OK

			switch op.Type {
			case "Get":
                if op.ConfigNum != kv.config.Num {
					res = ErrWrongGroup
				} else {
                    shard := key2shard(op.Key)
                    if kv.config.Shards[shard] != kv.gid || kv.shardStatus[shard] != 1 {
                        res = ErrWrongGroup
                        // Get 不修改状态，无需去重
                    } else {
                        // Get 操作一般不修改状态机，返回值通过 notify 通道回复给 RPC handler
                        // 这里不做修改，只做基本验证
                    }
                }

			case "Put", "Append":
                if op.ConfigNum != kv.config.Num {
					res = ErrWrongGroup
				} else {
                    shard := key2shard(op.Key)
                    if kv.config.Shards[shard] != kv.gid || kv.shardStatus[shard] != 1 {
                        res = ErrWrongGroup
                    } else {
                        // 去重判断
                        lastSeq, ok := kv.clientSeq[op.ClientId]
                        if !ok || op.SeqId > lastSeq {
                            if op.Type == "Put" {
                                kv.kvStore[op.Key] = op.Value
                                // fmt.Printf("%d of gid %d Put %s = %s in kv[%s]\n", kv.me, kv.gid, op.Key, op.Value, op.Key)
                            } else {
                                kv.kvStore[op.Key] += op.Value
                                // fmt.Printf("%d of gid %d Appended %s to %s now kv[%s] = %s\n", kv.me, kv.gid, op.Value, op.Key, op.Key, kv.kvStore[op.Key])
                            }
                            kv.clientSeq[op.ClientId] = op.SeqId
                        }
				    }
                }

			case "ConfigUpdate":
				// fmt.Printf("Applying ConfigUpdate for config num %d\n", op.ConfigNum)
				if op.NewConfigNum == kv.config.Num + 1 {
					oldConfig := deepCopyConfig(kv.config)
					kv.nextConfig = kv.mck.Query(op.NewConfigNum)

					// 更新 shardStatus 逻辑，准备迁移或接收数据
					for shard := 0; shard < shardctrler.NShards; shard++ {
						oldOwner := oldConfig.Shards[shard]
						newOwner := kv.nextConfig.Shards[shard]
                        // fmt.Printf("%d of gid %d ConfigUpdate to %d shard %d oldOwner %d newOwner %d\n",kv.me, kv.gid, op.NewConfigNum, shard, oldOwner, newOwner)
						if oldOwner != kv.gid && newOwner == kv.gid {
							kv.shardStatus[shard] = 2 // Pulling
						} else if oldOwner == kv.gid && newOwner != kv.gid {
                            // fmt.Printf("gid %d will lose shard %d to new owner %d, setting BePulling\n", kv.gid, shard, newOwner)
							kv.shardStatus[shard] = 3 // BePulling
						} else if newOwner == kv.gid {
							kv.shardStatus[shard] = 1 // Serving
						} else {
							kv.shardStatus[shard] = 0 // NotOwned
						}
					}
				}
				// fmt.Printf("gid %d Config updated to %d new shardStatus: %+v\n", kv.gid, kv.config.Num+1, kv.shardStatus)

			case "ShardData":
				// fmt.Printf("Received ShardData updated shardStatus: %+v\n", kv.shardStatus)
				if op.NewConfigNum == kv.config.Num {
					shard := op.ShardId
					if kv.shardStatus[shard] == 2 { // Pulling 状态接收数据
						for k, v := range op.Data {
							kv.kvStore[k] = v
						}

						for clientId, seq := range op.LastSeq {
							if oldSeq, ok := kv.clientSeq[clientId]; !ok || seq > oldSeq {
								kv.clientSeq[clientId] = seq
							}
						}
						kv.shardStatus[shard] = 1
						
						// 异步通知旧组确认
						// go kv.asyncSendShardReceiveAck(shard, kv.config.Num)
					}
				}

            case "ConfigAdvance":
                // fmt.Printf("gid %d applying config advance to %d kv.next %d\n", kv.gid, op.ConfigNum, kv.nextConfig.Num)
                if op.ConfigNum == kv.nextConfig.Num {
                    kv.config = deepCopyConfig(kv.nextConfig)
                    kv.nextConfig = shardctrler.Config{}
                    // 可以打印日志方便调试
                    // fmt.Printf("%d of gid %d advanced config to %d\n", kv.me, kv.gid, kv.config.Num)
                }

            case "ShardReceiveAcked":
                if op.NewConfigNum == kv.config.Num {
                    shard := op.ShardId
                    if kv.shardStatus[shard] == 2 {
                        // fmt.Printf("%d of gid %d acked shard %d, changing status to Serving at config %d\n", kv.me, kv.gid, shard, kv.config.Num)
                        kv.shardStatus[shard] = 1 // 变成 Serving
                    }
                }

            case "AckShard":
                if op.NewConfigNum == kv.config.Num {
                    shard := op.ShardId
                    if kv.shardStatus[shard] == 3 {
                        // fmt.Printf("%d of gid %d acked shard %d, changing status to NotOwned at config %d\n", kv.me, kv.gid, shard, kv.config.Num)
                        kv.shardStatus[shard] = 0
                        // fmt.Printf("%d of gid %d acked shard %d, shardStatus: %+v\n", kv.me, kv.gid, shard, kv.shardStatus)
                        // 清理 shard 相关数据也可以在这里做
                    }
                }

            case "ShardNoSource":
                shard := op.ShardId
                if op.NewConfigNum == kv.config.Num && kv.shardStatus[shard] == 2 {
                    // fmt.Printf("%d of gid %d received ShardNoSource for shard %d, setting status to Serving at config %d\n", kv.me, kv.gid, op.ShardId, kv.config.Num)
                    kv.shardStatus[op.ShardId] = 1
                }

            case "ShardGone":
                shard := op.ShardId
                if shard >= 0 && shard < len(kv.shardStatus) && kv.shardStatus[shard] == 3 && op.NewConfigNum == kv.config.Num {
                    kv.shardStatus[shard] = 0 // NotOwned
                    // 这里可能还需要清理对应 shard 的数据、lastSeq 等
                    for k := range kv.kvStore {
                        if key2shard(k) == shard {
                            delete(kv.kvStore, k)
                        }
                    }
                }

            case "Nop":
                // 空操作，不修改任何状态，只是为了让 Raft 触发日志复制和状态机推进
                // 这里可以打印调试信息，或者什么都不做

			default:
				// 未知类型，忽略
			}

			kv.lastApplied = msg.CommandIndex

			// Prepare result to notify RPC handler
			var notifyRes CommandResult
			if op.Type == "Get" {
				// 对 Get 请求，需要返回 value 或 ErrWrongGroup
				val := ""
				if res == OK {
					val = kv.kvStore[op.Key]
					// fmt.Printf("Get key %s value: %s\n", op.Key, val)
				}
				notifyRes = CommandResult{
					Err:      res,
					Value:    val,
					ClientId: op.ClientId,
					RequestId:    op.SeqId,
				}
			} else {
				notifyRes = CommandResult{
					Err:      res,
					ClientId: op.ClientId,
					RequestId:    op.SeqId,
				}
			}

			kv.mu.Unlock()

			// 通知等待的RPC线程
			if chVal, ok := kv.notifyChans.Load(msg.CommandIndex); ok {
				ch := chVal.(chan CommandResult)
				select {
				case ch <- notifyRes:
				default:
				}
				close(ch)
				kv.notifyChans.Delete(msg.CommandIndex)
			}

			// 判断是否需要快照
			if kv.maxraftstate != -1 && kv.rf.RaftStateSize() > kv.maxraftstate {
                // fmt.Printf("doSnapshot\n")
				kv.doSnapshot(kv.lastApplied)
			}

		} else if msg.SnapshotValid {
            // fmt.Printf("doSnapshot\n")
            kv.readSnapshot(msg.Snapshot)
            kv.mu.Lock()
            kv.lastApplied = msg.SnapshotIndex
            kv.mu.Unlock()
		}
	}
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	// kv.kvStore = make(map[string]string)
	// kv.clientSeq = make(map[int64]int)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.mck = shardctrler.MakeClerk(ctrlers)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	snapshot := persister.ReadSnapshot()
	if !kv.readSnapshot(snapshot) {
        // fmt.Printf("Failed to read snapshot\n")
        kv.mu.Lock()
        kv.kvStore = make(map[string]string)
        kv.clientSeq = make(map[int64]int)
        kv.config = kv.mck.Query(1)
        for i := 0; i < shardctrler.NShards; i++ {
            if kv.config.Shards[i] == kv.gid {
                kv.shardStatus[i] = 1
            } else {
                kv.shardStatus[i] = 0
            }
        }
        kv.mu.Unlock()
    }

	go kv.applyLoop()
	go kv.configPoller()
    go kv.pollPullShards()
    go kv.pollBePullShards()
	go kv.configAdvancer()
    go kv.NopWorker()

	return kv
}


func deepCopyConfig(c shardctrler.Config) shardctrler.Config {
    // Shards 是数组，直接值拷贝
    newConfig := shardctrler.Config{
        Num:    c.Num,
        Shards: c.Shards,
        Groups: make(map[int][]string),
    }

    // 深拷贝 Groups 中的 map 和 slice
    for gid, servers := range c.Groups {
        copiedServers := make([]string, len(servers))
        copy(copiedServers, servers)
        newConfig.Groups[gid] = copiedServers
    }

    return newConfig
}

func (kv *ShardKV) NopWorker() {
    for !kv.killed() {
        term, isLeader := kv.rf.GetState()
        if isLeader && term != kv.rf.GetLastLogTerm() {
            op := Op{
                Type: "Nop",
            }
            kv.rf.Start(op)
        }
        time.Sleep(100 * time.Millisecond)
    }
}