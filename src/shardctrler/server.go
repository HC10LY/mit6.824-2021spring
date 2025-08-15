package shardctrler

import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "time"
import "sort"

// ShardCtrler is the Raft-backed shard controller
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// persistent state (the logical state machine)
	configs []Config // indexed by config num

	// ephemeral
	lastRequest map[int64]int                     // clientId -> last seen requestId
	waitChs     map[int]chan ApplyResult          // raft log index -> chan to notify RPC waiter
	// (no snapshot support here)
}

type ApplyResult struct {
	Op     Op
	Config Config // snapshot of config after applying (useful for Query)
}

type Op struct {
	// common
	OpType    string // "Join", "Leave", "Move", "Query"
	ClientId  int64
	RequestId int

	// Join
	JoinServers map[int][]string

	// Leave
	LeaveGIDs []int

	// Move
	MoveShard int
	MoveGID   int

	// Query
	QueryNum int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		OpType:     "Join",
		ClientId:   args.ClientId,
		RequestId:  args.RequestId,
		JoinServers: copyServersMap(args.Servers),
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "ErrWrongLeader"
		return
	}

	// wait for apply
	res, ok := sc.waitApply(index, op)
	if !ok {
		reply.WrongLeader = true
		reply.Err = "ErrTimeout"
		return
	}
	// success
	reply.WrongLeader = false
	reply.Err = OK
	_ = res
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		OpType:    "Leave",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		LeaveGIDs: append([]int(nil), args.GIDs...),
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "ErrWrongLeader"
		return
	}
	res, ok := sc.waitApply(index, op)
	if !ok {
		reply.WrongLeader = true
		reply.Err = "ErrTimeout"
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
	_ = res
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		OpType:    "Move",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		MoveShard: args.Shard,
		MoveGID:   args.GID,
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "ErrWrongLeader"
		return
	}
	res, ok := sc.waitApply(index, op)
	if !ok {
		reply.WrongLeader = true
		reply.Err = "ErrTimeout"
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
	_ = res
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		OpType:    "Query",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		QueryNum:  args.Num,
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "ErrWrongLeader"
		return
	}
	res, ok := sc.waitApply(index, op)
	if !ok {
		reply.WrongLeader = true
		reply.Err = "ErrTimeout"
		return
	}
	// fill reply.Config based on the applied state and query num
	reply.WrongLeader = false
	reply.Err = OK

	sc.mu.Lock()
	// If Query num is -1 or > last, return latest
	if args.Num == -1 || args.Num >= len(sc.configs) {
		reply.Config = sc.copyConfig(sc.configs[len(sc.configs)-1])
	} else {
		reply.Config = sc.copyConfig(sc.configs[args.Num])
	}
	sc.mu.Unlock()
	_ = res
}

// Kill is called by tester to shutdown this server.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

// Raft accessor for shardkv tests
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	sc := new(ShardCtrler)
	sc.me = me

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// init state
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	// all shards default to 0 (already zero value)

	sc.lastRequest = make(map[int64]int)
	sc.waitChs = make(map[int]chan ApplyResult)

	// start apply loop
	go sc.applyLoop()

	return sc
}

/*** helper functions ***/

func (sc *ShardCtrler) waitApply(index int, expected Op) (ApplyResult, bool) {
	ch := make(chan ApplyResult, 1)

	sc.mu.Lock()
	sc.waitChs[index] = ch
	sc.mu.Unlock()

	// wait with timeout
	select {
	case res := <-ch:
		// ensure that the applied op matches our request (clientId & requestId)
		if res.Op.ClientId == expected.ClientId && res.Op.RequestId == expected.RequestId {
			// success
			sc.mu.Lock()
			delete(sc.waitChs, index)
			sc.mu.Unlock()
			return res, true
		} else {
			// someone else wrote at this index (rare), treat as failure for leader
			sc.mu.Lock()
			delete(sc.waitChs, index)
			sc.mu.Unlock()
			return ApplyResult{}, false
		}
	case <-time.After(800 * time.Millisecond):
		// timeout
		sc.mu.Lock()
		delete(sc.waitChs, index)
		sc.mu.Unlock()
		return ApplyResult{}, false
	}
}

func (sc *ShardCtrler) applyLoop() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				// ignore unexpected type
				continue
			}

			sc.mu.Lock()
			// default: not duplicate
			isDup := false
			if last, exists := sc.lastRequest[op.ClientId]; exists {
				if op.RequestId <= last {
					isDup = true
				}
			}

			var appliedConfig Config
			// For Query, we still go through Raft and produce an ApplyResult; don't modify configs unless op is not duplicate and mutating
			// If op is not duplicate and is a mutating operation -> modify configs
			if !isDup {
				switch op.OpType {
				case "Join":
					sc.applyJoin(op.JoinServers)
				case "Leave":
					sc.applyLeave(op.LeaveGIDs)
				case "Move":
					sc.applyMove(op.MoveShard, op.MoveGID)
				case "Query":
					// nothing to change in state machine for Query
				}
				// update lastRequest for this client
				sc.lastRequest[op.ClientId] = op.RequestId
			}
			// snapshot config state for result (always reflect current last config)
			appliedConfig = sc.copyConfig(sc.configs[len(sc.configs)-1])

			// If there's a waiting RPC for this log index, notify it
			if ch, ok := sc.waitChs[msg.CommandIndex]; ok {
				select {
				case ch <- ApplyResult{Op: op, Config: appliedConfig}:
				default:
					// avoid blocking if nobody is listening
				}
			}
			sc.mu.Unlock()
		} else {
			// ignore other messages (e.g., snapshots if not used)
		}
	}
}

/*** state-machine mutation helpers ***/

// deep copy servers map
func copyServersMap(src map[int][]string) map[int][]string {
	if src == nil {
		return nil
	}
	dst := make(map[int][]string, len(src))
	for k, v := range src {
		slice := make([]string, len(v))
		copy(slice, v)
		dst[k] = slice
	}
	return dst
}

// create a deep copy of a Config
func (sc *ShardCtrler) copyConfig(c Config) Config {
	var nc Config
	nc.Num = c.Num
	// copy Shards array
	for i := 0; i < NShards; i++ {
		nc.Shards[i] = c.Shards[i]
	}
	// deep copy Groups map
	nc.Groups = make(map[int][]string)
	for k, v := range c.Groups {
		slice := make([]string, len(v))
		copy(slice, v)
		nc.Groups[k] = slice
	}
	return nc
}

func (sc *ShardCtrler) applyJoin(newServers map[int][]string) {
	// create new config from last
	last := sc.configs[len(sc.configs)-1]
	newc := sc.copyConfig(last)
	newc.Num = last.Num + 1

	// add new servers (deep copy)
	for gid, servers := range newServers {
		// if gid already exists, overwrite with new server list (allowed)
		slice := make([]string, len(servers))
		copy(slice, servers)
		newc.Groups[gid] = slice
	}

	// rebalance
	sc.rebalance(&newc)

	sc.configs = append(sc.configs, newc)
}

func (sc *ShardCtrler) applyLeave(gids []int) {
	last := sc.configs[len(sc.configs)-1]
	newc := sc.copyConfig(last)
	newc.Num = last.Num + 1

	// remove gids
	for _, gid := range gids {
		delete(newc.Groups, gid)
	}

	// rebalance
	sc.rebalance(&newc)

	sc.configs = append(sc.configs, newc)
}

func (sc *ShardCtrler) applyMove(shard int, gid int) {
	last := sc.configs[len(sc.configs)-1]
	newc := sc.copyConfig(last)
	newc.Num = last.Num + 1

	// assign shard to gid (even if gid==0 or not in Groups; Move should still set)
	if shard >= 0 && shard < NShards {
		newc.Shards[shard] = gid
	}

	// No rebalance for Move (Move is explicit)
	sc.configs = append(sc.configs, newc)
}

/*** rebalance algorithm (deterministic, try to minimize moves) ***/
func (sc *ShardCtrler) rebalance(c *Config) {
	// If no groups -> all shards to 0
	if len(c.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			c.Shards[i] = 0
		}
		return
	}

	// deterministic order of gids
	gids := make([]int, 0, len(c.Groups))
	for gid := range c.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// desired target counts
	numGroups := len(gids)
	base := NShards / numGroups
	rem := NShards % numGroups
	target := make(map[int]int)
	for i, gid := range gids {
		target[gid] = base
		if i < rem {
			target[gid]++
		}
	}

	// current counts (only count shards belonging to existing gids)
	count := make(map[int]int)
	for _, gid := range gids {
		count[gid] = 0
	}
	// count and also mark shards whose current gid is not in groups
	badShards := make([]int, 0) // shards assigned to non-existing gid -> must reassign
	for i := 0; i < NShards; i++ {
		gid := c.Shards[i]
		if _, ok := c.Groups[gid]; !ok {
			// not an existing group => needs reassignment
			badShards = append(badShards, i)
		} else {
			count[gid]++
		}
	}

	// build list of underfull gids (need more shards), with repetitions equal to deficit
	under := make([]int, 0)
	for _, gid := range gids {
		if count[gid] < target[gid] {
			for k := 0; k < target[gid]-count[gid]; k++ {
				under = append(under, gid)
			}
		}
	}

	// build list of overfull shards (shard indices that can be moved)
	overShards := make([]int, 0)
	for i := 0; i < NShards; i++ {
		gid := c.Shards[i]
		if _, ok := c.Groups[gid]; !ok {
			// already included in badShards; skip here
			continue
		}
		if count[gid] > target[gid] {
			overShards = append(overShards, i)
			// decrement count because we'll consider this shard movable
			count[gid]--
		}
	}

	// preferred move candidates: first badShards (must assign), then overShards
	moveCandidates := append(badShards, overShards...)

	// Assign moveCandidates to under (one by one)
	j := 0
	for _, shardIdx := range moveCandidates {
		if j >= len(under) {
			break
		}
		toGid := under[j]
		c.Shards[shardIdx] = toGid
		j++
	}

	// any remaining shards (shouldn't be) remain as-is
}
