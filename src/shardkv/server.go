package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Id   int64
	Name string
	Args interface{}
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	transition_to    int
	operation_number int // agreement number of latest applied operation
	config_now       shardmaster.Config
	storage          map[string]string      // key/value data storage
	cache            map[string]interface{} // "request_id" -> reply cache
	shards           map[int]bool           // shards in charge
}

const (
	Get           = "Get"
	Put           = "Put"
	Append        = "Append"
	ReceiveShard  = "ReceiveShard"
	SentShard     = "SentShard"
	ReconfigStart = "ReconfigStart"
	ReconfigEnd   = "ReconfigEnd"
	Noop          = "Noop"
)

func (self *ShardKV) await_paxos_decision(agreement_number int) (decided_val interface{}) {
	sleep_max := 10 * time.Second
	sleep_time := 10 * time.Millisecond
	for {
		has_decided, decided_val := self.px.Status(agreement_number)
		if has_decided == paxos.Decided {
			return decided_val
		}
		time.Sleep(sleep_time)
		if sleep_time < sleep_max {
			sleep_time *= 2
		}
	}
	panic("unreachable")
}

func (self *ShardKV) paxos_agree(operation Op) int {
	var agreement_number int
	var decided_operation = Op{}

	for decided_operation.Id != operation.Id {
		agreement_number = self.available_agreement_number()
		//fmt.Printf("Proposing %+v with agreement_number:%d\n", operation, agreement_number)
		self.px.Start(agreement_number, operation)
		decided_operation = self.await_paxos_decision(agreement_number).(Op) // type assertion
	}
	//output_debug(fmt.Sprintf("(server%d) Decided op_num:%d op:%v", self.me, agreement_number, decided_operation))
	return agreement_number
}

func (self *ShardKV) paxos_staus(agreement int) (bool, Op) {
	fate, val := self.px.Status(agreement)
	if fate == paxos.Decided && val != nil {
		operation := val.(Op)
		return true, operation
	}
	return false, Op{}
}

func (self *ShardKV) available_agreement_number() int {
	return self.px.Max() + 1
}

func (self *ShardKV) last_operation_number() int {
	return self.operation_number
}

func (self *ShardKV) sync(limit int) {
	seq := self.last_operation_number() + 1

	for seq < limit {
		decided, operation := self.paxos_staus(seq)
		if decided {
			self.perform_operation(seq, operation)
			seq = self.last_operation_number() + 1
		} else {
			noop := make_op("Noop", Op{})
			self.px.Start(seq, noop)
			decided_val := self.await_paxos_decision(seq)
			operation = decided_val.(Op)
			self.perform_operation(seq, operation)
			seq = self.last_operation_number() + 1
		}
	}
}

func (self *ShardKV) perform_operation(agreement int, operation Op) interface{} {
	var result interface{}
	switch operation.Name {
	case Get:
		var get_args = (operation.Args).(GetArgs)
		result = self.doGet(&get_args)
	case Put:
		var put_append_args = (operation.Args).(PutAppendArgs)
		result = self.doPut(&put_append_args)
	case Append:
		var put_append_args = (operation.Args).(PutAppendArgs)
		result = self.doAppend(&put_append_args)
	case ReceiveShard:
		var receive_shard_args = (operation.Args).(ReceiveShardArgs)
		result = self.doReceiveShard(&receive_shard_args)
	case SentShard:
		var sent_shard_args = (operation.Args).(SentShardArgs)
		result = self.doSentShard(&sent_shard_args)
	case ReconfigStart:
		var reconfig_start_args = (operation.Args).(ReconfigStart)
		result = self.doReconfigStart(&reconfig_start_args)
	case ReconfigEnd:
		var reconfig_end_args = (operation.Args).(ReconfigEnd)
		result = self.doReconfigEnd(&reconfig_end_args)
	case Noop:
		//
	default:
		fmt.Printf("do nothing ... \n")
	}
	self.operation_number = agreement
	self.px.Done(agreement)
	return result
}

func make_op(name string, args interface{}) Op {
	operation := Op{Id: generate_uuid(), Name: name, Args: args}
	return operation
}

// RPC handler for client Get requests
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config_now.Num == 0 {
		return nil
	}
	operation := make_op(Get, args)
	agreement := kv.paxos_agree(operation)
	kv.sync(agreement)
	result := kv.perform_operation(agreement, operation)
	get_reply := result.(GetReply)
	reply.Err = get_reply.Err
	reply.Value = get_reply.Value
	return nil
}

func (kv *ShardKV) doGet(args *GetArgs) GetReply {

	shard_index := key2shard(args.Key)
	var reply GetReply
	if kv.gid != kv.config_now.Shards[shard_index] {
		reply.Err = ErrWrongGroup
		reply.Value = ""
		return reply
	}

	if !kv.shards[kv.gid] {
		reply.Err = ErrNotReady
		reply.Value = ""
		return reply
	}

	value, present := kv.storage[args.Key]
	if present {
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}

	return reply
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config_now.Num == 0 {
		return nil
	}
	operation := make_op(args.Op, args)
	agreement := kv.paxos_agree(operation)
	kv.sync(agreement)
	result := kv.perform_operation(agreement, operation)
	put_append_reply := result.(PutAppendReply)
	reply.Err = put_append_reply.Err
	return nil
}

func (kv *ShardKV) doPut(args *PutAppendArgs) PutAppendReply {
	shard_index := key2shard(args.Key)
	var reply PutAppendReply
	if kv.gid != kv.config_now.Shards[shard_index] {
		reply.Err = ErrWrongGroup
		return reply
	}

	if !kv.shards[kv.gid] {
		reply.Err = ErrNotReady
		return reply
	}

	kv.storage[args.Key] = args.Value
	reply.Err = OK
	return reply
}

func (kv *ShardKV) doAppend(args *PutAppendArgs) PutAppendReply {
	shard_index := key2shard(args.Key)
	var reply PutAppendReply
	if kv.gid != kv.config_now.Shards[shard_index] {
		reply.Err = ErrWrongGroup
		return reply
	}

	if !kv.shards[kv.gid] {
		reply.Err = ErrNotReady
		return reply
	}

	value, present := kv.storage[args.Key]
	if present {
		value += args.Value
	} else {
		kv.storage[args.Key] = args.Value
	}
	reply.Err = OK
	return reply
}

func (kv *ShardKV) ReceiveShard(args *ReceiveShardArgs, reply *ReceiveShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config_now.Num == 0 {
		return nil
	}
	operation := make_op(ReceiveShard, args)
	agreement := kv.paxos_agree(operation)
	kv.sync(agreement)
	result := kv.perform_operation(agreement, operation)
	receive_shard_reply := result.(ReceiveShardReply)
	reply.Err = receive_shard_reply.Err
	return nil
}

func (kv *ShardKV) doRceiveShard(args *ReceiveShardArgs) ReceiveShardReply {
	if kv.transition_to > kv.config_now.Num {

	}
}

func (kv *ShardKV) SentShard(args *SentShardArgs, reply *SentShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config_now.Num == 0 {
		return nil
	}
	operation := make_op(SentShard, args)
	agreement := kv.paxos_agree(operation)
	kv.sync(agreement)
	result := kv.perform_operation(agreement, operation)
	sent_shard_reply := result.(SentShardReply)
	reply.Err = sent_shard_reply.Err
	return nil
}

func (kv *ShardKV) doSentShard(args *SentShardArgs) SentShardReply {

}

func (kv *ShardKV) doReconfigStart(args *ReConfigStartArgs) Reply {

	config_next := kv.sm.Query(kv.config_now.Num + 1)
	kv.config_now = config_next
	//shards :=
	//config_prior :=
}

func (kv *ShardKV) doReconfigEnd(args *ReConfigEndArgs) Reply {

}

func (kv *ShardKV) ensure_updated() {
	noop := makeOp(Noop, Op{})               // requested Op
	agreement_number := kv.paxos_agree(noop) // sync call returns after agreement reached

	kv.sync(agreement_number)                    // sync call, operations up to limit performed
	kv.perform_operation(agreement_number, noop) // perform requested Op
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.ensure_updated()

	if kv.transition_to == -1 {
		config_latest := kv.sm.Query(-1)
		if config_latest.Num > kv.config_now.Num {
			operation := make_op(ReconfigStart, ReceiveShardArgs{})
			agreement_number := kv.paxos_agree(operation)
			kv.sync(agreement_number)
			kv.perform_operation(agreement_number, operation)
		}
	} else {

	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
