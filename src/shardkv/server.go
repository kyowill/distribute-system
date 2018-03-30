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
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Id   string
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
	config_prior     shardmaster.Config
	config_now       shardmaster.Config
	storage          map[string]string // key/value data storage
	cache            map[string]Reply  // "request_id" -> reply cache
	shards           []bool            // shards in charge
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

type KVPair struct {
	Key   string
	Value string
}

/*
Converts a shards array of int64 gids (such as Config.Shards) into a slice of
booleans of the same length where an entry is true if the gid of the given
shards array equals my_gid and false otherwise.
*/
func shard_state(shards [shardmaster.NShards]int64, my_gid int64) []bool {
	shard_state := make([]bool, len(shards))
	for shard_index, gid := range shards {
		if gid == my_gid {
			shard_state[shard_index] = true
		} else {
			shard_state[shard_index] = false
		}
	}
	return shard_state
}

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
		var reconfig_start_args = (operation.Args).(ReconfigStartArgs)
		result = self.doReconfigStart(&reconfig_start_args)
	case ReconfigEnd:
		var reconfig_end_args = (operation.Args).(ReconfigEndArgs)
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

// helpers
func generate_uuid() string {
	return strconv.Itoa(rand.Int())
}

func request_identifier(request_id int64) string {
	return strconv.FormatInt(request_id, 10)
}

func internal_request_identifier(transition_id int, request_id int) string {
	return "i" + strconv.Itoa(transition_id) + strconv.Itoa(request_id)
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
	operation := make_op(Get, *args)
	agreement := kv.paxos_agree(operation)
	kv.sync(agreement)
	result := kv.perform_operation(agreement, operation)
	get_reply := result.(GetReply)
	reply.Err = get_reply.Err
	reply.Value = get_reply.Value
	return nil
}

func (kv *ShardKV) doGet(args *GetArgs) GetReply {

	client_request := request_identifier(args.Id)
	val, present := kv.cache[client_request]
	if present {
		return val.(GetReply)
	}

	shard_index := key2shard(args.Key)
	var reply GetReply
	if kv.gid != kv.config_now.Shards[shard_index] {
		reply.Err = ErrWrongGroup
		reply.Value = ""
		return reply
	}

	if !kv.shards[shard_index] {
		fmt.Printf("gid=%v, shard=%v not ready \n", kv.gid, shard_index)
		reply.Err = ErrNotReady
		reply.Value = ""
		return reply
	}

	value, ok := kv.storage[args.Key]
	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}

	kv.cache[client_request] = reply

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
	operation := make_op(args.Op, *args)
	agreement := kv.paxos_agree(operation)
	kv.sync(agreement)
	result := kv.perform_operation(agreement, operation)
	put_append_reply := result.(PutAppendReply)
	reply.Err = put_append_reply.Err
	return nil
}

func (kv *ShardKV) doPut(args *PutAppendArgs) PutAppendReply {
	client_request := request_identifier(args.Id)
	value, present := kv.cache[client_request]
	if present {
		return value.(PutAppendReply)
	}

	shard_index := key2shard(args.Key)
	var reply PutAppendReply
	if kv.gid != kv.config_now.Shards[shard_index] {
		reply.Err = ErrWrongGroup
		return reply
	}

	if !kv.shards[shard_index] {
		reply.Err = ErrNotReady
		return reply
	}

	kv.storage[args.Key] = args.Value
	reply.Err = OK
	kv.cache[client_request] = reply
	return reply
}

func (kv *ShardKV) doAppend(args *PutAppendArgs) PutAppendReply {
	client_request := request_identifier(args.Id)
	val, present := kv.cache[client_request]
	if present {
		return val.(PutAppendReply)
	}
	shard_index := key2shard(args.Key)
	var reply PutAppendReply
	if kv.gid != kv.config_now.Shards[shard_index] {
		reply.Err = ErrWrongGroup
		return reply
	}

	if !kv.shards[shard_index] {
		reply.Err = ErrNotReady
		return reply
	}

	_, ok := kv.storage[args.Key]
	if ok {
		kv.storage[args.Key] += args.Value
	} else {
		kv.storage[args.Key] = args.Value
	}
	reply.Err = OK
	kv.cache[client_request] = reply
	return reply
}

func (kv *ShardKV) ReceiveShard(args *ReceiveShardArgs, reply *ReceiveShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config_now.Num == 0 {
		return nil
	}
	operation := make_op(ReceiveShard, *args)
	agreement := kv.paxos_agree(operation)
	kv.sync(agreement)
	result := kv.perform_operation(agreement, operation)
	receive_shard_reply := result.(ReceiveShardReply)
	reply.Err = receive_shard_reply.Err
	return nil
}

func (kv *ShardKV) doReceiveShard(args *ReceiveShardArgs) ReceiveShardReply {

	client_request := internal_request_identifier(args.Trans_to, args.Shard_index)
	val, present := kv.cache[client_request]
	if present {
		return val.(ReceiveShardReply)
	}

	var reply ReceiveShardReply
	if args.Trans_to > kv.transition_to {
		reply.Err = ErrNotReady
		//return reply
	} else if args.Trans_to < kv.transition_to {
		//fmt.Printf("sender trans=%v, receiver trans=%v \n", args.Trans_to, kv.transition_to)
		reply.Err = OK
		kv.cache[client_request] = reply
		//return reply
	} else {

		for _, pair := range args.Kvpairs {
			kv.storage[pair.Key] = pair.Value
		}
		kv.shards[args.Shard_index] = true
		reply.Err = OK
		kv.cache[client_request] = reply
	}
	//fmt.Printf("receive shard index=%v, reply=%v, sender_trains=%v, self_trains=%v \n", args.Shard_index, reply, args.Trans_to, kv.transition_to)
	return reply
}

func (kv *ShardKV) doSentShard(args *SentShardArgs) SentShardReply {
	if kv.transition_to == args.Trans_to {
		kv.remove_shard(args.Trans_to)
	}
	return SentShardReply{}
}

func (kv *ShardKV) remove_shard(shard_index int) {
	for key, _ := range kv.storage {
		if key2shard(key) == shard_index {
			delete(kv.storage, key)
		}
	}
	kv.shards[shard_index] = false
}

func (kv *ShardKV) doReconfigStart(args *ReconfigStartArgs) Reply {

	if kv.transition_to == kv.config_now.Num {
		return nil
	}
	config_next := kv.sm.Query(kv.config_now.Num + 1)
	fmt.Printf("gid =%v, before shard 2 state=%v, transition=%v \n", kv.gid, kv.shards[2], kv.transition_to)
	kv.config_prior = kv.config_now
	kv.config_now = config_next
	kv.transition_to = kv.config_now.Num
	fmt.Printf("gid =%v, after shard 2 state=%v, transition=%v \n", kv.gid, kv.shards[2], kv.transition_to)
	kv.shards = shard_state(kv.config_prior.Shards, kv.gid)
	//fmt.Printf("after shard 2 state=%v, transition=%v \n", kv.shards[2], kv.transition_to)
	return nil
}

func (kv *ShardKV) doReconfigEnd(args *ReconfigEndArgs) Reply {
	if kv.transition_to == -1 {
		return nil
	}
	kv.transition_to = -1 // no longer in transition to a new config
	return nil
}

func (kv *ShardKV) ensure_updated() {
	noop := make_op(Noop, Op{})              // requested Op
	agreement_number := kv.paxos_agree(noop) // sync call returns after agreement reached

	kv.sync(agreement_number)                    // sync call, operations up to limit performed
	kv.perform_operation(agreement_number, noop) // perform requested Op
}

func (kv *ShardKV) broadcast_shards() {
	for shard_index, gid := range kv.config_now.Shards {
		if shard_index == 2 {
			fmt.Printf("shard=%v, group=%v, old group=%v \n", kv.shards[shard_index], gid, kv.gid)
		}
		if (kv.shards[shard_index]) && (gid != kv.gid) {
			//fmt.Printf("shard=%v, group=%v \n", shard_index, gid)
			kv.send_shard(shard_index, gid)
		}
	}
}

func (kv *ShardKV) send_shard(shard_index int, gid int64) {
	var kvpairs []KVPair
	for key, value := range kv.storage {
		if key2shard(key) == shard_index {
			kvpairs = append(kvpairs, KVPair{Key: key, Value: value})
		}
	}

	servers := kv.config_now.Groups[gid]
	for _, srv := range servers {
		args := &ReceiveShardArgs{}
		args.Kvpairs = kvpairs
		args.Shard_index = shard_index
		args.Trans_to = kv.transition_to
		var reply ReceiveShardReply
		ok := call(srv, "ShardKV.ReceiveShard", args, &reply)
		//fmt.Printf("server=%v, shard=%v, reply=%v \n", srv, args.Shard_index, reply)
		if ok && (reply.Err == OK) {
			sent_shard_args := SentShardArgs{}
			sent_shard_args.Shard_index = shard_index
			sent_shard_args.Trans_to = kv.transition_to
			operation := make_op(SentShard, sent_shard_args)
			agreement_number := kv.paxos_agree(operation)
			kv.sync(agreement_number)
			kv.perform_operation(agreement_number, operation)
			//fmt.Printf("server = %v is ok \n", srv)
			return
		}
	}
}

func (kv *ShardKV) done_sending_shards() bool {
	goal_shards := shard_state(kv.config_now.Shards, kv.gid)
	for shard_index, _ := range kv.shards {
		if kv.shards[shard_index] == true && goal_shards[shard_index] == false {
			// still at least one send has not been acked
			return false
		}
	}
	return true
}

func (self *ShardKV) done_receiving_shards() bool {
	goal_shards := shard_state(self.config_now.Shards, self.gid)
	for shard_index, _ := range self.shards {
		if self.shards[shard_index] == false && goal_shards[shard_index] == true {
			// still at least one send has not been received
			return false
		}
	}
	return true
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
		if kv.config_now.Num == 0 {
			// conf := kv.sm.Query(0)
			config := kv.sm.Query(1)
			if config.Num == 1 {
				kv.config_prior = kv.config_now
				kv.config_now = config
				// No shard transfers needed. Automatically have shards of first valid Config.
				kv.shards = shard_state(kv.config_now.Shards, kv.gid)
				// debug(fmt.Sprintf("(svr:%d,rg:%d) InitialConfig: %+v, %+v", self.me, self.gid, self.config_now, self.shards))
				return
			}
			// No Join has been performed yet. ShardMaster still has initial Config
			return
		}
		config_latest := kv.sm.Query(-1)
		//fmt.Printf("gid =%v, config latest =%v \n", kv.gid, config_latest)
		if config_latest.Num > kv.config_now.Num {
			operation := make_op(ReconfigStart, ReconfigStartArgs{})
			agreement_number := kv.paxos_agree(operation)
			kv.sync(agreement_number)
			kv.perform_operation(agreement_number, operation)
		}
	} else {
		kv.broadcast_shards()
		if kv.done_sending_shards() && kv.done_receiving_shards() {
			operation := make_op(ReconfigEnd, ReconfigEndArgs{})
			agreement_number := kv.paxos_agree(operation)
			kv.sync(agreement_number)
			kv.perform_operation(agreement_number, operation)
			//fmt.Println("reconfig end")
		}
	}
	return
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
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register(ReconfigStartArgs{})
	gob.Register(ReconfigEndArgs{})
	gob.Register(ReceiveShardArgs{})
	gob.Register(SentShardArgs{})
	//gob.Register(NoopArgs{})
	gob.Register(KVPair{})
	kv.config_prior = shardmaster.Config{}        // initial prior Config
	kv.config_prior.Groups = map[int64][]string{} // initialize map
	kv.config_now = shardmaster.Config{}          // initial prior Config
	kv.config_now.Groups = map[int64][]string{}   // initialize map
	kv.shards = make([]bool, shardmaster.NShards)
	kv.transition_to = -1
	kv.storage = map[string]string{} // key/value data storage
	kv.cache = map[string]Reply{}    // "client_id:request_id" -> reply cache
	kv.operation_number = -1         // first agreement number will be 0

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
