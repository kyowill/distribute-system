package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
	Noop  = "Noop"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs          []Config // indexed by config num
	operation_number int      // agreement number of latest applied operation
}

type Op struct {
	// Your data here.
	Id   string
	Name string
	Args interface{}
}

func generate_uuid() string {
	return strconv.Itoa(rand.Int())
}

func make_op(name string, args interface{}) Op {
	operation := Op{Id: generate_uuid(), Name: name, Args: args}
	return operation
}

func (self *ShardMaster) await_paxos_decision(agreement_number int) (decided_val interface{}) {
	sleep_max := 10 * time.Second
	sleep_time := 10 * time.Millisecond
	for {
		has_decided, decided_val := self.px.Status(agreement_number)
		if has_decided {
			return decided_val
		}
		time.Sleep(sleep_time)
		if sleep_time < sleep_max {
			sleep_time *= 2
		}
	}
	panic("unreachable")
}

func (self *ShardMaster) paxos_agree(operation Op) int {
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

func (self *ShardMaster) paxos_staus(agreement int) (bool, Op) {
	fate, val := self.px.Status(agreement)
	if fate == paxos.Decided && val != nil {
		operation := val.(Op)
		return true, operation
	}
	return false, Op{}
}

func (self *ShardMaster) available_agreement_number() int {
	return self.px.Max() + 1
}

func (self *ShardMaster) last_operation_number() int {
	return self.operation_number
}

func (self *ShardMaster) sync(limit int) {
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

func (self *ShardMaster) perform_operation(agreement int, operation Op) interface{} {
	switch operation.Name {
	case Join:
		var join_args = (operation.Args).(JoinArgs) // type assertion, Args is a JoinArgs
		result = self.join(&join_args)
	case Leave:
		var leave_args = (operation.Args).(LeaveArgs) // type assertion, Args is a LeaveArgs
		result = self.leave(&leave_args)
	case Move:
		var move_args = (operation.Args).(MoveArgs) // type assertion, Args is a MoveArgs
		result = self.move(&move_args)
	case Query:
		var query_args = (operation.Args).(QueryArgs) // type assertion, Args is a QueryArgs
		result = self.query(&query_args)
	case Noop:
		//
	default:
		panic(fmt.Printf("do nothing ... \n"))
	}
	self.operation_number = agreement
	self.px.Done(agreement)
	return result
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	operation := make_op(Join, args)
	agreement := sm.paxos_agree(operation)
	sm.sync(agreement)
	sm.perform_operation(agreement, operation)
	return nil
}

func (self *ShardMaster) join(args *JoinArgs) JoinReply {
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	operation := make_op(Leave, args)
	agreement := sm.paxos_agree(operation)
	sm.sync(agreement)
	sm.perform_operation(agreement, operation)
	return nil
}

func (self *ShardMaster) leave(args *LeaveArgs) LeaveReply {
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	operation := make_op(Move, args)
	agreement := sm.paxos_agree(operation)
	sm.sync(agreement)
	sm.perform_operation(agreement, operation)
	return nil
}

func (self *ShardMaster) move(args *MoveArgs) MoveReply {

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	operation := make_op(Query, args)
	agreement := sm.paxos_agree(operation)
	sm.sync(agreement)
	result := sm.perform_operation(agreement, operation)
	val, ok := result.(QueryReply)
	if ok {
		reply = val
	}
	return nil
}

func (self *ShardMaster) query(args *QueryArgs) QueryReply {
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
