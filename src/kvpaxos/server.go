package kvpaxos

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


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpID  int64
	Op    string
	Key   string
	Value string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	kvstore          map[string]string     // store kv pair
	filters          map[int64]bool        // to filter duplicates
	replies          map[int64]interface{} // history replies (key:OpID)
	//agreement_number int64                 // paxos instance last agreement number
}

const TickInterval = 100 * time.Millisecond

func (kv *KVPaxos) Lock() {
	kv.mu.Lock()
}

func (kv *KVPaxos) Unlock() {
	kv.mu.Unlock()
}

func (kv *KVPaxos) visit_db(op *Op) {
	if op.Op == "get" {
		val, ok := kv.kvstore[op.Key]
		if !ok {
			op.Value = ""
			kv.replies[op.OpID].(*GetReply).Value = ""
			kv.replies[op.OpID].(*GetReply).Err = ErrNoKey
		}else{
			op.Value = val
			kv.replies[op.OpID].(*GetReply).Value = val
			kv.replies[op.OpID].(*GetReply).Err = OK
		}
		return
	}

	if op.Op == "put" {
		kv.kvstore[op.Key] = op.Value
		kv.replies[args.OpID].(*PutAppendReply).Err = OK
		return
	}

	if op.Op == "append" {
		val, ok := kv.kvstore[op.Key]
		if !ok {
			kv.kvstore[op.Key] = op.Value
		}else{
			kv.kvstore[op.Key] += op.Value
		}
		kv.replies[args.OpID].(*PutAppendReply).Err = OK
		return
	}
}

func (kv *KVPaxos) sync(op *Op) {
	agreement_number := kv.px.Max() + 1
	kv.px.Start(agreement_number, *op)
	for {
		fate, value := kv.px.Status(agreement_number)
		if fate == Decided {
			// update or look up kvstore
			kv.visit_db(&value.(Op))
		}else{
			time.Sleep(TickInterval)
		}

	}
	kv.px.Done(agreement_number)
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.Lock()
	defer kv.Unlock()

	_, ok := kv.filters[args.OpID]
	if ok {
		reply.Value = kv.replies[args.OpID].(*GetReply).Value
		reply.Err = kv.replies[args.OpID].(*GetReply).Err
		return nil
	}

	op := &Op{OpID: args.OpID, Op: "get", Key: args.Key, Value: ""}

	kv.sync(op)

	reply.Value = kv.replies[args.OpID].(*GetReply).Value
	reply.Err = kv.replies[args.OpID].(*GetReply).Err

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.Lock()
	defer kv.Unlock()

	_, ok := kv.filters[args.OpID]
	if ok {
		reply.Err = kv.replies[args.OpID].(*PutAppendReply).Err
		return nil
	}

	op := &Op{OpID: args.OpID, Op: args.Op, Key: args.Key, Value: args.Value}

	kv.sync(op)

	reply.Err = kv.replies[args.OpID].(*PutAppendReply).Err
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.filters = make(map[int64]bool)
	kv.kvstore = make(map[string]string)
	kv.replies = make(map[int64]interface{})

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
