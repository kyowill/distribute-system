package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view	   viewservice.View
	data       map[string]string
	client	   map[string]int64
}


func (pb *PBServer) isPrimary() bool {
	return pb.view.Primary == pb.me
}

func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {
	return pb.PutAppend(args.PutAppendInput, reply.PutAppendOutput)
}

func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.data = args.Data
	pb.client = args.Client
	reply.Err = OK
	return nil
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	vx, ok := pb.vs.Get()
	if ok == false || vx.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}
	v := pb.data[args.Key]
	reply.Value = v
	if v != "" {
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if args.Op == "Put" {
		pb.data[args.Key] = args.Value
		reply.Err = OK
	}else if args.Op == "Append" {
		if pb.client[args.Name] == args.UUid {
			// do nothing
		} else {
			pb.client[args.Name] = args.UUid
			pb.data[args.Key] += args.Value
		}
		reply.Err = OK
	}

	// forward request to backup
	if pb.isPrimary() {
		pb.TransferRpc(pb.view.Backup)
	}
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	vx, err := pb.vs.Ping(pb.view.Viewnum)
	if err == nil {
		var oldBackup = pb.view.Backup
		pb.view = vx
		if vx.Backup != oldBackup && pb.isPrimary() {
			pb.TransferRpc(vx.Backup)
		}		
	}else {
		pb.view = vx
	} 
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.data = make(map[string]string)
	pb.client = make(map[string]int64)
	pb.mu = sync.Mutex{}

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}

func (pb *PBServer) TransferRpc(name string) bool{
	
	args := &TransferArgs{}
	args.Data = pb.data
	args.Client = pb.client
	var reply TransferReply
	ok := call(name, "PBServer.Transfer", args, &reply)
	return ok
}

func (pb *PBServer) ForwardRpc(key string, value string, op string) {
	// Your code here
	if pb.view.Backup == ""{
		return
	}

	args := &PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Name = pb.me
	args.UUid = nrand()
	var reply PutAppendReply
	ok := false


	for ok == false {
		ok = call(pb.view.Backup, "PBServer.Forward", args, &reply)
		if ok == false {
			vx, _:= pb.vs.Get()
			pb.view = vx
			if vx.Backup == "" {
				break
			}
			// when client1 send k-v before client2, 
			// Server maybe receive client2 first, so sleep a while
			time.Sleep(viewservice.PingInterval) 
		}
	}
}