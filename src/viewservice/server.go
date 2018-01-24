package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	view     View 
	live     int32
	lastPingNano map[string]int64
	lastPingNum map[string]uint
	volunteer string  
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	if vs.live == 0 {
		if vs.lastPingNum[args.Me] == 0 && args.Viewnum == 0 && vs.view.Viewnum != 0 {
			// do nothing		

		} else {
			vs.view.Viewnum += 1
			vs.view.Primary = args.Me
			vs.live = 1  
		} 
		reply.View = vs.view
		vs.lastPingNano[args.Me] = time.Now().UnixNano() / int64(time.Millisecond)
		vs.lastPingNum[args.Me] = args.Viewnum
	}else if vs.live == 1 && vs.view.Primary != args.Me {
		vs.view.Viewnum += 1 
		vs.view.Backup = args.Me
		reply.View = vs.view
		vs.live = 2
		vs.lastPingNano[args.Me] = time.Now().UnixNano() / int64(time.Millisecond)
		vs.lastPingNum[args.Me] = args.Viewnum
	}else if vs.live == 1 && vs.view.Primary == args.Me {
		vs.lastPingNano[args.Me] = time.Now().UnixNano() / int64(time.Millisecond)
		vs.lastPingNum[args.Me] = args.Viewnum
		reply.View = vs.view
	}else if vs.view.Primary == args.Me {
		vs.lastPingNano[vs.view.Primary] = time.Now().UnixNano() / int64(time.Millisecond)
		vs.lastPingNum[args.Me] = args.Viewnum
		if args.Viewnum == 0 {
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = args.Me
			vs.view.Viewnum += 1
		}
		reply.View = vs.view
	}else if vs.view.Backup == args.Me {
		vs.lastPingNano[vs.view.Backup] = time.Now().UnixNano() / int64(time.Millisecond)
		vs.lastPingNum[args.Me] = args.Viewnum
		reply.View = vs.view
	}else{
		vs.volunteer = args.Me
		vs.lastPingNano[vs.volunteer] = time.Now().UnixNano() / int64(time.Millisecond)
		vs.lastPingNum[args.Me] = args.Viewnum
		reply.View = vs.view
	}
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.view
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	// fmt.Printf("viewserver tick...\n")
	vs.mu.Lock()
	now := time.Now().UnixNano() / int64(time.Millisecond)
	maxInterval := int64(100 * DeadPings)
	primarylastPingNanoLeft := now - vs.lastPingNano[vs.view.Primary]
	backuplastPingNanoLeft := now - vs.lastPingNano[vs.view.Backup]

	if vs.live == 2 && primarylastPingNanoLeft >= maxInterval && 
	backuplastPingNanoLeft >= maxInterval {			
		// delete(vs.lastPingNano, vs.view.Primary)
		// delete(vs.lastPingNano, vs.view.Backup)

		vs.view.Primary = ""
		vs.view.Backup = ""
		vs.live = 0
	}else if vs.live == 2 && primarylastPingNanoLeft >= maxInterval && 
	backuplastPingNanoLeft < maxInterval {
		// delete(vs.lastPingNano, vs.view.Primary)
		primary	:= vs.view.Primary
		if vs.lastPingNum[primary] == vs.view.Viewnum {
			vs.view.Primary = vs.view.Backup
			if vs.volunteer != "" && vs.volunteer != vs.view.Backup &&
			(now - vs.lastPingNano[vs.volunteer]) < maxInterval {
				vs.view.Backup = vs.volunteer
				vs.volunteer = ""
				vs.live = 2
				vs.view.Viewnum += 1
			} else {
				// delete(vs.lastPingNano, vs.volunteer)

				vs.view.Backup = ""
				vs.view.Viewnum += 1
				vs.live = 1
			}			
		}

	}else if vs.live == 2 && primarylastPingNanoLeft < maxInterval && 
	backuplastPingNanoLeft >= maxInterval {
		// delete(vs.lastPingNano, vs.view.Backup)

		vs.view.Backup = ""
		vs.view.Viewnum += 1
		if vs.volunteer != "" {
			vs.view.Backup = vs.volunteer
		}else {
			vs.live = 1
		}	
	}else if vs.live == 1 && primarylastPingNanoLeft >= maxInterval {
		if vs.view.Backup == "" {
			// delete(vs.lastPingNano, vs.view.Primary)
			vs.view.Primary = ""
			vs.live = 0
		} else {
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = ""
		}
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.live = 0
	vs.mu = sync.Mutex{}
	vs.rpccount = 0
	vs.view = View{0, "", ""}
	vs.lastPingNano = make(map[string]int64)
	vs.lastPingNum = make(map[string]uint)
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
