package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]
	// Your data here.
	peer_count int                     // number of peers
	state      map[int]*AgreementState // Key: Agreement instance number -> Value: Agreement State
	done       map[string]int          // Key: Server name, Value: The most recently received value for that server's highest done value.
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	_, present := px.state[seq]
	if !present {
		px.state[seq] = px.make_default_agreementstate()
	}

	go px.proposer_role(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return 0
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.peer_count = len(peers)
	px.state = map[int]*AgreementState{}
	px.done = map[string]int{}
	for _, peer := range px.peers {
		px.done[peer] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

func (px *Paxos) proposer_role(agreement_number int, proposal_value interface{}) {
	var proposal_number = -1
	for px.still_deciding(agreement_number) && (!px.isdead()) {
		proposal_number = px.next_proposal_number(agreement_number)
		proposal := Proposal{Number: proposal_number, Value: proposal_value}
		replies_in_prepare := px.broadcast_prepare(agreement_number, proposal)

	}
}

func (px *Paxos) Prepare_handler(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	var agreement_number = args.Agreement_number
	var proposal_number = args.Proposal_number

	_, present := px.state[agreement_number]
	if !present {
		px.state[agreement_number] = px.make_default_agreementstate()
	}

	if proposal_number > px.state[agreement_number].highest_promised {
		px.state[agreement_number].highest_promised = proposal_number
		reply.Prepare_ok = true
		reply.Number_promised = proposal_number
		reply.Accepted_proposal = px.state[agreement_number].accepted_proposal
	} else {
		reply.Prepare_ok = false
		reply.Number_promised = px.state[agreement_number].highest_promised
	}
	return nil
}

func (px *Paxos) Accept_handler(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	var agreement_number = args.Agreement_number
	var proposal = args.Proposal

	_, present := px.state[agreement_number]

	if !present {
		px.state[agreement_number] = px.make_default_agreementstate()
	}

	if proposal.Number >= px.state[agreement_number].highest_promised {
		reply.Accept_ok = true
		reply.Highest_done = px.done[px.peers[px.me]]
		px.state[agreement_number].accepted_proposal = proposal
	} else {
		reply.Accept_ok = false
		reply.Highest_done = px.done[px.peers[px.me]]
	}
	return nil
}

func (px *Paxos) Decide_handler(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	var agreement_number = args.Agreement_number
	var proposal = args.Proposal

	_, present := px.state[agreement_number]

	if !present {
		px.state[agreement_number] = px.make_default_agreementstate()
	}
	px.state[agreement_number].decided_proposal = proposal
	px.state[agreement_number].accepted_proposal = proposal
	px.state[agreement_number].decided = true
	return nil
}

func (px *Paxos) broadcast_prepare(agreement_number int, proposal Proposal) []PrepareReply {

	var replies_in_prepare []PrepareReply = make([]PrepareReply, px.peer_count)
	var reply PrepareReply
	args := &PrepareArgs{}
	args.Agreement_number = agreement_number
	args.Proposal_number = proposal.Number
	for index, peer := range px.peers {
		ok := call(peer, "Paxos.Prepare_handler", args, &reply)
		if ok {
			replies_in_prepare[index] = reply
		} else {
			replies_in_prepare[index] = PrepareReply{Prepare_ok: false, Number_promised: -1, Accepted_proposal: Proposal{Number: -1}}
		}
	}
	return replies_in_prepare
}

func (px *Paxos) broadcast_accept(agreement_number int, proposal Proposal) []AcceptReply {
	var replies_in_accept []AcceptReply = make([]AcceptReply, px.peer_count)
	var reply AcceptReply
	args := &AcceptArgs{}
	args.Agreement_number = agreement_number
	args.Proposal = proposal
	for index, peer := range px.peers {
		ok := call(peer, "Paxos.Accept_handler", args, &reply)
		if ok {
			replies_in_accept[index] = reply
		} else {
			replies_in_accept[index] = AcceptReply{Accept_ok: false, Highest_done: -1}
		}
	}
	return replies_in_accept
}

func (px *Paxos) broadcast_decide(agreement_number int, proposal Proposal) {
	args := &DecidedArgs{}
	args.Agreement_number = agreement_number
	args.Proposal = proposal
	for index, peer := range px.peers {
		call(peer, "Paxos.Decide_handler", args, nil)
	}
}

func (px *Paxos) evaluate_prepare_replies(replies []PrepareReply) (bool, int, Proposal) {
	var ok_count = 0
	var highest_proposal_number = -1
	var proposal = Proposal{Number: -1}
	for _, reply := range replies {

		if !reply.Prepare_ok {
			continue
		}

		if reply.Accepted_proposal.Number > highest_proposal_number {

		}

		ok_count += 1

	}
}

/*
Returns a reference to an AgreementState instance initialized with the correct default
values so that it is ready to be used in the px.state map.
The highest seen and highest promised are set to -1 and the proposal number is set to
the px.me index minux the number of peers since each time next_proposal_number is
called, the number is incremented by the number of peers.
*/
func (px *Paxos) make_default_agreementstate() *AgreementState {
	initial_proposal_number := px.me
	agrst := AgreementState{highest_promised: -1,
		decided:           false,
		proposal_number:   initial_proposal_number,
		accepted_proposal: Proposal{Number: -1},
	}
	return &agrst
}

/*
Computes the minimum agreement number marked as done among all the done agreement
numbers reported back by peers and represented in the px.peers map.
Callee is responsible for taking out a lock on the paxos instance.
*/
func (px *Paxos) minimum_done_number() int {
	var min_done_number = px.done[px.peers[px.me]]
	for _, peers_done_number := range px.done {
		if peers_done_number < min_done_number {
			min_done_number = peers_done_number
		}
	}
	return min_done_number
}

func (px *Paxos) next_proposal_number(agreement_number int) int {
	px.mu.Lock()
	defer px.mu.Unlock()

	var proposal_number = px.state[agreement_number].proposal_number + px.peer_count
	px.state[agreement_number].proposal_number = proposal_number
	return proposal_number
}

func (px *Paxos) still_deciding(agreement_number int) bool {
	px.mu.Lock()
	defer px.mu.Unlock()

	_, present := px.state[agreement_number]

	if !present {
		px.state[agreement_number] = px.make_default_agreementstate()
	}

	if px.state[agreement_number].decided {
		return false
	} else {
		return true
	}
}

func (px *Paxos) is_majority(ok_count int) bool {
	if ok_count > (px.peer_count / 2) {
		return true
	}
	return false
}
