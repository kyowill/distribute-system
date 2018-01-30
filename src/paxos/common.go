package paxos

type PrepareArgs struct {
	Agreement_number int // number of the agreement instance
	Proposal_number  int // proposal number of the proposal
}

type PrepareReply struct {
	Prepare_ok        bool     // was prepare ok'ed?
	Number_promised   int      // promise not to accept any more proposals less than n
	Accepted_proposal Proposal // highest numbered proposal that has been accepted
}

type Proposal struct {
	Number int
	Value  interface{}
}

type AcceptArgs struct {
	Agreement_number int      // number of the agreement instance
	Proposal         Proposal // Proposal to contend to be the decided proposal
}

type AcceptReply struct {
	Accept_ok    bool // whether the accept proposal request was accepted
	Highest_done int  // sender's highest "done" argument number
}

type DecidedArgs struct {
	Agreement_number int      // number of the agreement instance
	Proposal         Proposal // Proposal containing the decided upon value
}

type DecidedReply struct {
}

type AgreementState struct {
	decided bool
	// Proposer
	proposal_number int // Proposal number propser is using currently.
	// Acceptor
	highest_promised  int      // highest proposal number promised in a prepare_ok reply
	accepted_proposal Proposal // highest numbered proposal that has been accepted
	decided_proposal  Proposal // proposal that has been decided on
}
