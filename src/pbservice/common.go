package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op    string
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name  string
	UUid  int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.

type ForwardArgs struct {
	PutAppendInput *PutAppendArgs 
}

type ForwardReply struct {
	PutAppendOutput *PutAppendReply
}

type TransferArgs struct {
	Data 	map[string]string
	Client 	map[string]int64
}

type TransferReply struct {
	Err Err
}