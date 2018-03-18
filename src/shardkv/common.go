package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
}

type GetReply struct {
	Err   Err
	Value string
}

type ReceiveShardReply struct {
	Err Err
}

type ReceiveShardArgs struct {
	Kvpairs     []KVPair // slice of Key/Value pairs
	Trans_to    int      // config number the sender is transitioning to
	Shard_index int      // index of shard being sent
}

type SentShardArgs struct {
	Trans_to    int // config number the sender is transitioning to
	Shard_index int // index of shard that was successfully sent
}

type SentShardReply struct {
	Err Err
}
