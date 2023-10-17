package kvraft

const (
	AGREEDTIMEOUT int = 3000
)

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrServerKilled = "ErrServerKilled"
	ErrDuplicate    = "ErrDuplicate"
	ErrResend       = "ErrResend"
)
const (
	GET    = " Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	RpcSeq   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	RpcSeq   int
}

type GetReply struct {
	Err   Err
	Value string
}
