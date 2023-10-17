package kvraft

import (
	"crypto/rand"
	"math/big"
	randM "math/rand"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	rpcSeq   int
	leadId   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.rpcSeq = 0
	ck.leadId = -1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	arg := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		RpcSeq:   ck.rpcSeq,
	}
	server := randM.Intn(len(ck.servers))
	if ck.leadId != -1 {
		server = ck.leadId
	}
	DPrintf("CK%d Get key %s", ck.clientId, key)
	for {
		reply := GetReply{}
		// DPrintf("Get server id %d", server)
		ok := ck.servers[server].Call("KVServer.Get", &arg, &reply)
		if ok {
			switch reply.Err {
			case ErrResend:
				server = (server + 1) % len(ck.servers)
			case ErrWrongLeader:
				server = (server + 1) % len(ck.servers)
			case ErrServerKilled:
				server = (server + 1) % len(ck.servers)
			case ErrNoKey:
				ck.rpcSeq++
				ck.leadId = server
				return ""
			case OK:
				ck.rpcSeq++
				ck.leadId = server
				return reply.Value
			case ErrDuplicate:
				ck.rpcSeq++
				ck.leadId = server
				return reply.Value
			default:
				DPrintf("Get unknown replyErr: %s", reply.Err)
			}
		} else {
			server = (server + 1) % len(ck.servers)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	arg := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		RpcSeq:   ck.rpcSeq,
	}
	server := randM.Intn(len(ck.servers))
	if ck.leadId != -1 {
		server = ck.leadId
	}
	DPrintf("CK%d %s kv (%s, %s)", ck.clientId, op, key, value)
	for {
		reply := PutAppendReply{}

		ok := ck.servers[server].Call("KVServer.PutAppend", &arg, &reply)
		DPrintf("CK%d %s kv (%s, %s) call backed,is OK? %t", ck.clientId, op, key, value, ok)
		// DPrintf("PutAppend server id is %d, the reply ok %t, the err is %v", server, ok, reply.Err)
		if ok {
			// DPrintf("the reply err %s", reply.Err)
			switch reply.Err {
			case ErrResend:
				server = (server + 1) % len(ck.servers)
			case OK:
				ck.rpcSeq++
				ck.leadId = server
				// DPrintf("the putappend rpcseq %d", ck.rpcSeq)
				return
			case ErrServerKilled:
				server = (server + 1) % len(ck.servers)
			case ErrDuplicate:
				ck.rpcSeq++
				ck.leadId = server
				// DPrintf("the putappend rpcseq %d", ck.rpcSeq)
				return
			case ErrWrongLeader:
				server = (server + 1) % len(ck.servers)
			default:
				DPrintf("Put unknown replyErr: %s", reply.Err)
			}
		} else {
			server = (server + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
