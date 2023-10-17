package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string
	Key      string
	Value    string
	ClientId int64
	RpcSeq   int
}
type ReplyContent struct {
	RpcSeq int
	Value  string
}
type Result struct {
	Err
	Value string
}
type NotifyMsg struct {
	ch          chan Result
	currentTerm int
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db             map[string]string
	duplicateTable map[int64]ReplyContent
	notifyChans2   map[int]NotifyMsg

	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// DPrintf("Get %v", args)
	// if v, isDuplicate := kv.checkDuplicate(args.ClientId, args.RpcSeq); isDuplicate {
	// 	reply.Err = OK
	// 	reply.Value = v.Value
	// 	return
	// }
	if kv.killed() {
		reply.Err = ErrServerKilled
		return
	}
	op := Op{
		OpType:   GET,
		Key:      args.Key,
		ClientId: args.ClientId,
		RpcSeq:   args.RpcSeq,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	/*
		ch := kv.getChan(index)
		DPrintf("Leader%d the GET op key (%s) at index %d, term %d", kv.rf.GetMe(), args.Key, index, term)
		select {
		case res := <-ch:
			reply.Err = res.Err
			reply.Value = res.Value
		case <-time.After(time.Duration(AGREEDTIMEOUT) * time.Millisecond):
			reply.Err = ErrWrongLeader
		}
		kv.deleteChan(index) // ???
	*/
	DPrintf("Get %v begin at term %d, index %d", args, term, index)
	notify := kv.getChan2(index, term)
	select {
	case res := <-notify.ch:
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(time.Duration(AGREEDTIMEOUT) * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	kv.deleteChan2(index)
	DPrintf("Get %v end", args)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// DPrintf("%v", args)
	// if _, isDuplicate := kv.checkDuplicate(args.ClientId, args.RpcSeq); isDuplicate {
	// 	return
	// }
	if kv.killed() {
		reply.Err = ErrServerKilled
		return
	}
	op := Op{
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		RpcSeq:   args.RpcSeq,
	}
	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("PutAppend %v begin at term %d, index %d", args, term, index)
	// DPrintf("the append index is %d", index)
	/*
		ch := kv.getChan(index)
		DPrintf("Leader%d the APPEND op kv(%s, %s) at index %d, term %d", kv.rf.GetMe(), args.Key, args.Value, index, term)
		select {
		case res := <-ch:
			reply.Err = res.Err
		case <-time.After(time.Duration(AGREEDTIMEOUT) * time.Millisecond):
			// 超时可能代表server分区
			reply.Err = ErrWrongLeader
		}
		kv.deleteChan(index) // 删除由于分区中old term leader添加的日志位置，防止new term leader在相同index添加不同的request，造成两个goroutine读阻塞
	*/
	notify := kv.getChan2(index, term)
	select {
	case res := <-notify.ch:
		reply.Err = res.Err
	case <-time.After(time.Duration(AGREEDTIMEOUT) * time.Millisecond):
		// 超时可能代表leader crash或者commit超时
		reply.Err = ErrWrongLeader
	}
	kv.deleteChan2(index)
	DPrintf("PutAppend %v end", args)
}

func (kv *KVServer) checkDuplicate(clientId int64, rpcSeq int) (ReplyContent, bool) {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	v, exist := kv.duplicateTable[clientId]
	// DPrintf("the op seq %d, the duplicate seq %d", rpcSeq, v.RpcSeq)
	if !exist || v.RpcSeq < rpcSeq {
		return v, false
	}
	return v, true
}
func (kv *KVServer) deleteChan2(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exist := kv.notifyChans2[index]
	if exist {
		delete(kv.notifyChans2, index)
		DPrintf("Server%d delete the %d chan", kv.rf.GetMe(), index)
	}
}
func (kv *KVServer) getChan2(index, term int) NotifyMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Server%d get the %d index chan at %d term", kv.rf.GetMe(), index, term)
	notifyMsg, exist := kv.notifyChans2[index]
	if !exist {
		notifyMsg = NotifyMsg{
			currentTerm: term,
			ch:          make(chan Result),
		}

	}
	kv.notifyChans2[index] = notifyMsg
	return notifyMsg
}
func (kv *KVServer) getNotifyMsg(index, term int) (NotifyMsg, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	notifyMsg, exist := kv.notifyChans2[index]
	DPrintf("Server%d getNotifyMsg at %d index %d term", kv.rf.GetMe(), index, term)
	if exist && notifyMsg.currentTerm != term {
		res := Result{
			Err: ErrResend,
		}
		notifyMsg.ch <- res
		return notifyMsg, false
	}
	DPrintf("the don't exist notifyMsg %v", notifyMsg)
	return notifyMsg, true
}
func (kv *KVServer) persist(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.duplicateTable)
	e.Encode(kv.db)

	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}
func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var duplicateTable map[int64]ReplyContent
	if d.Decode(&duplicateTable) != nil || d.Decode(&db) != nil {
		DPrintf("Decode failed")
	}
	kv.db = db
	kv.duplicateTable = duplicateTable
}
func (kv *KVServer) apply(op Op) Result {
	// DPrintf("before the %s op kv (%s, %s)", op.OpType, op.Key, op.Value)
	res := Result{
		Err: OK,
	}
	DPrintf("Server%d apply the %s op kv(%s, %s)", kv.rf.GetMe(), op.OpType, op.Key, op.Value)
	if _, isDuplidate := kv.checkDuplicate(op.ClientId, op.RpcSeq); isDuplidate {
		res.Err = ErrDuplicate
		res.Value = kv.duplicateTable[op.ClientId].Value
		// DPrintf("%t, %s", isDuplidate, ErrDuplicatePutAppend)
		DPrintf("Duplicate %s, key %s, value %s", op.OpType, op.Key, op.Value)
		return res
	}
	value, exist := kv.db[op.Key]
	// DPrintf("the %s op kv (%s, %s)", op.OpType, op.Key, op.Value)
	switch op.OpType {
	case GET:
		res.Value = value
		if !exist {
			res.Err = ErrNoKey
		}
	case PUT:
		kv.db[op.Key] = op.Value
	case APPEND:
		value += op.Value
		kv.db[op.Key] = value
	}
	// DPrintf("the kv (%s, %s)", op.Key, kv.db[op.Key])
	kv.duplicateTable[op.ClientId] = ReplyContent{
		RpcSeq: op.RpcSeq,
		Value:  res.Value,
	}
	return res
}

/*
	1.client提交给leader，获得index，但是该leader在提交该log之前，崩溃，失去leadership
	如何发现失去leadership?
	2.发生分区，server无限期等待分区愈合
*/
func (kv *KVServer) ApplyMsgs2() {
	for !kv.killed() {
		msg := <-kv.applyCh
		currentTerm, isleader := kv.rf.GetState()
		if msg.CommandValid {
			op := msg.Command.(Op)
			//DPrintf("APPLY OP %v", op)
			res := kv.apply(op)
			DPrintf("Server%d get the apply msg %v", kv.rf.GetMe(), msg)

			// 如果log bytes size > maxraftstate
			if res.Err != ErrDuplicate && kv.maxraftstate > -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.persist(msg.CommandIndex)
			}
			// DPrintf("the apply res %v", res)
			// 不是leader不需要通知其他goroutine
			// 如果不是自己term中添加的log，只需apply
			//kv.mu.Lock()
			// DPrintf("notify the index %d", msg.CommandIndex)
			// DPrintf("Leader apply res: %v, index is %d", res, msg.CommandIndex)

			notifyMsg, checkLeaderShip := kv.getNotifyMsg(msg.CommandIndex, currentTerm)
			//kv.mu.Unlock()
			if isleader && checkLeaderShip {
				if notifyMsg.ch == nil {
					// 当选为leader之前的log，notifyChans[index] == nil
					DPrintf("apply the previous term %d", msg.CommandIndex)
					continue
				}
				DPrintf("leader%d the index %d ch is %v", kv.rf.GetMe(), msg.CommandIndex, notifyMsg.ch)
				notifyMsg.ch <- res
			}
		} else {
			// 应用快照
			kv.readPersist(msg.Snapshot)
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 初始化db
	kv.db = make(map[string]string)
	kv.duplicateTable = make(map[int64]ReplyContent)
	kv.notifyChans2 = make(map[int]NotifyMsg)

	kv.persister = persister

	kv.readPersist(persister.ReadSnapshot())

	go kv.ApplyMsgs2()
	// You may need initialization code here.

	return kv
}
