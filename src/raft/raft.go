package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	FOLLOWER int = iota
	LEADER
	CANDIDATE
)
const (
	TICK_INTERVAL          int     = 10
	HEARTBEAT_TIME         int     = 100 // 一秒十次心跳
	BASE_HEARTBEAT_TIMEOUT int64   = 200
	BASE_ELECTION_TIMEOUT  int64   = 1000
	APPLY_TIMEOUT          int     = 10
	RAND_FACTOR            float64 = 0.8
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm   int
	votedFor      int
	log           []LogEntry
	state         int
	electionTime  time.Time
	heartbeatTime time.Time
	voteNums      int

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	snapshot          []byte
	applySnapshot     []byte
	lastIncludedIndex int
	lastIncludeTerm   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludeTerm)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, lastIncludedIndex, lastIncludeTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludeTerm) != nil || d.Decode(&log) != nil {
		//   error...
		Debug(dError, "S%d readPersist failed", rf.me)
		return
	}
	snapshot := rf.persister.ReadSnapshot()
	// Debug(dPersist, "S%d currenterm(%d), votedFor(%d), log(%v)", rf.me, currentTerm, votedFor, log)
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludeTerm = lastIncludeTerm
	rf.snapshot = snapshot
	rf.applySnapshot = snapshot

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.lastIncludedIndex {
		if index > rf.commitIndex {
			Debug(dSnap, "S%d commit index %d < snapIndex %d", rf.me, rf.commitIndex, index)
			return
		}
		rf.snapshot = snapshot
		rf.applySnapshot = snapshot
		// Debug(dSnap, "S%d lastIncludeindex is %d, log len is %d, index is %d", rf.me, rf.lastIncludedIndex, len(rf.log), index)
		y := make([]LogEntry, len(rf.log)-(index-rf.lastIncludedIndex))
		copy(y, rf.log[index-rf.lastIncludedIndex:])
		//Debug(dSnap, "S%d new log length is %d", rf.me, len(y))
		rf.lastIncludeTerm = rf.getLogEntry(index).Term
		rf.lastIncludedIndex = index
		rf.log = y
		// Debug(dSnap, "S%d lastIncludeindex is %d, log len is %d, index is %d", rf.me, rf.lastIncludedIndex, len(rf.log), index)

	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // term in the conflicting entry (if any)
	XIndex  int // index of first entry with that term (if any)
	XLen    int // log length
}
type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) setelectionTime(duration time.Duration) {
	now := time.Now()
	rf.electionTime = now.Add(duration)
}
func (rf *Raft) setheartbeatTime(duration time.Duration) {
	now := time.Now()
	rf.heartbeatTime = now.Add(duration)
}
func (rf *Raft) getLastLoginfo() (index, term int) {
	lastIndex := rf.lastIncludedIndex + len(rf.log)
	log := rf.getLogEntry(lastIndex)
	return lastIndex, log.Term
}
func (rf *Raft) getLogEntry(index int) LogEntry {
	if index < rf.lastIncludedIndex {
		Debug(dLog, "S%d index < rf.lastIncludeIndex", rf.me)
		return LogEntry{
			Command: nil,
			Term:    -1,
		}

	}
	if index == 0 {
		return LogEntry{
			Command: nil,
			Term:    0,
		}
	}
	if index < 0 || index > len(rf.log)+rf.lastIncludedIndex {
		return LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	if rf.lastIncludedIndex == index {
		return LogEntry{Command: nil, Term: rf.lastIncludeTerm}
	}
	//Debug(dLog, "S%d log entry index is %d, rf.X is %d", rf.me, index, rf.X)
	// index - rf.lastIncludedIndex <= len(rf.log)
	return rf.log[index-rf.lastIncludedIndex-1]
}
func (rf *Raft) getSlice(startIndex, endIndex int) []LogEntry {
	// [startIndex, endIndex)

	return rf.log[startIndex-rf.lastIncludedIndex-1 : endIndex-rf.lastIncludedIndex-1]
}
func (rf *Raft) checkTerm(term int) bool {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		return true
	}
	return false
}
func randelectionTimeOut() time.Duration {
	dur := int64(RAND_FACTOR*float64((rand.Int63()%BASE_ELECTION_TIMEOUT))) + BASE_ELECTION_TIMEOUT
	return time.Millisecond * time.Duration(dur)
}
func randheartbeatTimeOut() time.Duration {
	dur := int64(RAND_FACTOR*float64((rand.Int63()%BASE_HEARTBEAT_TIMEOUT))) + BASE_HEARTBEAT_TIMEOUT
	return time.Millisecond * time.Duration(dur)
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	// 判断投票
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 判断up-to-date
		index, term := rf.getLastLoginfo()
		if args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= index) {
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.setelectionTime(randelectionTimeOut())
			reply.VoteGranted = true
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term < rf.currentTerm {
			return false
		}
		if args.Term != rf.currentTerm {
			return false
		}
		if rf.checkTerm(reply.Term) {
			return false
		}
		if reply.VoteGranted {
			rf.voteNums++
			if rf.voteNums > len(rf.peers)/2 && rf.state == CANDIDATE {
				rf.state = LEADER
				// 初始化volatile state on leader
				lastIndex, _ := rf.getLastLoginfo()
				for i, _ := range rf.nextIndex {
					rf.nextIndex[i] = lastIndex + 1
					rf.matchIndex[i] = 0
				}
				// 成为leader之后，开始发送心跳包
				rf.sendEntries(true)
			}
		}
	}
	return ok
}
func (rf *Raft) beginElection() {

	// convert to candidate
	rf.state = CANDIDATE
	rf.currentTerm++
	// vote for self
	rf.votedFor = rf.me
	rf.voteNums = 1
	rf.persist()
	// reset election timer
	rf.setelectionTime(randelectionTimeOut())
	index, term := rf.getLastLoginfo()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: index,
		LastLogTerm:  term,
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(peer, &args, &reply)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	// 5.1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if rf.state == CANDIDATE && rf.currentTerm == args.Term {
		rf.state = FOLLOWER
	}
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	rf.setelectionTime(randheartbeatTimeOut())
	// check prevLogIndex/prevLogTerm
	// if args.PrevLogTerm == -1 || rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
	// 	Debug(dTerm, "S%d args.PrevLogTerm(%v), follower term(%v)", rf.me, args.PrevLogTerm, rf.getLogEntry(args.PrevLogIndex).Term)
	// 	return
	// }
	if args.PrevLogIndex < rf.lastIncludedIndex {
		snapshotLogLen := rf.lastIncludedIndex - args.PrevLogIndex
		if snapshotLogLen >= len(args.Entries) {
			// AppendEntries中的日志已经全部在镜像中
			reply.Success = true
			return
		} else {
			// 从lastIncludeIndex截断日志
			args.PrevLogIndex = rf.lastIncludedIndex
			args.PrevLogTerm = rf.lastIncludeTerm
			args.Entries = args.Entries[snapshotLogLen:]
		}
	}
	// 优化 checkprevLogIndex/prevLogTerm
	if rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.XLen = len(rf.log) + rf.lastIncludedIndex
		conflictEntry := rf.getLogEntry(args.PrevLogIndex)
		if conflictEntry.Term == -1 {
			// 不存在对应PrevLogIndex
			reply.XTerm = -1
		} else {
			reply.XTerm = conflictEntry.Term
			for index := args.PrevLogIndex; index > rf.lastIncludedIndex; index-- {
				if rf.getLogEntry(index).Term != conflictEntry.Term {
					reply.XIndex = index + 1
					return
				}
			}
			reply.XIndex = 1
		}
		return
	}
	// 添加新entry
	for index, entry := range args.Entries {
		if rf.getLogEntry(args.PrevLogIndex+index+1).Term != entry.Term {
			rf.log = append(rf.getSlice(1+rf.lastIncludedIndex, args.PrevLogIndex+index+1), args.Entries[index:]...)
			Debug(dCommit, "S%d the most new log are (%v)", rf.me, rf.log)
			rf.persist()
			break
		}
	}
	// 检查leaderCommit
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}
	reply.Success = true
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term < rf.currentTerm {
			// 过期AppendEntries回复
			return false
		}
		if args.Term != rf.currentTerm {
			//
			return false
		}
		if rf.checkTerm(reply.Term) {
			return false
		}
		if reply.Success {
			rf.matchIndex[server] = Max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
			//Debug(dLeader, "S%d follower(%d) matchIndex:%d", rf.me, server, rf.matchIndex[server])
			rf.nextIndex[server] = Max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])
			//Debug(dLeader, "S%d follower(%d) nextIndex:%d", rf.me, server, rf.nextIndex[server])
			// 尝试更新commitIndex
			for N := len(rf.log) + rf.lastIncludedIndex; N > rf.commitIndex && rf.getLogEntry(N).Term == rf.currentTerm; N-- {
				majorCommit := 1
				for peer, matchIndex := range rf.matchIndex {
					if peer == rf.me {
						continue
					}
					if matchIndex >= N {
						majorCommit++
					}
					if majorCommit > len(rf.peers)/2 {
						rf.commitIndex = N
						Debug(dLeader, "S%d commit index is (%d)", rf.me, N)
						break
					}
				}
			}
		} else {
			// 更新nextIndex位置,重新发送
			// rf.nextIndex[server]--
			if reply.XTerm == -1 {
				// follower's log is too short
				rf.nextIndex[server] = reply.XLen + 1
			} else {
				exist := false
				for index := args.PrevLogIndex; index > rf.lastIncludedIndex; index-- {
					if rf.getLogEntry(index).Term == reply.XTerm {
						rf.nextIndex[server] = index + 1
						exist = true
						break
					}
				}
				if !exist {
					rf.nextIndex[server] = reply.XIndex
				}
			}
			newNextIndex := rf.nextIndex[server]
			if newNextIndex > rf.lastIncludedIndex {
				Debug(dLeader, "S%d the newnextIndex of %d follower is %d", rf.me, server, newNextIndex)
				newReply := AppendEntriesReply{}
				newArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: newNextIndex - 1,
					PrevLogTerm:  rf.getLogEntry(newNextIndex - 1).Term,
					LeaderCommit: rf.commitIndex,
				}
				newArgs.Entries = append(newArgs.Entries, rf.log[newNextIndex-rf.lastIncludedIndex-1:]...)
				go rf.sendAppendEntries(server, &newArgs, &newReply)
			} else {
				Debug(dSnap, "S%d is sending snapshot", rf.me)
				// 发送快照
				snapshotArgs := InstallSnapshotArgs{
					Term:             rf.currentTerm,
					LeaderId:         rf.me,
					LastIncludeIndex: rf.lastIncludedIndex,
					LastIncludeTerm:  rf.lastIncludeTerm,
					Data:             rf.snapshot,
				}
				snapshotReply := InstallSnapshotReply{}
				// 重置心跳时间
				rf.setheartbeatTime(randheartbeatTimeOut())
				go rf.SendSnapshot(server, &snapshotArgs, &snapshotReply)
			}
		}
	}
	return ok
}
func (rf *Raft) sendEntries(heartbeat bool) {
	rf.setheartbeatTime(time.Duration(HEARTBEAT_TIME) * time.Millisecond)
	rf.setelectionTime(randheartbeatTimeOut())
	lastindex, _ := rf.getLastLoginfo()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		reply := AppendEntriesReply{}
		nextIndex := rf.nextIndex[peer]
		Debug(dLeader, "S%d the nextIndex of %d follower is %d", rf.me, peer, nextIndex)
		if lastindex <= rf.lastIncludedIndex || nextIndex <= rf.lastIncludedIndex {
			// 发送快照
			snapshotArgs := InstallSnapshotArgs{
				Term:             rf.currentTerm,
				LeaderId:         rf.me,
				LastIncludeIndex: rf.lastIncludedIndex,
				LastIncludeTerm:  rf.lastIncludeTerm,
				Data:             rf.snapshot,
			}
			snapshotReply := InstallSnapshotReply{}
			// // 重置心跳时间
			// rf.setheartbeatTime(randheartbeatTimeOut())
			go rf.SendSnapshot(peer, &snapshotArgs, &snapshotReply)
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.getLogEntry(nextIndex - 1).Term,
			LeaderCommit: rf.commitIndex,
		}
		args.Entries = make([]LogEntry, 0)
		if lastindex >= nextIndex {
			// 将nextIndex之后的entry全部发送给followers
			args.Entries = append(args.Entries, rf.log[nextIndex-rf.lastIncludedIndex-1:]...)
			//Debug(dLeader, "S%d the nextIndex is (%d), the append entries are (%v)", rf.me, nextIndex, args.Entries)
		}
		go rf.sendAppendEntries(peer, &args, &reply)
	}
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 重置选举时间
	rf.setelectionTime(randheartbeatTimeOut())
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.LastIncludeIndex < rf.lastIncludedIndex {
		// 过期快照
		return
	}
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	entry := rf.getLogEntry(args.LastIncludeIndex)
	if args.LastIncludeTerm == entry.Term {
		return
	}
	rf.log = make([]LogEntry, 0)
	rf.snapshot = args.Data
	rf.applySnapshot = args.Data
	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludedIndex = args.LastIncludeIndex
	rf.persist()
}
func (rf *Raft) SendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term < rf.currentTerm {
			return
		}
		if args.Term != rf.currentTerm {
			return
		}
		rf.checkTerm(reply.Term)
		rf.nextIndex[server] = Max(rf.nextIndex[server], args.LastIncludeIndex+1)
		rf.matchIndex[server] = Max(rf.matchIndex[server], args.LastIncludeIndex)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	// Your code here (2B).
	if !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == LEADER {
			isLeader = true
			term = rf.currentTerm
			entry := LogEntry{Term: term, Command: command}
			rf.log = append(rf.log, entry)
			Debug(dClient, "S%d client->leader command(%v)", rf.me, entry)
			rf.persist()
			index = len(rf.log) + rf.lastIncludedIndex

			rf.sendEntries(false)
		}
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		// Check if a leader election should be started.
		if time.Now().After(rf.electionTime) {
			rf.beginElection()
		}
		// 检查是否该发送心跳包
		if rf.state == LEADER && time.Now().After(rf.heartbeatTime) {
			rf.sendEntries(true)
		}
		rf.mu.Unlock()
		// TICK_INTERNAL间隔来检测是否超时
		time.Sleep(time.Duration(TICK_INTERVAL) * time.Millisecond)
	}
}
func (rf *Raft) applyTicker(applyChan chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		applyEntries := make([]ApplyMsg, 0)
		if rf.lastIncludedIndex > rf.lastApplied && rf.applySnapshot != nil {
			rf.lastApplied = rf.lastIncludedIndex
			snapshotEntry := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				SnapshotTerm:  rf.lastIncludeTerm,
				SnapshotIndex: rf.lastIncludedIndex,
				Snapshot:      rf.applySnapshot,
			}
			rf.applySnapshot = nil
			applyEntries = append(applyEntries, snapshotEntry)
		}
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyEntry := ApplyMsg{
				CommandValid: true,
				Command:      rf.getLogEntry(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			applyEntries = append(applyEntries, applyEntry)
		}
		rf.mu.Unlock()
		for _, applyEntry := range applyEntries {
			applyChan <- applyEntry
		}
		time.Sleep(time.Duration(APPLY_TIMEOUT) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//Debug(dTimer, "S%d Leader, checking hearbeats", me)
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		currentTerm:       0,
		votedFor:          -1,
		log:               []LogEntry{},
		commitIndex:       0,
		lastApplied:       0,
		state:             FOLLOWER,
		voteNums:          0,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		lastIncludedIndex: 0,
		lastIncludeTerm:   0,
		snapshot:          nil,
		applySnapshot:     nil,
	}
	rf.setelectionTime(randheartbeatTimeOut())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// periodlly apply the committed log entry, 10ms一次
	go rf.applyTicker(applyCh)
	return rf
}
