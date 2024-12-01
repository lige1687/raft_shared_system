package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server. 创建新的raft
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader 获取日志状态, 当前的term 并且是否是leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sort"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry. 包含了一个新提交的 日志项目
//
// in part 3D you'll want to send other kinds of messages (e.g.,  发送其他通过这个 applymsg 如快照
// 如果 valid为false 表示 是用于其他用途的 , ! 一个flag 而已
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
// 即每个节点 需要能通知上层的服务自己的节点提交到哪里了? applymas就是这样的作用 commmand 就是实际的日志的命令, 具体类型取决于你的 实现

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState int

const (
	FOLLOWER RaftState = iota // 初始状态为 FOLLOWER
	CANDIDATE
	LEADER
)

type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers 所有的 其他集群存储 地方 , 通过rpc交流 ,所以存放的是一群rpc 的client
	persister *Persister          // Object to hold this peer's persisted state  持久者, 存放当前节点的持久化数据
	me        int                 // this peer's index into peers[] 表示当前 节点的索引
	dead      int32               // set by Kill() 使用kill

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 持久化的属性
	currentTerm int         // 当前任期
	votedFor    int         // 当前任期投票给了谁
	lm          *LogManager // 管理日志存储

	snapshot []byte // 上一次保存的快照

	//volatile 属性 , 易失属性

	// 易失属性
	state RaftState // 当前角色状态

	commitIndex int // 已提交的最大下标
	lastApplied int // 以应用到状态机的最大下标( 提交和应用没是两回事

	//installSnapCh chan int // install snapshot的信号通道，传入trim index
	// backupApplied bool        // 宕机, 看是否有快照? 是否需要重新的apply

	lastIncludedIndex interface{} // 最后一个包含快照里边的日志index
	lastIncludedTerm  interface{}

	// Leader特有字段
	nextIndexes    []int       // 对于每个follower，leader要发送的下一个复制日志下标
	matchIndexes   []int       // 已知每个follower和自己一致的最大日志下标
	electionTimer  *time.Timer // 选举计时器
	heartbeatTimer *time.Timer // 心跳计时器

	applyCh   chan ApplyMsg // apply通道 , 用于和peer交流
	applyCond *sync.Cond    // apply协程唤醒条件  ,应该根据 mu 创建

	replicatorCond []*sync.Cond // 去管理每个 fo 的复制状态? 去通知每个fo的复制器 的等待条件

	// 待定是否需要

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 保存Raft的持久化状态（编码）
func (rf *Raft) persist() {
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), nil)
	// 调用方法
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetId() int {
	return rf.me
}

// GetState return currentTerm and whether this server believes it is the leader.
// 要求两个返回值哦
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 客户端发来请求, 要求index之前的都 快照了, 只保留之后的

	//放心锁 ,没有问题
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// snapshotIndex := rf.getFirstLog().Index
	snapshotIndex := rf.lm.FirstIndex()
	// 第一个log 的逻辑日志, 即在内存中的 第一个log 的逻辑index
	if index <= snapshotIndex {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	// trim 目录， 移除快照之前的 日志条目
	// index - 第一个内存中的index 才是 logs数组中的索引
	//rf.lm.logs = shrinkEntriesArray(rf.lm.logs, index) // 结果重新赋值给 lm的logs

	// 别忘记更新 lastincluded  index !
	var log []LogEntry
	for i := index + 1; i <= rf.getLastLogIndex(); i++ {
		log = append(log, rf.lm.GetEntry(i))
	}
	rf.lm.logs = log
	rf.lm.logs[0].Command = nil
	// 更新 snapshot 的状态, 等
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lm.GetEntry(rf.lm.FirstIndex()), rf.lm.GetEntry(rf.lm.LastIndex()), index, snapshotIndex)
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lm.logs)
	return w.Bytes()
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters !
type RequestVoteArgs struct {
	// 详细内容参考 论文中的设计
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (3A, 3B).
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
// 大写字母开始! 必须
type RequestVoteReply struct {

	// Your data here (3A).
	Term        int  // 投票者的任期
	VoteGranted bool // 是否决定给你投票
}

// example code to send a requestVote RPC to a server.
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
	return ok

}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, -1, false
	}
	// first append log entry for itself
	newLogIndex := rf.lm.LastIndex() + 1
	rf.lm.logs = append(rf.lm.logs, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   newLogIndex,
	})
	rf.persist()
	rf.matchIndexes[rf.me], rf.nextIndexes[rf.me] = newLogIndex, newLogIndex+1
	DPrintf("{Node %v} starts agreement on a new log entry with command %v in term %v", rf.me, command, rf.currentTerm)
	// then broadcast to all peers to append log entry
	rf.BroadcastHeartbeat(false)
	// return the new log index and term, and whether this server is the leader
	return newLogIndex, rf.currentTerm, true
}

//func (rf *Raft) Start(command interface{}) (int, int, bool) {
//	rf.mu.Lock()
//	// 需要锁,  保证状态  !
//	defer rf.mu.Unlock()
//	if rf.state != LEADER {
//		return -1, -1, false
//	}
//	//append里边获取的是 lm 的内部锁, 没问题, 层级更低
//	newLog := rf.lm.AppendEntry(rf.currentTerm, command)
//	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
//	rf.BroadcastHeartbeat(false) // 开始广播维护统治和 更新等
//	return newLog.Term, newLog.Term, true
//}

// Kill the tester doesn't halt goroutines created by Raft after each test,
//
//	but it does call the Kill() method. your code can use killed() to
//
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

// 当一个raft节点产生的,时候, 就需要定期的去检查是否需要 重新选举? 即是否有leader
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(CANDIDATE)
			rf.currentTerm += 1
			rf.StartElection()
			rf.resetElectionTimer()
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.BroadcastHeartbeat(true)
				rf.resetHeartbeatTimer()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) StartElection() {
	rf.votedFor = rf.me

	rf.persist()
	request := rf.getRequestVoteArgs()

	// c , 这个方法也不能加锁 !!
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, request)
	// use Closure
	grantedVotes := 1

	// 释放方法上边的锁 ,不能释放, 防止重复的 unlock
	// rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue //跳过自己
		}
		go func(peer int) {
			response := new(RequestVoteReply)
			// 对于每一个同类, 发送请求
			// todo 发送之前需要保证有锁, 否则可能死锁, 这里确实如果在上层获取锁了, 但是里边go 又去获得锁, 会不会死锁?
			if rf.sendRequestVote(peer, request, response) {
				// 这个时候再 获取锁 , defer, 因为在现场里边, 需要避免多线程并发的访问
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, response, peer, request, rf.currentTerm)
				if rf.currentTerm == request.Term && rf.state == CANDIDATE {
					if response.VoteGranted { // 确保投给我了
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 { // 超过半数, 开始
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)

							// 在外层调用了锁, 所以内存不能再 调用同一个锁
							//todo 是否是这个 changestate出了问题? 什么时候 term需要+1 ?
							//rf.initLeader(LEADER)
							rf.ChangeState(LEADER)
							// 发送心跳
							rf.BroadcastHeartbeat(true)
						}
					} else if response.Term > rf.currentTerm { // 人家比你还大, 回去吧你
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.ChangeState(FOLLOWER)
						rf.currentTerm, rf.votedFor = response.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) RequestVote(request *RequestVoteArgs, response *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.ChangeState(FOLLOWER)
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

	if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = request.CandidateId
	rf.resetElectionTimer()
	response.Term, response.VoteGranted = rf.currentTerm, true
}

func (rf *Raft) isLogUpToDate(index, term int) bool {
	lastLog := rf.lm.GetEntry(rf.lm.LastIndex())
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

func (rf *Raft) enterNewTerm(newTerm int) {
	//rf.mu.Lock()         // 加锁，确保操作是原子的
	//defer rf.mu.Unlock() // 在函数结束时解锁
	// 但是这里的加锁 , 所有的父使用方法, 已经加过锁了, 所以 这里不能锁了, 锁的粒度有点大了

	if newTerm <= rf.currentTerm {
		return // 新的 term 必须比当前 term 大，才需要更新
	}

	rf.currentTerm = newTerm // 更新当前的任期
	rf.votedFor = -1         // 重置投票信息，以便在新任期重新投票
	rf.state = FOLLOWER      // 回到 FOLLOWER 状态，防止在旧的任期中担任 Leader

	rf.persist() // 持久化新的状态，确保重启后能恢复

	// 可以在此处记录日志或其他处理逻辑
	// fmt.Printf("Node %d entered new term %d\n", rf.me, newTerm)
}

// getRequestVoteArgs 构造请求投票的参数。 要求调用方法, 加锁 , 本层不能加锁
func (rf *Raft) getRequestVoteArgs() *RequestVoteArgs {
	//rf.mu.Lock()
	//defer rf.mu.Unlock() // 需要锁
	// 锁锁锁
	// 构造 RequestVoteArgs 并返回
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,       // 当前的任期
		CandidateId:  rf.me,                // 当前节点的 ID
		LastLogIndex: rf.getLastLogIndex(), // 获取最后一个日志条目的索引
		LastLogTerm:  rf.getLastLogTerm(),  // 获取最后一个日志条目的任期
	}
	return args
}

// getLastLogIndex 返回最后一个日志条目的索引。
func (rf *Raft) getLastLogIndex() int {
	if len(rf.lm.logs) == 0 {
		return 0
	}
	return len(rf.lm.logs) - 1
}

// getLastLogTerm 返回最后一个日志条目的任期。
func (rf *Raft) getLastLogTerm() int {
	if len(rf.lm.logs) == 0 {
		return 0
	}
	return rf.lm.logs[len(rf.lm.logs)-1].Term
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int        // 即自己的id
	PrevLogIndex int        // 前一个日志的日志号
	PrevLogTerm  int        //
	Entries      []LogEntry //  这是一个数组存放的是指向日志体的指针,
	LeaderCommit int        //leader已经提交的日志 数
	LeaderID     int
}

func (request AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,PrevLogIndex:%v,PrevLogTerm:%v,LeaderCommit:%v,Entries:%v}", request.Term, request.LeaderId, request.PrevLogIndex, request.PrevLogTerm, request.LeaderCommit, request.Entries)
}

// fo 回复leader 的内容
type AppendEntriesReply struct {
	Term    int // 当前 fo 的任期
	XTerm   int // 冲突的log的日期, 用于定位一致性检查
	XIndex  int // 任期为X term 的第一个log 的index
	XLen    int // follower自己的log长度
	Success bool
}

func (rf *Raft) resetElectionTimer() {
	timeout := RandomElectionTimeout()
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(timeout)
	} else {
		rf.electionTimer.Reset(timeout)
	}
}

// 重置 心跳
func (rf *Raft) resetHeartbeatTimer() {
	if rf.heartbeatTimer == nil {
		rf.heartbeatTimer = time.NewTimer(StableHeartbeatTimeout())
	} else {
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}

const ElectionTimeout = 1000
const HeartbeatTimeout = 125

type LockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

var GlobalRand = &LockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

func (r *LockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

// 生成一个随机的选举超时时间（例如 150 到 300 毫秒之间）
func RandomElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+GlobalRand.Intn(ElectionTimeout)) * time.Millisecond
}
func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

// Make the service or tester wants to create a Raft server. the ports
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

	rf := &Raft{
		peers:          peers,
		persister:      persister,
		commitIndex:    0,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          FOLLOWER,
		currentTerm:    0,
		votedFor:       -1,
		lm: &LogManager{
			logs:             make([]LogEntry, 1), // Initialize with a dummy entry at index 0
			lastTrimmedIndex: 0,
			lastTrimmedTerm:  0,
			persister:        persister,
		},
		nextIndexes:    make([]int, len(peers)),
		matchIndexes:   make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()), // 固定心跳, 时间是多少来着, 10s ?,,
		electionTimer:  time.NewTimer(RandomElectionTimeout()),  // 随机 选举时长,todo时长还没确定
		//lastIncludedIndex: 0,
		//lastIncludedTerm:  -1,
	}
	// 在 崩溃后重新读取信息, 从持久化的介质
	// 之前没有产生锁, 此方法内部需要 锁来保持 磁盘中的 准确读取到 当前!
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	// 需要开共len(peer) - 1 个线程replicator，分别管理对应 peer 的复制状态, apply使用的也是 节点的互斥锁啊
	rf.applyCond = sync.NewCond(&rf.mu)

	// 为每个不是leader 的 产生一个replicator goroutine
	for i := 0; i < len(peers); i++ {
		rf.matchIndexes[i], rf.nextIndexes[i] = 0, rf.lm.LastIndex()+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}
	// start ticker goroutine to start elections， 用来触发 heartbeat timeout 和 election timeout
	go rf.ticker()
	//....

	// start applier goroutine to push committed logs into applyCh exactly once， ，用来往 applyCh 中 push 提交的日志并保证 exactly once
	// 确保applier能获取锁哦!
	//todo, 这里的锁的问题? 是否死锁 了
	go rf.applier()

	return rf
}

// 复制线程, 用于follower同步, leader调用, 去检查
// 每个 fo有一个leader
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	// 获取对应fol 的锁

	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// 如果不需要为这个 peer 复制条目，只要释放 CPU 并等待其他 goroutine 的信号，如果服务添加了新的命令
		// 如果这个peer需要复制条目，这个goroutine会多次调用replicateOneRound(peer)直到这个peer赶上，然后等待
		//不断的 追加使得 复制追上进度
		// leader调用去检查是否一个节点需要复制
		for !rf.needReplicating(peer) {
			// 如果不需要复制, 就阻塞当前线程即可, 直到被唤醒

			rf.replicatorCond[peer].Wait()
			// todo , 你还没释放锁, 这个replicator先 阻塞了, 那岂不是 你持有的资源在 signal前不会被释放?
			// the principle of the wait, it'll release the lock on cond and when it be signaled it will try to reclaim the lock
			// so there is no need to check it
			//
		}

		//或许使用管道技术会更好一点这里
		// 如果需要复制, 遍历总会等到一个需要复制的 , 直接复制一个回合, 单轮的工作
		// 搭配外层的 进行多轮的工作, 直到没有任何节点需要复制
		rf.replicateOnceRound(peer)
	}
}

// 是否一个节点需要 复制? leader调用
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock() // 只需要用到读suo
	// 当前调用节点是leader才能 复制日志!
	defer rf.mu.RUnlock()
	return rf.state == LEADER && rf.matchIndexes[peer] < rf.lm.LastIndex()
}

// BroadcastHeartbeat 心跳维持领导力( 另一个方法sendheartbeat的设计方式
func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// 是心跳, 则 作用是维持统治
			go rf.replicateOnceRound(peer)
		} else {
			// 不是心跳, 表示此时是真的来append
			//  将任务交给replicator, 去唤醒
			rf.replicatorCond[peer].Signal()
		}
	}
}

type InstallSnapshotRequest struct {
	Term     int // 当前请求的任期
	LeaderID int // 领导者的节点 ID

	SnapshotData      []byte
	LastIncludedTerm  int
	LastIncludedIndex int // 快照包含的最后一个日志的信息
}

type InstallSnapshotResponse struct {
	Term    int  // 当前任期
	Success bool // 快照安装是否成功
}

// 生成 安装快照请求
func (rf *Raft) genInstallSnapshotRequest() *InstallSnapshotRequest {
	return &InstallSnapshotRequest{
		Term:              rf.currentTerm,                         // 当前任期
		LeaderID:          rf.me,                                  // 领导者 ID
		LastIncludedIndex: rf.lm.LastIndex(),                      // 快照的索引
		LastIncludedTerm:  rf.lm.GetEntry(rf.lm.LastIndex()).Term, // 快照的任期
		SnapshotData:      rf.persister.snapshot,                  // 快照数据
	}
}

// leader发送安装snapshot请求
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotResponse) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// InstallSnapshot follower处理leader发来的 安装快照的请求, 除了检查term外, 无条件的服从
func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lm.GetEntry(rf.lm.FirstIndex()), rf.lm.GetEntry(rf.lm.LastIndex()), request, response)

	response.Term = rf.currentTerm

	if request.Term < rf.currentTerm {
		// 检查term
		return
	}

	if request.Term > rf.currentTerm { // 更新自己的term
		rf.currentTerm, rf.votedFor = request.Term, -1
		// rf.state=FOLLOWER
		rf.persist()
	}

	// 重置自己的选举, 因为自己得到了一个 认可的leader 的 请求, 需要维护自己的 follower状态
	rf.ChangeState(FOLLOWER)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// 如果是expired 的 snapshot , 则直接返回即可, 不需要应用
	if request.LastIncludedIndex <= rf.commitIndex {
		return
	}

	// 否则将snapshot 信息放入applych, 等待异步的apply
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      request.SnapshotData,
			SnapshotTerm:  request.LastIncludedTerm,
			SnapshotIndex: request.LastIncludedIndex,
		}
	}()
}

// 服务端送来的请求, 你去处理
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// 和上述逻辑一样, 过期的则跳过
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}
	// 如果不过期, 需要apply的话, 两种情况, 一种是超过了索引界限, 则直接置空内存里边的 slice即可, 此时的数据在snapshot里边, 注意保存snapshot 在本地哦

	if lastIncludedIndex > rf.lm.LastIndex() {
		rf.lm.logs = make([]LogEntry, 1)
	} else { // rf.lm.logs[lastIncludedIndex-rf.getFirstLog().Index:]
		//relative := lastIncludedIndex - rf.lm.LastIndex()
		//
		//rf.lm.logs = shrinkEntriesArray(rf.lm.logs, relative)
		//// 表示裁剪了, 是不是得更新 lastincluded index ?

		rf.lm.logs = shrinkEntries(rf.lm.logs[lastIncludedIndex-rf.lm.FirstIndex():])

		rf.lm.logs[0].Command = nil
	}
	// 维护此时的快照boundary, 即 lastIncluded
	rf.lm.logs[0].Term, rf.lm.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	// 保持 snapshot 的状态
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lm.GetEntry(rf.lm.FirstIndex()), rf.lm.GetEntry(rf.lm.LastIndex()), lastIncludedTerm, lastIncludedIndex)
	return true
}

// ...todo 我的方法都是 get args啊, 你这个命名错了吧
// genAppendEntriesRequest generates an appendEntries request to send to a peer.

func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesArgs {
	// 获取prevLogIndex对应的日志条目
	firstLogIndex := rf.lm.FirstIndex()
	entries := make([]LogEntry, len(rf.lm.logs[prevLogIndex-firstLogIndex+1:]))
	copy(entries, rf.lm.logs[prevLogIndex-firstLogIndex+1:])
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.lm.logs[prevLogIndex-firstLogIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	return args
}

// Leader 向Follwer 发送复制请求, 根据fo 的进度, 决定是通过 安装快照还是 追加条目来同步日志
// 同步有两种方法, 一个是 apply快照, 另一个是 追加日志

// replicatooneround 实现有问题

func (rf *Raft) replicateOnceRound(peer int) {
	rf.mu.RLock()
	if rf.state != LEADER {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndexes[peer] - 1
	if prevLogIndex < rf.lm.FirstIndex() {
		// only send InstallSnapshot RPC
		args := rf.genInstallSnapshotRequest()
		rf.mu.RUnlock()
		reply := new(InstallSnapshotResponse)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			if rf.state == LEADER && rf.currentTerm == args.Term {
				if reply.Term > rf.currentTerm {
					rf.ChangeState(FOLLOWER)
					rf.currentTerm, rf.votedFor = reply.Term, -1
					rf.persist()
				} else {
					rf.nextIndexes[peer] = args.LastIncludedIndex + 1
					rf.matchIndexes[peer] = args.LastIncludedIndex
				}
			}
			rf.mu.Unlock()
			DPrintf("{Node %v} sends InstallSnapshotArgs %v to {Node %v} and receives InstallSnapshotReply %v", rf.me, args, peer, reply)
		}
	} else {
		args := rf.genAppendEntriesRequest(prevLogIndex)
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			if args.Term == rf.currentTerm && rf.state == LEADER {
				if !reply.Success {
					if reply.Term > rf.currentTerm {
						// 现在不是leader
						rf.ChangeState(FOLLOWER)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					} else if reply.Term == rf.currentTerm {
						// 减 index并且重新尝试
						rf.nextIndexes[peer] = reply.XIndex
						// TODO: 可以使用二分查找优化, 但是论文里边说不需要的
						if reply.XTerm != -1 {
							firstLogIndex := rf.lm.FirstIndex()
							for index := args.PrevLogIndex - 1; index >= firstLogIndex; index-- {
								if rf.lm.logs[index-firstLogIndex].Term == reply.XTerm {
									rf.nextIndexes[peer] = index
									break
								}
							}
						}
					}
				} else {
					rf.matchIndexes[peer] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndexes[peer] = rf.matchIndexes[peer] + 1
					// advance commitIndex if possible
					rf.advanceCommitIndexForLeader()
				}
			}
			rf.mu.Unlock()
			DPrintf("{Node %v} sends AppendEntriesArgs %v to {Node %v} and receives AppendEntriesReply %v", rf.me, args, peer, reply)
		}
	}
}

// Leader处理Follower 的 ae response
func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesArgs, response *AppendEntriesReply) {
	if rf.state == LEADER && rf.currentTerm == request.Term {
		// 如果当前是leader, 并且 任期一致,  则进行处理
		if response.Success { // 如果 ae成功, leader 进行
			// 根据返回结果, 维护对应的 matchidnex和nextindex,

			rf.matchIndexes[peer] = request.PrevLogIndex + len(request.Entries)
			//match是 追随者同步的最大索引位置
			// 而 next是要发送的下一个索引的位置, 即match 的下一个咯
			rf.nextIndexes[peer] = rf.matchIndexes[peer] + 1
			rf.advanceCommitIndexForLeader() // 推进leader 的 commitindex ( 根据follower
		} else {
			//失败的follower ae response , leader要检查哪里有问题
			// 有可能是自己不再是leader了, 所以检查一下是否有新的 leader , 即term 的检查
			if response.Term > rf.currentTerm {
				rf.ChangeState(FOLLOWER)
				// 将自己变回fo , 并重启 选举计时器等
				rf.currentTerm, rf.votedFor = response.Term, -1
				rf.persist() // 保持自己的follower状态

			} else if response.Term == rf.currentTerm {
				// 确定当前是同一个任期  , 需要重新定位, 即还没找到 缺失的位置, 需要继续定位
				// 更新nextindex 为冲突的index( 这里的nextindex是逻辑上的哦, 不是实际的

				rf.nextIndexes[peer] = response.XIndex

				if response.XTerm != -1 { // 说明 是日志的xterm出现了冲突, 即虽然节点在一个 term上,但是日志进度不在
					// 需要锁定到对应的term, 这里可以使用fastbackup方法

					firstIndex := rf.lm.FirstIndex()

					// 找到冲突的 任期, 通过leader 的第一个 index for直接找( 一个个的找, 这也是论文给出的方案
					// 为什么用for? 因为差的不太多, for 不会很复杂的
					// leader从previndex往前找, 直到找到对应的 冲突的term为止
					// 从request里边找自己的 上一个index, 因为request也是leader发的,包含了leader 的信息
					for i := request.PrevLogIndex; i >= firstIndex; i-- {

						//因为之前判断用ae的话, 肯定是差很多, 所以肯定在 leader 的firstindex之前
						// 通过i- firstindex, 确定实际 leader中的索引位置
						if rf.lm.logs[i-firstIndex].Term == response.XTerm {
							rf.nextIndexes[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lm.GetEntry(rf.lm.FirstIndex()), rf.lm.GetEntry(rf.lm.LastIndex()), rf.lastIncludedTerm, rf.lastIncludedIndex)
}

// handleInstallSnapshotResponse
// 从一个peer来的请求, 告诉你要按照快照了
// 传入request, 通过response返回信息
func (rf *Raft) handleInstallSnapshotResponse(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	if response.Term > rf.currentTerm {
		// 如果返回的任期大于当前任期，更新 Raft 节点的任期并转换为 Follower
		rf.enterNewTerm(response.Term)
		rf.state = FOLLOWER
	}

	// 如果响应成功，更新该 peer 的 matchIndex 和 nextIndex
	if response.Success {
		rf.matchIndexes[peer] = request.LastIncludedIndex
		rf.nextIndexes[peer] = request.LastIncludedIndex + 1
		rf.applyCond.Signal() // 唤醒 apply goroutine，应用新的快照
	} else {
		// 处理失败的响应，通常会进行重试或调整 nextIndex
		rf.nextIndexes[peer] = rf.matchIndexes[peer] // 调整 nextIndex
	}
}

func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndexes)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndexes)
	sort.Ints(sortMatchIndex)
	// 从 所欲的follower中找到一个最大的 并且超过半数的 commit index 给leader
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		if rf.LogMatched(newCommitIndex, rf.currentTerm) {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}
}

// 根据leader 的 commit 值, 更新 当前follower 的 commitindex , 确保 fo 的节点提交不落后于 leader
func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 更新 commitIndex 使其不超过 leaderCommit 和当前日志的长度
	// 1. leaderCommit 必须大于当前 commitIndex 才能推进
	// 2. 新的 commitIndex 不能超过当前日志的最大索引
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, rf.lm.LastIndex()) // 确保 commitIndex 不会超过当前日志的最后一个索引
		rf.applyCond.Signal()                                 // 通知 apply 进程处理提交的日志( 因为此时commitindex更新了, 可以去apply了, 所以唤醒
	}
}

// a dedicated applier goroutine to guarantee that 保证每个日志会被放到applych 一次
// 并且保证效果是并行的
// each log will be push into ApplyCh exactly once,
// ensuring that service's applying entries
// and raft's committing entries can be parallel
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// 如果没有必要 apply, 则阻塞, 通过信号量, todo 那你岂不是 这个锁就不会放开了??? 如果阻塞的话, 如果永远没有人来通知你的applier 启动,是否需要在之前释放锁?

		// 上一次 applied 的 应用快照的 索引大于 , 提交的, 说明此时需要 等待 apply信息, 即不需要apply现在 , 释放锁 ?

		for rf.lastApplied >= rf.commitIndex {
			// todo 多次的unlock一个锁,?? todo !!
			// rf.mu.Unlock()
			//应该把锁放开, 然后再等待继续执行
			rf.applyCond.Wait()
		}
		// rf.mu.Lock()
		firstIndex, commitIndex, lastApplied := rf.lm.FirstIndex(), rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.lm.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()

		// 对于每个要apply 的条目, 将其放入到applych中 用于异步的apply

		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// 直接使用中jian变量是因为 如果通过rf调用 ,index可能随着时间而改变
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		// 防止安装snapshot (强制性的) 导致 index回退, 选择更大的那个肯定没错, 因为已经applied 的状态无法倒退了

		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// Follwer 接收 Leader日志同步 处理

func (rf *Raft) AppendEntries(request *AppendEntriesArgs, response *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lm.GetEntry(rf.lm.FirstIndex()), rf.lm.GetEntry(rf.lm.LastIndex()), request, response)

	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

	rf.ChangeState(FOLLOWER)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// 若leader安装了snapshot，会出现rf.log.getFirstLog() > PrevLogIndex的情况。
	if request.PrevLogIndex < rf.lm.FirstIndex() {
		response.Term, response.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId, request.PrevLogIndex, rf.lm.GetEntry(rf.lm.FirstIndex()))
		return
	}
	// 判断PrevLog存不存在
	if !rf.LogMatched(request.PrevLogTerm, request.PrevLogIndex) {
		response.Term, response.Success = rf.currentTerm, false
		lastIndex := rf.lm.LastIndex()
		//1.  Follower 的 log 不够新，prevLogIndex 已经超出 log 长度
		if lastIndex < request.PrevLogIndex {
			response.XTerm, response.XIndex = -1, lastIndex+1
			// 2. Follower prevLogIndex 处存在 log
			// 向主节点上报信息，加速下次复制日志
			// 当PreLogTerm与当前日志的任期不匹配时，找出日志第一个不匹配任期的index
		} else {
			firstIndex := rf.lm.FirstIndex()
			response.XTerm = rf.lm.logs[request.PrevLogIndex-firstIndex].Term
			index := request.PrevLogIndex - 1
			for index >= firstIndex && rf.lm.logs[index-firstIndex].Term == response.XTerm {
				index--
			}
			response.XIndex = index
		}
		return
	}

	firstIndex := rf.lm.FirstIndex()
	// 找到第一个物理内存中的日志的index
	// 找到leader发来的所有 单独的日志

	for index, entry := range request.Entries {

		//检查 leader发来的所有的 日志, 他是否都在 当前follower 的 内存里边
		relativePosition := entry.Index - firstIndex
		// 如果 相对位置超过了 内存长度, 说明太长了, 说明leader需要覆盖一些 follower 的数据
		// 或者term不匹配,  说明日志冲突, 也是要覆盖 ,  此时需要裁剪并且添加新的 日志条目
		// 不仅要检查 index, 还要检查term ! 才能唯一的确定一个日志 !
		if relativePosition >= len(rf.lm.logs) || rf.lm.logs[relativePosition].Term != entry.Term {

			//rf.lm.logs = shrinkEntriesArray(append(rf.lm.logs[relativePosition], request.Entries[index:]...))
			//  找到需要覆盖的部分, 截取掉, 把后边不覆盖的部分继续append即可
			//rf.lm.logs = shrinkEntriesArray(rf.lm.logs[:relativePosition], entry.Index)
			//rf.lm.logs = mergeAndDereference(
			//	rf.lm.logs,
			//	request.Entries[index:],
			//)
			rf.lm.logs = shrinkEntries(append(rf.lm.logs[:entry.Index-firstIndex], request.Entries[index:]...))
			rf.persist()
			break
		}
	}

	// 通知上层可以apply主节点已经commit的日志。
	rf.advanceCommitIndexForFollower(request.LeaderCommit)

	response.Term, response.Success = rf.currentTerm, true
}

func (rf *Raft) ChangeState(state RaftState) {
	if rf.state == state {
		return
	}
	DPrintf("{Node %v} changes state from %v to %v", rf.me, rf.state, state)
	rf.state = state
	switch state {
	case FOLLOWER:
		rf.resetElectionTimer()
		rf.heartbeatTimer.Stop() // stop heartbeat
	case CANDIDATE:
	case LEADER:
		rf.electionTimer.Stop() // stop election
		rf.resetHeartbeatTimer()
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
