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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 根据Figure2来确定应该持久化的变量，对它们编码
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lm.logs)

	// raft快照中恢复时需要用到这两个，因此也持久化了
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()

	// 调用SaveRaftState()将编码后的字节数组传递给Persister
	rf.persister.SaveRaftState(data)

}

// GetState return currentTerm and whether this server believes it is the leader.
// 要求两个返回值哦
func (rf *Raft) GetState() (int, bool) {
	if rf.killed() {
		return -1, false
	}
	var term int
	var isleader bool

	rf.mu.Lock()
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()
	return term, isleader
}

//done

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
	// 将 Raft 的状态序列化为 []byte
	return []byte(fmt.Sprintf("%d:%d:%d", rf.currentTerm, rf.votedFor, rf.commitIndex))
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
	_, isLeader := rf.GetState()
	if !isLeader || rf.killed() {
		DPrintf("[%d] Start() Fail isleader = %t, isKilled = %t", rf.me, isLeader, rf.killed())
		return -1, -1, false
	}
	// Your code here (2B).
	//atomic.StoreInt32(&rf.applying, 1)
	rf.mu.Lock()
	logEntry := LogEntry{Command: command, Term: rf.currentTerm, Index: rf.lm.LastIndex() + 1}
	rf.lm.logs = append(rf.lm.logs, logEntry)
	rf.persist()
	rf.mu.Unlock()
	DPrintf("[%d] Start() ,index : %d,term : %d,command : %d", rf.me, logEntry.Index, logEntry.Term, command)

	return logEntry.Index, logEntry.Term, isLeader
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
		// candidate 有的选举timer , 存在每个candidate内部
		case <-rf.electionTimer.C:
			// 从选举 timer中去取消息, 这里的timer类似于一个chan
			//定时器一旦超时, 就会发送一个消息 到这个里边, 所以如果接受到消息, 就说明 选举超市了
			// 选举超时处理
			rf.mu.Lock()
			if rf.state != LEADER { // 只有 FOLLOWER 或 CANDIDATE 状态才需要重新选举
				// todo  对于candidate需要做什么? 并且锁的获取?
				// 内部不能再有锁的获取了

				rf.currentTerm += 1 // todo .... 你选举的时候忘记 +1了, 把自己的term

				rf.ChangeState(CANDIDATE)

				//
				rf.StartElection()
			}

			rf.resetElectionTimer()
			// todo 这里的锁应该释放呢, 因为还有不进入 election 的情况呢
			rf.mu.Unlock()
			// 重置选举计时器
			//rf.resetElectionTimer()

		case <-rf.heartbeatTimer.C:
			// 即 心跳超时,leader  此时需要重新发送 心跳 , leader独有的 心跳timer( 需要定期的发送心跳
			// 存在leader 的内部
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.BroadcastHeartbeat(true) // Leader 定期发送心跳
				// 重置心跳计时器
				rf.resetHeartbeatTimer()
			}
			rf.mu.Unlock()

		}
	}
}
func (rf *Raft) StartElection() {
	request := rf.getRequestVoteArgs()
	// c , 这个方法也不能加锁 !!
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, request)
	// use Closure
	grantedVotes := 1
	rf.votedFor = rf.me
	rf.persist()

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
			if rf.sendRequestVote(peer, &request, response) {
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
							rf.initLeader(LEADER)
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

//func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing RequestVote,  RequestVoteArgs %v and RequestVoteReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)
//	// Reply false if term < currentTerm(§5.1)
//	// if the term is same as currentTerm, and the votedFor is not null and not the candidateId, then reject the vote(§5.2)
//	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
//		reply.Term, reply.VoteGranted = rf.currentTerm, false
//		return
//	}
//
//	if args.Term > rf.currentTerm {
//		rf.ChangeState(FOLLOWER)
//		rf.currentTerm, rf.votedFor = args.Term, -1
//	}
//
//	// if candidate's log is not up-to-date, reject the vote(§5.4)
//	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
//		reply.Term, reply.VoteGranted = rf.currentTerm, false
//		return
//	}
//
//	rf.votedFor = args.CandidateId
//	rf.electionTimer.Reset(RandomElectionTimeout())
//	reply.Term, reply.VoteGranted = rf.currentTerm, true
//}

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

// 判断当前日志是否更新
func (rf *Raft) isLogUpToDate(lastLogTerm int, lastLogIndex int) bool {
	// 获取当前节点的最后一条日志的索引和 term
	currentLastLogIndex := rf.lm.LastIndex()
	currentLastLogTerm := rf.lm.GetEntry(currentLastLogIndex).Term

	// 比较日志的 term 和 index
	if lastLogTerm > currentLastLogTerm {
		// 请求中的日志 term 更大，说明更新
		return true
	} else if lastLogTerm == currentLastLogTerm {
		// 如果 term 相同，比较索引
		return lastLogIndex >= currentLastLogIndex
	}
	// 否则，日志较旧
	return false
}

// todo 可能是getentry 等方法上出了问题

//// 收到投票请求后的处理 handler
//func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
//	// 处理投票, 是要获取锁的 !
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	if args.Term >= rf.currentTerm {
//		// 至少任期持平才会 进行下一步
//		if args.Term > rf.currentTerm {
//			// 碰到比自己新的了, 更新自己的任期, 并且立刻将自己的状态更新为follower
//			//根据 paper的设计, 别人已经比你更新了, 你就别candidate
//
//			rf.mu.Lock()
//			rf.enterNewTerm(args.Term) // 应该是需要一个atomic的操作
//			// todo 这个方法加锁不?
//			rf.mu.Unlock()
//
//			// 并且一些投票操作,  需要在新的任期里边做
//		}
//		if rf.state != CANDIDATE { // 如果不是候选人 就重置选举计时
//			// 因为此时接收到一个 选举请求了, 所以重置自己的计时器即可, 你不用选了, 有比你大的人在选了
//
//			rf.resetElectionTimer()
//		}
//		rf.mu.Lock()
//		reply.Term = rf.currentTerm // 构造reply
//		reply.VoteGranted = true
//		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
//			// 表示已经投过票给当前的人 , 或者-1表示还没投给任何人
//			// 安全性检查
//			len := rf.lm.len() // 日志的逻辑长度, 因为有一部分可能已经到 快照里边了
//
//			var lastLogIndex, lastLogTerm int
//			if len == rf.lm.lastTrimmedIndex {
//				// 如果 日志逻辑 长度 = trimmed index, 表示所有的日志都在 快照里边
//				lastLogIndex = rf.lm.lastTrimmedIndex
//				lastLogTerm = rf.lm.lastTrimmedTerm
//
//			} else {
//				lastLogIndex = len
//				lastLogTerm = rf.lm.GetEntry(len).Term
//			}
//			if lastLogTerm == 0 || lastLogTerm < args.LastLogTerm {
//				//在term 初始为0, 表示自己刚启动,
//				//或者自己的最后一个日志的 term小于 请求者的 term就投票
//				reply.VoteGranted = true
//				rf.votedFor = args.CandidateId
//				// 之前更新的是节点的term , 这里是最后一个log 的term
//			} else if lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex {
//				// 在最后一个日志的 任期相同的情况下, 比较 logindex , 他比我的新, 投
//				reply.VoteGranted = true
//				rf.votedFor = args.CandidateId
//			}
//		}
//		// 将其放在锁的内部比较好, 因为他是不受锁的保护的
//		go rf.persist() // 将 一些信息 持久化 , 如当前的term和voted for
//		rf.mu.Unlock()
//	}
//
//}

// done
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
func (rf *Raft) getRequestVoteArgs() RequestVoteArgs {
	//rf.mu.Lock()
	//defer rf.mu.Unlock() // 需要锁
	// 锁锁锁
	// 构造 RequestVoteArgs 并返回
	args := RequestVoteArgs{
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
	LeaderId     int         // 即自己的id
	PrevLogIndex int         // 前一个日志的日志号
	PrevLogTerm  int         //
	Entries      []*LogEntry //  这是一个数组存放的是指向日志体的指针,
	LeaderCommit int         //leader已经提交的日志 数
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

// 初始化leader, 一些leader才有的属性
// 这里不用锁, 就要确保外部 调用的时候有锁 !
func (rf *Raft) initLeader(RaftState) {
	// 因为要访问 shared

	rf.state = LEADER

	// Initialize nextIndex and matchIndex for each follower
	rf.nextIndexes = make([]int, len(rf.peers))
	rf.matchIndexes = make([]int, len(rf.peers))

	// Set nextIndex to the index after the last log entry
	lastLogIndex := rf.getLastLogIndex()

	//更新两个数组, 反正后续会通过 心跳和一致性检查 进行维护的, 根据follower 的进度
	for i := range rf.peers {
		rf.nextIndexes[i] = lastLogIndex + 1
		rf.matchIndexes[i] = 0
	}

	// 重置 心跳
	if rf.heartbeatTimer != nil {
		rf.heartbeatTimer.Stop()
	}

	if rf.electionTimer != nil {
		rf.electionTimer.Stop() // Stop election timer for the leader
	}

	// rf.resetHeartbeatTimer()

	// 别忘记leader开始发送心跳,不过不是这个函数的功能
}

func (rf *Raft) resetElectionTimer() {
	timeout := randomElectionTimeout()
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(timeout)
	} else {
		rf.electionTimer.Reset(timeout)
	}
}

// 重置 心跳
func (rf *Raft) resetHeartbeatTimer() {
	if rf.heartbeatTimer == nil {
		rf.heartbeatTimer = time.NewTimer(50 * time.Millisecond)
	} else {
		rf.heartbeatTimer.Reset(50 * time.Millisecond)
	}
}

// 生成一个随机的选举超时时间（例如 150 到 300 毫秒之间）
func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
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
		nextIndexes:       make([]int, len(peers)),
		matchIndexes:      make([]int, len(peers)),
		heartbeatTimer:    time.NewTimer(50 * time.Millisecond),   // 固定心跳, 时间是多少来着, 10s ?,,
		electionTimer:     time.NewTimer(randomElectionTimeout()), // 随机 选举时长,todo时长还没确定
		lastIncludedIndex: 0,
		lastIncludedTerm:  -1,
	}
	// 在 崩溃后重新读取信息, 从持久化的介质
	// 之前没有产生锁, 此方法内部需要 锁来保持 磁盘中的 准确读取到 当前!
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	// 需要开共len(peer) - 1 个线程replicator，分别管理对应 peer 的复制状态
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
		rf.replicateOneRound(peer)
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
			go rf.replicateOneRound(peer)
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
func (rf *Raft) sendInstallSnapshot(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) bool {
	// 假设发送请求到 peer
	return rf.peers[peer].Call("Raft.InstallSnapshot", request, response)
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
	rf.electionTimer.Reset(randomElectionTimeout())

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
		relative := lastIncludedIndex - rf.lm.LastIndex()

		rf.lm.logs = shrinkEntriesArray(rf.lm.logs, relative)
		// 表示裁剪了, 是不是得更新 lastincluded index ?

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
	prevLogTerm := rf.lm.GetEntry(prevLogIndex).Term

	// 获取 LogEntry 的指针切片
	entries := make([]*LogEntry, 0, len(rf.lm.logs)) // 初始容量优化
	for i := prevLogIndex + 1; i <= rf.lm.LastIndex(); i++ {
		entries = append(entries, &rf.lm.logs[i-rf.lm.FirstIndex()])
	}
	// 创建AppendEntriesArgs对象
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

// Leader 向Follwer 发送复制请求, 根据fo 的进度, 决定是通过 安装快照还是 追加条目来同步日志
// 同步有两种方法, 一个是 apply快照, 另一个是 追加日志

// replicatooneround 实现有问题

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != LEADER {
		// 只有leader能调用
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndexes[peer] - 1
	// 计算当前 fo 的 日志index
	// 如果小于 leader 的第一个日志, 说明你差的也太多了
	//  那么说明日志条目无法直接同步, 只能通过快照
	if prevLogIndex < rf.lm.FirstIndex() {
		// only snapshot can catch upetFirstLog().Index {

		//产生快照请求
		request := rf.genInstallSnapshotRequest()
		rf.mu.RUnlock()
		response := new(InstallSnapshotResponse)
		if rf.sendInstallSnapshot(peer, request, response) {
			//发送成功, 表示会有一个response, 此时接受response 并进行处理( leader

			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response)
			rf.mu.Unlock()
		}
	} else { // 否则  不通过快照, 而是通过 appendentries

		request := rf.genAppendEntriesRequest(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			//处理送回的 response ,可能涉及多轮的 日志追加
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
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

// advanceCommitIndexForLeader 推进 leader  的 commitindex, 确定哪些日志 可以提交和应用到状态机(多半数提交
// 更新 commitindex给leader( 需要一个手段去通知leader, 自己的提交的index , 来帮助 leader判断一些半数情况
// 根据 follower 的进度来更新leader 中的matchindex , 并且使得leader 来 确定哪些 日志可以提交, 即更新leader 的 commit index
// leader 的commitindex是 半数以上的结果, 需要去访问所有的节点达到共识
//
//	func (rf *Raft) advanceCommitIndexForLeader() {
//		// 获取当前日志的最后一个索引
//		lastIndex := rf.lm.LastIndex()
//
//		// 计算大多数节点需要同步的 commitIndex
//		for i := lastIndex; i >= rf.lm.FirstIndex(); i-- {
//			// 获取该日志条目的任期
//			term := rf.lm.GetEntry(i).Term
//
//			// 计算大多数节点的 matchIndex
//			count := 0
//			for peer := range rf.peers {
//				if rf.matchIndexes[peer] >= i && rf.lm.GetEntry(i).Term == term {
//					count++
//				}
//			}
//
//			// 如果大多数节点的 matchIndex 大于或等于 i，则更新 commitIndex
//			if count > len(rf.peers)/2 && rf.lm.GetEntry(i).Term == rf.currentTerm {
//				rf.commitIndex = i
//				break
//			}
//		}
//
//		// 打印调试信息
//		DPrintf("{Node %v} advanced commitIndex to %v", rf.me, rf.commitIndex)
//	}
func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndexes)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndexes)
	sort.Ints(sortMatchIndex)
	// get the index of the log entry with the highest index that is known to be replicated on a majority of servers
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		if rf.lm.matchLog(newCommitIndex, rf.currentTerm) {
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

//// 每一个fo都有一个这个, 和他对应
//func (rf *Raft) replicator(peer int) {
//	for {
//		// 获取追随者需要同步的日志信息
//		prevLogIndex := rf.nextIndexes[peer] - 1
//		prevLogTerm := rf.lm.GetTerm(prevLogIndex)
//		entries := rf.lm.GetEntriesFromIndex(rf.nextIndexes[peer])
//
//		// 构造请求
//		args := &AppendEntriesArgs{
//			Term:         rf.currentTerm,
//			LeaderID:     rf.me,
//			PrevLogIndex: prevLogIndex,
//			PrevLogTerm:  prevLogTerm,
//			Entries:      entries,
//			LeaderCommit: rf.commitIndex,
//		}
//
//		// 调用 AppendEntries 方法，实际处理在 follower 端
//		var reply AppendEntriesReply
//		rf.RequestAppendEntries(args, &reply)
//
//		// 根据 reply 更新追随者日志状态
//		if reply.Success {
//			rf.nextIndexes[peer] = len(rf.lm.logs)              // 更新下一个复制的日志位置
//			rf.matchIndexes[peer] = prevLogIndex + len(entries) // 更新已匹配的日志位置
//		} else {
//			// 如果复制失败，调整 nextIndex 重新复制
//			rf.adjustNextIndex(peer)
//		}
//	}
//}
//// Leader处理Follower 日志复制响应
//func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesArgs, response *AppendEntriesReply) {
//	if rf.state == StateLeader && rf.currentTerm == request.Term {
//		if response.Success {
//			rf.matchIndexes[peer] = request.PrevLogIndex + len(request.Entries)
//			rf.nextIndexes[peer] = rf.matchIndexes[peer] + 1
//			rf.advanceCommitIndexForLeader()
//		} else {
//			if response.Term > rf.currentTerm {
//				rf.ChangeState(StateFollower)
//				rf.currentTerm, rf.votedFor = response.Term, -1
//				rf.persist()
//			} else if response.Term == rf.currentTerm {
//				rf.nextIndex[peer] = response.ConflictIndex
//				if response.ConflictTerm != -1 {
//					firstIndex := rf.getFirstLog().Index
//					for i := request.PrevLogIndex; i >= firstIndex; i-- {
//						if rf.logs[i-firstIndex].Term == response.ConflictTerm {
//							rf.nextIndex[peer] = i + 1
//							break
//						}
//					}
//				}
//			}
//		}
//	}
//	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling AppendEntriesResponse %v for AppendEntriesRequest %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), response, request)
//}

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
	rf.electionTimer.Reset(randomElectionTimeout())

	// 若leader安装了snapshot，会出现rf.log.getFirstLog() > PrevLogIndex的情况。
	if request.PrevLogIndex < rf.lm.FirstIndex() {
		response.Term, response.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId, request.PrevLogIndex, rf.lm.GetEntry(rf.lm.FirstIndex()))
		return
	}
	// 判断PrevLog存不存在
	if !rf.lm.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
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
			rf.lm.logs = shrinkEntriesArray(rf.lm.logs[:relativePosition], entry.Index)
			rf.lm.logs = mergeAndDereference(
				rf.lm.logs,
				request.Entries[index:],
			)
			break
		}
	}

	// 通知上层可以apply主节点已经commit的日志。
	rf.advanceCommitIndexForFollower(request.LeaderCommit)

	response.Term, response.Success = rf.currentTerm, true
}

// ChangeState 这里不能用锁, 确保外部 调用的时候有锁
// ChangeState sets the node's state to the given state (e.g., FOLLOWER).
func (rf *Raft) ChangeState(newState RaftState) {
	//rf.mu.Lock()         // 锁定以确保线程安全
	//defer rf.mu.Unlock() // 确保在函数返回时解锁

	// 如果当前状态已经是目标状态，则不做任何操作

	if rf.state == RaftState(newState) {
		return
	}

	// 设置新的状态
	rf.state = RaftState(newState)

	// 如果目标状态是 FOLLOWER
	if rf.state == FOLLOWER {
		// 1. 重置选举计时器
		rf.resetElectionTimer()

		// 2. 清除投票信息，表示该节点不再参与当前选举
		rf.votedFor = -1

	}
	//todo candidate 的内容
	//其他的状态, 如 leader和leader 的初始化可以写在这里, initleader 等

}

//// AppendEntries  follower 使用, 用来 处理日志同步( 即leader发来的请求 对xTerm等 进行一个操作 ,
//// 不断的处理 leader寻找 缺失日志位置的情况 ( 什么情况下需要发送 xterm? 即你 拒绝了 leader请求的时候 actually the
//// requesthandler here
//// 承担了处理日志同步和日志复制( 追加) 等两个核心的功能
//func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
//	reply.Term = rf.currentTerm
//	if args.Term < rf.currentTerm { // 说明这个leader已经过期
//		reply.Success = false
//		return
//	}
//	// 进入新的一个Term，更新( 如果 发过来的人 的 term更新的话, 自己肯定不进行选举,  遂reset选举计时器
//	if args.Term > rf.currentTerm {
//		rf.enterNewTerm(args.Term) // 改变votedFor
//	} else {
//		rf.state = FOLLOWER // 不改变votedFor
//	}
//	rf.resetElectionTimer() // 刷新选举计时
//	rf.mu.Lock()
//	// 因为需要访问共享的变量, 所以 锁到结束为止
//	defer rf.mu.Unlock()
//
//	if args.PrevLogIndex < rf.commitIndex || rf.findLastLogIndexOfTerm(args.Term) > args.PrevLogIndex {
//		// 说明这个请求滞后了 , 竟然比当前fo节点还靠后, 回复失败
//		reply.XLen = rf.lm.len()
//		reply.XTerm = -1
//		reply.Success = false
//		return
//	}
//	// 一致性检查, 看是否日志冲突?  如果
//	if args.PrevLogIndex > rf.lm.len() {
//		reply.XLen = rf.lm.len()
//		reply.XTerm = -1
//		reply.Success = false
//		return
//	}
//	// 如果不在一个 term内, 表示冲突, 返回false
//	if args.PrevLogIndex >= 1 && args.PrevLogIndex > rf.lm.lastTrimmedIndex { // 如果prevIndex已经被裁剪了，那一定不冲突
//		if rf.lm.GetEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
//			// 有冲突了, 此时 需要进行一致性的检查, 连续的 fo和leader之间的确认,所以需要 将 xterm 和xidnex 返回
//			// 因为使用的是快速定位, fastbackup, 定位先以term为单位, 找到term后, 以index为单位
//
//			reply.XTerm = rf.lm.GetEntry(args.PrevLogIndex).Term
//			reply.XIndex = rf.findFirstLogIndexOfTerm(reply.XTerm)
//			reply.Success = false
//			return
//		}
//	}
//	// 接下来就是接受日志, 并且覆盖
//
//	// 这里有可能leader传来的一部分log已经裁掉了，需要过滤一下
//
//	// 确定当前的 起始index , 谁靠后一点取谁
//	// 避免处理掉已经 裁剪掉的日志( 注意leader 的prelogindex即 这个日志体前一个日志的位置, 即提示你更新的位置
//
//	from := max(args.PrevLogIndex+1, rf.lm.lastTrimmedIndex+1)
//
//	// 确定需要过滤的 日志的个数, 并且不能越界
//	// from位置之前的是需要过滤的, 画个图很好理解
//	filter := min(from-args.PrevLogIndex-1, len(args.Entries)) // 防止越界,
//	args.Entries = args.Entries[filter:]
//	rf.lm.appendFrom(from, args.Entries) // 强制追加（覆盖）日志
//
//	// 提交log, 更新自己的 提交index 为 leader 提交的index 或者 本地的长度
//	// 因为这个长度是随着 term更新, 所以需要 判断一下哪个 长
//	//目前还在 lm里边的日志 , 即没有 持久apply 的日志 ,可能比 leader 的要长
//	if args.LeaderCommit > rf.commitIndex {
//		rf.commitIndex = min(args.LeaderCommit, rf.lm.len())
//	}
//	rf.applyCond.Signal() // 唤醒异步apply, 真正的将东西 本地化
//
//	rf.persist() // 将一些 信息持久化
//	reply.Success = true
//}

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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

//func (rf *Raft) persist() {
//	// Your code here (3C).
//	// Example:
//	// w := new(bytes.Buffer)
//	// e := labgob.NewEncoder(w)
//	// e.Encode(rf.xxx)
//	// e.Encode(rf.yyy)
//	// raftstate := w.Bytes()
//	// rf.persister.Save(raftstate, nil)
//
//	// 注意, 应该是需要锁的
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	w := new(bytes.Buffer)
//	e := labgob.NewEncoder(w) // 创建一个新的写入 的encoder
//	e.Encode(rf.currentTerm)
//	e.Encode(rf.votedFor)
//	e.Encode(rf.lm) // 持久化日志
//	// 这里并不 持久日志,只是一些基本的数据, 如leader才有的或者 fo 的投票给谁的一个数据
//	raftstate := w.Bytes() //将缓冲区的东西写到 机器
//	// 返回结果
//	// 将raft 结果 保存
//	rf.persister.Save(raftstate, rf.snapshot)
//}

// restore previously persisted state.

//
//func (rf *Raft) sendHeartbeats() {
//	total := len(rf.peers)
//	args := AppendEntriesArgs{}
//	args.Term = rf.currentTerm
//	args.LeaderId = rf.me
//	args.LeaderCommit = rf.commitIndex
//	// 和论文设计的一样, follower会检查prev 的index和 term是否对的上? 对的上说明我们同步
//	//对不上, 说明需要同步, 需要找到到底哪个开始缺了, 一致性检查
//	// 注意 边界条件判断, 即 leader当前 的两prev 的取值如何? 逻辑地址? lastTrimmedIndex ?
//	if rf.lm.len() >= 1 {
//		// len返回的是逻辑长度 , 而不是实际长度, 实际长度通过 len logs返回哦
//		args.PrevLogIndex = rf.lm.len()
//		if args.PrevLogIndex > rf.lm.lastTrimmedIndex {
//			// 表示内存里边有日志的
//			args.PrevLogTerm = rf.lm.LastTerm()
//		} else { // 否则说明 已有的都被 放到snapshot里边了, 直接用trim 的即可
//
//			args.PrevLogTerm = rf.lm.lastTrimmedTerm
//			args.PrevLogIndex = rf.lm.lastTrimmedIndex
//		}
//
//	}
//
//	rf.mu.Lock()
//	// 如果当前没被kill并且还是leader , 就继续发送心跳
//	for i := 0; !rf.killed() && i < total && rf.state == LEADER; i++ {
//		if i != rf.me { // 发送心跳给所有的, 通过routine 并发的发
//			go func(server int) {
//				reply := AppendEntriesReply{} // 准备构造 返回结果
//				if reply.XTerm > rf.currentTerm {
//					//心跳实际是 一个双向交通信息的过程
//					// 其他节点必自己高, 自己就变为follower ,更新自己的term
//					rf.enterNewTerm(reply.Term)
//					rf.resetElectionTimer() // 重置选举器
//					return
//				}
//
//				if !reply.Success {
//					//此时说明出现了日志的不一致 ,先 快速备份第一次, 找到 下一个term位置, 进行一次的
//					rf.fastBackup(server, reply)             // 快速恢复
//					rf.agreement(server, rf.lm.len(), false) //同步的过程
//					//从最新的位置检查一致性
//				} else {
//					// 更新一致的位置, 维护 数组
//					rf.mu.Lock()
//					rf.nextIndexes[server] = args.PrevLogIndex + 1 // 这个fo下一个index的索引位置
//					rf.matchIndexes[server] = args.PrevLogIndex    // 当前fo和自己同步的位置
//					rf.mu.Unlock()
//					rf.tryCommit() // 每次心跳后检查都能进行提交
//				}
//			}(i) // 遍历每一个发送的对象, 即server peer
//
//		}
//	}
//
//}

// 快速的定位到不一致的term和index, 并且重置 leader中维护的 每个fo对应的index的数组
// 可能需要多次调用 这个 方法去找到 最终的term
//剩下的同步任务交给 aggrement去做
//
//func (rf *Raft) fastBackup(server int, reply AppendEntriesReply) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock() // 因为需要访问rf节点的 leader特有的 记录 每个 follower 的nextIndex和 matchIndex 数组
//	// 需要更新并发访问的变量, 所以需要锁
//
//	if reply.XTerm == -1 {
//		// follower对应的index , 即 leader想哟啊检查的位置 位置没有log
//		//
//		rf.nextIndexes[server] = reply.XLen + 1
//
//	} else if reply.XTerm == 0 { // 某个地方出问题了，执行到这里不应该是temr 0
//		// 将 leader中的 这个 fo对应的索引位置 设置为1 肯定没错
//		rf.nextIndexes[server] = 1
//	} else {
//		// 这种情况, 表示 fo有冲突的term, 这算是锁定到term目标了
//		//前边 的情况, 表示还没锁定到对应的term , 注意这个方法是 leader调用的
//
//		//找到当前日期的最后一个日志的index, 这个即我们要开始更新的位置 !
//		lastOfTerm := rf.findLastLogIndexOfTerm(reply.XTerm)
//		if lastOfTerm == -1 { // leader没有follower的Term
//			rf.nextIndexes[server] = reply.XIndex
//		} else { //否则说明 leader有这个term 的日志, 标记 在index数组里边即可
//			//至此我们知道了这个fo 下一个日志位置
//			rf.nextIndexes[server] = lastOfTerm + 1
//		}
//	}
//}

//
////
//
//func (rf *Raft) agreement(server int, nextIndex int, isHeartbeat bool) {
//	for {
//		rf.mu.Lock()
//		args := AppendEntriesArgs{
//			Term:         rf.currentTerm,
//			LeaderId:     rf.me,
//			PrevLogIndex: nextIndex - 1,
//			PrevLogTerm:  rf.lm.GetEntry(nextIndex - 1).Term,
//			Entries:      rf.lm.getEntriesFrom(nextIndex),
//			LeaderCommit: rf.commitIndex,
//		}
//		rf.mu.Unlock()
//
//		reply := AppendEntriesReply{}
//		if !rf.sendAppendEntries(server, &args, &reply) {
//			break // 如果 RPC 失败，则退出
//		}
//
//		if reply.Success {
//			rf.mu.Lock()
//			rf.nextIndexes[server] = nextIndex + len(args.Entries)
//			rf.matchIndexes[server] = nextIndex + len(args.Entries) - 1
//			rf.mu.Unlock()
//			rf.tryCommit()
//			break // 日志同步成功，退出循环
//		} else {
//			rf.fastBackup(server, reply)
//			nextIndex = rf.nextIndexes[server] // 更新 nextIndex
//		}
//	}
//}

//	rf.mu.Lock()
//
//	// 构造 AppendEntriesArgs
//	args := AppendEntriesArgs{
//		Term:         rf.currentTerm,
//		LeaderId:     rf.me,
//		PrevLogIndex: nextIndex - 1,
//		PrevLogTerm:  rf.lm.GetEntry(nextIndex - 1).Term, // 获取前一个日志条目的任期
//		Entries:      rf.lm.getEntriesFrom(nextIndex),    // 从 nextIndex 开始的所有日志条目
//		LeaderCommit: rf.commitIndex,
//	}
//
//	rf.mu.Unlock()
//
//	// 发送 AppendEntries RPC
//	reply := AppendEntriesReply{}
//
//	if rf.sendAppendEntries(server, &args, &reply) {
//		if reply.Success {
//			// 更新一致的位置
//			rf.mu.Lock()
//			rf.nextIndexes[server] = nextIndex + len(args.Entries)      // 更新为下一个索引
//			rf.matchIndexes[server] = nextIndex + len(args.Entries) - 1 // 更新 matchIndexes
//			rf.mu.Unlock()
//			rf.tryCommit() // 尝试提交日志
//		} else {
//			// 如果失败，说明还有不一致，调用 fastBackup
//			rf.fastBackup(server, reply)
//			// 再次调用 agreement，递归直到日志同步, 这里的递归可能 处理不当, 如果缺少很多日志
//			//可能导致 栈的溢出, 考虑使用for代替
//
//			rf.agreement(server, rf.nextIndexes[server], isHeartbeat)
//		}
//	}

// 创建一个快照， 包含了 截取到指定的index 的所有信息， 服务不再包含该索引之前的所有日志
// raft 尽可能快的trim这些日志

// trim, 实际实现的是shrink array 的任务 ...
//func (rf *Raft) trim(index int) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	// 确保 trim 的索引是合法的
//	if index <= rf.lm.lastTrimmedIndex {
//		return // 如果裁剪索引已经被裁剪过，直接返回
//	}
//
//	// 裁剪日志
//	relativeIndex := index - rf.lm.lastTrimmedIndex - 1
//	if relativeIndex >= len(rf.lm.logs) {
//		// 如果要裁剪的范围大于日志数组，清空日志
//		rf.lm.logs = nil
//	} else {
//		// 从裁剪点之后的日志开始保留
//		rf.lm.logs = rf.lm.logs[relativeIndex:]
//	}
//
//	// 更新最后裁剪的索引
//	rf.lm.lastTrimmedIndex = index
//
//	// 持久化裁剪后的状态
//	rf.persist()
//}

//// 超时选举实现
//func (rf *Raft) kickOffElection() {
//	rf.currentTerm++
//	rf.state = CANDIDATE
//	rf.votedFor = rf.me // candidate id
//	rf.persist()        // 将当前的持久化状态持久化 ! 防止宕机导致的不一致
//	votes := 1
//	total := len(rf.peers)
//	args := rf.getRequestVoteArgs() // 获取 请求的content  , 即创建请求的方法
//	mu := sync.Mutex{}
//	for i := 0; i < total; i++ {
//		//
//		if i == rf.me {
//			continue
//		}
//		// 使用goroutine去 投票, 并发的,
//		// 将 server传入, 表示是发给 谁的 ? 对应参数i,  即每个 server都会给你发一个则更好
//		go func(server int) {
//			reply := RequestVoteReply{}
//			if !rf.sendRequestVote(server, &args, &reply) {
//				return
//			}
//			if args.Term == rf.currentTerm && rf.state == CANDIDATE {
//				// 判断防止自己身份已经改变, 并且判断term 是否和自己是一个
//				if reply.Term < rf.currentTerm {
//					// 投票者日期小于自己, 丢弃
//					return
//				}
//				if reply.VoteGranted {
//					//如果确认给自己投票
//					mu.Lock()
//					votes++
//					if rf.state != LEADER && votes > total/2 {
//						// 超过半数, 并且当前不是leader  !
//						rf.initLeader() // 初始化一些leader 才有的信息
//						rf.state = LEADER
//						// 立刻开始心跳, 告诉所有的 节点我是leader了
//						go rf.BroadcastHeartbeat(true)
//					}
//
//					mu.Unlock()
//				}
//			}
//		}(i)
//
//	}
//}

//
//// leader统计matchIndex，尝试提交 , 提交到本地
//func (rf *Raft) tryCommit() {
//	total := len(rf.peers)
//	// 找到一个最大的N>commitIndex，使得超过半数的follower的matchIndex大于等于N，
//	// 且leader自己N位置的log的Term等于当前Term   （这一点很重要，安全性问题中提过, 否则的话需要提交一个空日志来确保最新的要提交的日志是自己的term
//	// 那么N的位置就可以提交
//
//	len := rf.lm.len()
//	for N := rf.commitIndex + 1; N <= len; N++ { // 遍历所有的 当前commit 的index , 去找到第一个能提交的 index , 即满足半数
//		majorityCnt := 1 // 记录是否超过半数
//		for _, matchIndex := range rf.matchIndexes {
//			if matchIndex >= N && rf.lm.logs[N].Term == rf.currentTerm { // 判断是否当前任期 并且是否 对应的  节点有超过 当前的 index
//				// 注意设计的matchindex 和next Index 的区别, 前者是提交的后者是发送copy 对应位置
//				majorityCnt++
//			}
//		}
//		if majorityCnt > total/2 {
//			rf.commitIndex = N // 更新提交Index
//		}
//	}
//	rf.mu.Lock()
//	rf.applyCond.Signal() // 通过channel 唤醒 异步的apply , 之前一直被阻塞
//	// 阻塞也是
//
//	rf.mu.Unlock()
//}

//func (rf *Raft) apply() {
//	// 先apply快照
//	if rf.lm.lastTrimmedIndex != 0 {
//		rf.applySnapshot()
//	}
//	rf.backupApplied = true // 已经
//	// 然后再apply剩余log
//	for !rf.killed() {
//		// 循环的去检查, 但是不意味着一直循环, 而是 等待唤醒而阻塞
//		// >=意味着没有 可以 apply 的snapshot
//		for rf.lastApplied >= rf.commitIndex {
//			// 每次休眠前先看有无快照可apply
//			select {
//			case index := <-rf.installSnapCh: // 获取快照的信息
//
//				// 这里获取到的就是apply 中的index, 即快照截止到了哪个索引, 如果有快照需要 apply, 就截断日志, 表示逻辑上的日志没了
//
//				//并调用方法去apply
//				// 这两个操作要保证原子性
//				rf.trim(index)
//				// 去截断相关的日志,
//				// 产生快照, 因为这个时候已经确定日志将会在快照里边, 所以 可以放心
//
//				rf.applySnapshot()
//			default: // 即没有快照需要更新, 就直接阻塞,
//				//这里的设计是每一个 raft只有一个apply协程  !
//			}
//			rf.mu.Lock()
//			rf.applyCond.Wait() // 等待别处唤醒去apply，避免了并发冲突
//			rf.mu.Unlock()
//		}
//
//		rf.mu.Lock()
//		// commitIndex领先了, 截取到commitindex , 表示本轮要 apply的scope
//		applyIndex := rf.lastApplied + 1
//		commitIndex := rf.commitIndex
//		entries := rf.lm.split(applyIndex, commitIndex+1)
//		rf.mu.Unlock()
//		// 这里用不到索引, 所以爆红
//		for _, log := range entries {
//			if applyIndex <= rf.lm.lastTrimmedIndex {
//				// applyIndex落后快照了, 表示有错误了哇, 怎么要应用的比截取的还小, 退出循环
//				break
//			}
//			msg := ApplyMsg{
//				CommandValid: true,
//				Command:      log.Command,
//				CommandIndex: applyIndex,
//				CommandTerm:  log.Term, // 为了Lab3加的
//			}
//			rf.applyCh <- msg // 消息放入 applych
//
//			rf.mu.Lock()
//			if rf.lastApplied > applyIndex { // 说明snapshot中已经覆盖了 当前的日志
//				// 退出循环( 这里需要原子操作! 来保证共享的 访问
//				rf.mu.Unlock()
//				break
//			}
//			rf.lastApplied = applyIndex
//			applyIndex++
//			rf.mu.Unlock()
//		}
//	}
//}
