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
	if index <= snapshotIndex || index > rf.getLastLogIndex() {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	// trim 目录， 移除快照之前的 日志条目
	// index - 第一个内存中的index 才是 logs数组中的索引
	//rf.lm.logs = shrinkEntriesArray(rf.lm.logs, index) // 结果重新赋值给 lm的logs

	// 别忘记更新 lastincluded  index !
	//var log []LogEntry
	//for i := index + 1; i <= rf.getLastLogIndex(); i++ {
	//	log = append(log, rf.lm.GetEntry(i))
	//}
	rf.lm.logs = shrinkEntries(rf.lm.logs[index-snapshotIndex:])
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
		mu: sync.RWMutex{},
		// todo,你初始化竟然没有带锁? ...
		peers:          peers,
		persister:      persister,
		commitIndex:    0,
		lastApplied:    0, //.. 初始化没全, 测
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
	//  正确的保持状态> rf.persist()
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

func (rf *Raft) ChangeState(state RaftState) {
	if rf.state == state {
		return
	}
	DPrintf("{Node %v} changes state from %v to %v", rf.me, rf.state, state)
	rf.state = state
	switch state {
	case FOLLOWER:
		rf.resetElectionTimer()
		rf.heartbeatTimer.Stop() // 停止心跳
	case CANDIDATE:
	case LEADER:
		rf.electionTimer.Stop() // 停止选举
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
