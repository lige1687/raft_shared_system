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
	"log"
	"math/rand"

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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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

	commitIndex int           // 已提交的最大下标
	lastApplied int           // 以应用到状态机的最大下标( 提交和应用没是两回事
	applyCh     chan ApplyMsg // apply通道 , 用于和peer交流
	applyCond   *sync.Cond    // apply协程唤醒条件 ,应该根据 mu 创建 , todo apply携程实现

	installSnapCh chan int    // install snapshot的信号通道，传入trim index
	backupApplied bool        // 从磁盘恢复的snapshot已经apply
	notTicking    chan bool   // 没有进行选举计时
	electionTimer *time.Timer // 选举计时器

	// Leader特有字段
	nextIndexes    []int       // 对于每个follower，leader要发送的下一个复制日志下标
	matchIndexes   []int       // 已知每个follower和自己一致的最大日志下标
	heartbeatTimer *time.Timer // 心跳计时器

	stateMachine *StateMachine
}

// GetState return currentTerm and whether this server believes it is the leader.

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()         // 加锁以确保线程安全
	defer rf.mu.Unlock() // 解锁

	// 获取当前任期
	term := rf.currentTerm

	// 判断是否是领导者
	isleader := (rf.state == LEADER)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

func (rf *Raft) persist() {
	// 加锁以确保线程安全
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 创建一个缓冲区
	w := new(bytes.Buffer)
	// 使用 labgob 创建一个编码器
	e := labgob.NewEncoder(w)

	// 持久化关键状态
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.lm) != nil {
		log.Fatalf("Failed to encode Raft state")
	}

	// 将缓冲区转换为字节数组
	raftstate := w.Bytes()

	// 将持久化状态保存到存储器中
	// 如果未实现快照，传递 nil 作为第二个参数 , 将 应用交给persister做
	rf.persister.Save(raftstate, rf.snapshot)
}

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
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // 如果没有持久化数据，直接返回
		return
	}

	r := bytes.NewBuffer(data) // 创建一个读取缓冲区
	d := labgob.NewDecoder(r)  // 创建解码器

	// 定义临时变量用于接收解码数据
	var currentTerm int
	var votedFor int
	var lm LogManager

	// 解码持久化数据
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lm) != nil {
		log.Fatalf("Failed to decode persisted state") // 解码失败，记录错误日志并退出
	}

	// 加锁保护 Raft 状态
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 恢复 Raft 的状态
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lm = &lm
}

// todo heartbeat
func (rf *Raft) sendHeartbeats() {
	total := len(rf.peers)
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	// 和论文设计的一样, follower会检查prev 的index和 term是否对的上? 对的上说明我们同步
	//对不上, 说明需要同步, 需要找到到底哪个开始缺了, 一致性检查
	// 注意 边界条件判断, 即 leader当前 的两prev 的取值如何? 逻辑地址? lastTrimmedIndex ?
	if rf.lm.len() >= 1 {
		// len返回的是逻辑长度 , 而不是实际长度, 实际长度通过 len logs返回哦
		args.PrevLogIndex = rf.lm.len()
		if args.PrevLogIndex > rf.lm.lastTrimmedIndex {
			// 表示内存里边有日志的
			args.PrevLogTerm = rf.lm.LastTerm()
		} else { // 否则说明 已有的都被 放到snapshot里边了, 直接用trim 的即可

			args.PrevLogTerm = rf.lm.lastTrimmedTerm
			args.PrevLogIndex = rf.lm.lastTrimmedIndex
		}

	}

	rf.mu.Lock()
	// 如果当前没被kill并且还是leader , 就继续发送心跳
	for i := 0; !rf.killed() && i < total && rf.state == LEADER; i++ {
		if i != rf.me { // 发送心跳给所有的, 通过routine 并发的发
			go func(server int) {
				reply := AppendEntriesReply{} // 准备构造 返回结果
				if reply.XTerm > rf.currentTerm {
					//心跳实际是 一个双向交通信息的过程
					// 其他节点必自己高, 自己就变为follower ,更新自己的term
					rf.enterNewTerm(reply.Term)
					rf.resetElectionTimer() // 重置选举器
					return
				}
				// todo 没搞懂还, 没搞完
				if !reply.Success {
					//此时说明出现了日志的不一致 ,先 快速备份第一次, 找到 下一个term位置, 进行一次的
					rf.fastBackup(server, reply)             // 快速恢复
					rf.agreement(server, rf.lm.len(), false) //同步的过程
					//从最新的位置检查一致性
				} else {
					// 更新一致的位置, 维护 数组
					rf.mu.Lock()
					rf.nextIndexes[server] = args.PrevLogIndex + 1 // 这个fo下一个index的索引位置
					rf.matchIndexes[server] = args.PrevLogIndex    // 当前fo和自己同步的位置
					rf.mu.Unlock()
					rf.tryCommit() // 每次心跳后检查都能进行提交
				}
			}(i) // 遍历每一个发送的对象, 即server peer

		}
	}

}

// 快速的定位到不一致的term和index, 并且重置 leader中维护的 每个fo对应的index的数组
// 可能需要多次调用 这个 方法去找到 最终的term
//剩下的同步任务交给 aggrement去做

func (rf *Raft) fastBackup(server int, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock() // 因为需要访问rf节点的 leader特有的 记录 每个 follower 的nextIndex和 matchIndex 数组
	// 需要更新并发访问的变量, 所以需要锁

	if reply.XTerm == -1 {
		// follower对应的index , 即 leader想哟啊检查的位置 位置没有log
		//
		rf.nextIndexes[server] = reply.XLen + 1

	} else if reply.XTerm == 0 { // 某个地方出问题了，执行到这里不应该是temr 0
		// 将 leader中的 这个 fo对应的索引位置 设置为1 肯定没错
		rf.nextIndexes[server] = 1
	} else {
		// 这种情况, 表示 fo有冲突的term, 这算是锁定到term目标了
		//前边 的情况, 表示还没锁定到对应的term , 注意这个方法是 leader调用的

		//找到当前日期的最后一个日志的index, 这个即我们要开始更新的位置 !
		lastOfTerm := rf.findLastLogIndexOfTerm(reply.XTerm)
		if lastOfTerm == -1 { // leader没有follower的Term
			rf.nextIndexes[server] = reply.XIndex
		} else { //否则说明 leader有这个term 的日志, 标记 在index数组里边即可
			//至此我们知道了这个fo 下一个日志位置
			rf.nextIndexes[server] = lastOfTerm + 1
		}
	}
}

// todo, 实现的完善, aggrement
func (rf *Raft) agreement(server int, nextIndex int, isHeartbeat bool) {
	rf.mu.Lock()

	// 构造 AppendEntriesArgs
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  rf.lm.GetEntry(nextIndex - 1).Term, // 获取前一个日志条目的任期
		Entries:      rf.lm.getEntriesFrom(nextIndex),    // 从 nextIndex 开始的所有日志条目
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	// 发送 AppendEntries RPC
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(server, &args, &reply) {
		if reply.Success {
			// 更新一致的位置
			rf.mu.Lock()
			rf.nextIndexes[server] = nextIndex + len(args.Entries)      // 更新为下一个索引
			rf.matchIndexes[server] = nextIndex + len(args.Entries) - 1 // 更新 matchIndexes
			rf.mu.Unlock()
			rf.tryCommit() // 尝试提交日志
		} else {
			// 如果失败，说明还有不一致，调用 fastBackup
			rf.fastBackup(server, reply)
			// 再次调用 agreement，递归直到日志同步
			rf.agreement(server, rf.nextIndexes[server], isHeartbeat)
		}
	}
}

// 超时选举
func (rf *Raft) kickOffElection() {
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me // candidate id
	rf.persist()        // 将当前的持久化状态持久化 ! 防止宕机导致的不一致
	votes := 1
	total := len(rf.peers)
	args := rf.getRequestVoteArgs() // 获取 请求的content  , 即创建请求的方法
	mu := sync.Mutex{}
	for i := 0; i < total; i++ {
		//
		if i == rf.me {
			continue
		}
		// 使用goroutine去 投票, 并发的,
		// 将 server传入, 表示是发给 谁的 ? 对应参数i,  即每个 server都会给你发一个则更好
		go func(server int) {
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}
			if args.Term == rf.currentTerm && rf.state == CANDIDATE {
				// 判断防止自己身份已经改变, 并且判断term 是否和自己是一个
				if reply.Term < rf.currentTerm {
					// 投票者日期小于自己, 丢弃
					return
				}
				if reply.VoteGranted {
					//如果确认给自己投票
					mu.Lock()
					votes++
					if rf.state != LEADER && votes > total/2 {
						// 超过半数, 并且当前不是leader  !
						rf.initLeader() // 初始化一些leader 才有的信息
						rf.state = LEADER
						// 立刻开始心跳, 告诉所有的 节点我是leader了
						go rf.sendHeartbeats()
					}

					mu.Unlock()
				}
			}
		}(i)

	}
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// todo  but it does call the Kill() method. your code can use killed() to
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
	// 初始化 electionTimer
	rf.resetElectionTimer()

	for {
		select {
		// candidate 有的选举timer , 存在每个candidate内部
		case <-rf.electionTimer.C:
			// 从选举 timer中去取消息, 这里的timer类似于一个chan
			//定时器一旦超时, 就会发送一个消息 到这个里边, 所以如果接受到消息, 就说明 选举超市了
			// 选举超时处理
			rf.mu.Lock()
			if rf.state != LEADER { // 只有 FOLLOWER 或 CANDIDATE 状态才需要重新选举
				rf.kickOffElection()
			}
			rf.mu.Unlock()

			// 重置选举计时器
			rf.resetElectionTimer()

		case <-rf.heartbeatTimer.C:
			// 即 心跳超时, 此时需要重新发送 心跳 , leader独有的 心跳timer
			// 存在leader 的内部
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.sendHeartbeats() // Leader 定期发送心跳
			}
			rf.mu.Unlock()

			// 重置心跳计时器
			rf.resetHeartbeatTimer()
		}
	}
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
		rf.heartbeatTimer = time.NewTimer(100 * time.Millisecond)
	} else {
		rf.heartbeatTimer.Reset(100 * time.Millisecond)
	}
}

// 生成一个随机的选举超时时间（例如 150 到 300 毫秒之间）
func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term >= rf.currentTerm {
		// 至少任期持平才会 进行下一步
		if args.Term > rf.currentTerm {
			// 碰到比自己新的了, 更新自己的任期, 并且立刻将自己的状态更新为follower
			//根据 paper的设计, 别人已经比你更新了, 你就别candidate了
			rf.enterNewTerm(args.Term) // 应该是需要一个atomic的操作
			// 并且一些投票操作,  需要在新的任期里边做
		}
		if rf.state != CANDIDATE { // 如果不是候选人 就重置选举计时
			// 因为此时接收到一个 选举请求了, 所以重置自己的计时器即可, 你不用选了, 有比你大的人在选了

			rf.resetElectionTimer()
		}
		rf.mu.Lock()
		reply.Term = rf.currentTerm // 构造reply
		reply.VoteGranted = true
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			// 表示已经投过票给当前的人 , 或者-1表示还没投给任何人
			// 安全性检查
			len := rf.lm.len() // 日志的逻辑长度, 因为有一部分可能已经到 快照里边了

			var lastLogIndex, lastLogTerm int
			if len == rf.lm.lastTrimmedIndex {
				// 如果 日志逻辑 长度 = trimmed index, 表示所有的日志都在 快照里边
				lastLogIndex = rf.lm.lastTrimmedIndex
				lastLogTerm = rf.lm.lastTrimmedTerm

			} else {
				lastLogIndex = len
				lastLogTerm = rf.lm.GetEntry(len).Term
			}
			if lastLogTerm == 0 || lastLogTerm < args.LastLogTerm {
				//在term 初始为0, 表示自己刚启动,
				//或者自己的最后一个日志的 term小于 请求者的 term就投票
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				// 之前更新的是节点的term , 这里是最后一个log 的term
			} else if lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex {
				// 在最后一个日志的 任期相同的情况下, 比较 logindex , 他比我的新, 投
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
			}
		}
		rf.mu.Unlock()
	}
	go rf.persist() // 将 一些信息 持久化 , 如当前的term和voted for
}

func (rf *Raft) enterNewTerm(newTerm int) {
	rf.mu.Lock()         // 加锁，确保操作是原子的
	defer rf.mu.Unlock() // 在函数结束时解锁

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

// getRequestVoteArgs 构造请求投票的参数。
func (rf *Raft) getRequestVoteArgs() RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock() // 需要锁

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
	PrevLogIndex int         // 前一个日志的日志号, 作用?
	PrevLogTerm  int         //
	Entries      []*LogEntry //  这是一个数组存放的是指向日志体的指针,
	LeaderCommit int         //leader已经提交的日志 数
}

// fo 回复leader 的内容
type AppendEntriesReply struct {
	Term    int // 当前 fo 的任期
	XTerm   int // 冲突的log的日期, 用于定位一致性检查
	XIndex  int // 任期为X term 的第一个log 的index
	XLen    int // follower自己的log长度
	Success bool
}

// todo , 初始化leader, 一些leader才有的属性
func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 因为要访问 shared

	rf.state = LEADER

	// Initialize nextIndex and matchIndex for each follower
	rf.nextIndexes = make([]int, len(rf.peers))
	rf.matchIndexes = make([]int, len(rf.peers))

	// Set nextIndex to the index after the last log entry
	lastLogIndex := rf.getLastLogIndex()
	for i := range rf.peers {
		rf.nextIndexes[i] = lastLogIndex + 1
		rf.matchIndexes[i] = 0
	}

	// 重置 心跳
	if rf.heartbeatTimer != nil {
		rf.heartbeatTimer.Stop()
	}

	rf.resetHeartbeatTimer()

	// 开始 发送心跳
	go rf.sendHeartbeats()
}

// leader统计matchIndex，尝试提交
func (rf *Raft) tryCommit() {
	total := len(rf.peers)
	// 找到一个最大的N>commitIndex，使得超过半数的follower的matchIndex大于等于N，
	// 且leader自己N位置的log的Term等于当前Term   （这一点很重要，安全性问题中提过, 否则的话需要提交一个空日志来确保最新的要提交的日志是自己的term
	// 那么N的位置就可以提交

	len := rf.lm.len()
	for N := rf.commitIndex + 1; N <= len; N++ { // 遍历所有的 当前commit 的index , 去找到第一个能提交的 index , 即满足半数
		majorityCnt := 1 // 记录是否超过半数
		for _, matchIndex := range rf.matchIndexes {
			if matchIndex >= N && rf.lm.logs[N].Term == rf.currentTerm { // 判断是否当前任期 并且是否 对应的  节点有超过 当前的 index
				// 注意设计的matchindex 和next Index 的区别, 前者是提交的后者是发送copy 对应位置
				majorityCnt++
			}
		}
		if majorityCnt > total/2 {
			rf.commitIndex = N // 更新提交Index
		}
	}
	rf.mu.Lock()
	rf.applyCond.Signal() // 通过channel 唤醒 异步的apply , 之前一直被阻塞
	// 阻塞也是

	rf.mu.Unlock()
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

// todo make函数, 功能: ?
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// RequestAppendEntries follower 处理日志同步
// 对xTerm等 进行一个操作 , 不断的处理 leader寻找 缺失日志位置的情况 ( 什么情况下需要发送 xterm? 即你 拒绝了 leader请求的时候

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // 说明这个leader已经过期
		reply.Success = false
		return
	}
	// 进入新的一个Term，更新( 如果 发过来的人 的 term更新的话, 自己肯定不进行选举,  遂reset选举计时器
	if args.Term > rf.currentTerm {
		rf.enterNewTerm(args.Term) // 改变votedFor
	} else {
		rf.state = FOLLOWER // 不改变votedFor
	}
	rf.resetElectionTimer() // 刷新选举计时
	rf.mu.Lock()
	// 因为需要访问共享的变量, 所以 锁到结束为止
	defer rf.mu.Unlock()

	if args.PrevLogIndex < rf.commitIndex || rf.findLastLogIndexOfTerm(args.Term) > args.PrevLogIndex {
		// 说明这个请求滞后了 , 竟然比当前fo节点还靠后, 回复失败
		reply.XLen = rf.lm.len()
		reply.XTerm = -1
		reply.Success = false
		return
	}
	// 一致性检查, 看是否日志冲突?  如果
	if args.PrevLogIndex > rf.lm.len() {
		reply.XLen = rf.lm.len()
		reply.XTerm = -1
		reply.Success = false
		return
	}
	// 如果不在一个 term内, 表示冲突, 返回false
	if args.PrevLogIndex >= 1 && args.PrevLogIndex > rf.lm.lastTrimmedIndex { // 如果prevIndex已经被裁剪了，那一定不冲突
		if rf.lm.GetEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
			// 有冲突了, 此时 需要进行一致性的检查, 连续的 fo和leader之间的确认,所以需要 将 xterm 和xidnex 返回
			// 因为使用的是快速定位, fastbackup, 定位先以term为单位, 找到term后, 以index为单位

			reply.XTerm = rf.lm.GetEntry(args.PrevLogIndex).Term
			reply.XIndex = rf.findFirstLogIndexOfTerm(reply.XTerm)
			reply.Success = false
			return
		}
	}
	// 接下来就是接受日志, 并且覆盖

	// 这里有可能leader传来的一部分log已经裁掉了，需要过滤一下

	// 确定当前的 起始index , 谁靠后一点取谁
	// 避免处理掉已经 裁剪掉的日志( 注意leader 的prelogindex即 这个日志体前一个日志的位置, 即提示你更新的位置

	from := max(args.PrevLogIndex+1, rf.lm.lastTrimmedIndex+1)

	// 确定需要过滤的 日志的个数, 并且不能越界
	// from位置之前的是需要过滤的, 画个图很好理解
	filter := min(from-args.PrevLogIndex-1, len(args.Entries)) // 防止越界,
	args.Entries = args.Entries[filter:]
	rf.lm.appendFrom(from, args.Entries) // 强制追加（覆盖）日志

	// 提交log, 更新自己的 提交index 为 leader 提交的index 或者 本地的长度
	// 因为这个长度是随着 term更新, 所以需要 判断一下哪个 长
	//目前还在 lm里边的日志 , 即没有 持久apply 的日志 ,可能比 leader 的要长
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lm.len())
	}
	rf.applyCond.Signal() // 唤醒异步apply, 真正的将东西 本地化

	rf.persist() // 将一些 信息持久化
	reply.Success = true
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

func (rf *Raft) apply() {
	// 先apply快照
	if rf.lm.lastTrimmedIndex != 0 {
		rf.applySnapshot()
	}
	rf.backupApplied = true // 已经
	// 然后再apply剩余log
	for !rf.killed() {
		// 循环的去检查, 但是不意味着一直循环, 而是 等待唤醒而阻塞
		// >=意味着没有 可以 apply 的snapshot
		for rf.lastApplied >= rf.commitIndex {
			// 每次休眠前先看有无快照可apply
			select {
			case index := <-rf.installSnapCh: // 获取快照的信息
				// 这里获取到的就是apply 中的index, 即快照截止到了哪个索引, 并调用方法去更新

				// 这两个操作要保证原子性
				rf.trim(index)
				// 去截断相关的日志, ? 应该就是 产生快照吧, 因为这个时候已经确定日志将会在快照里边, 所以 可以放心

				rf.applySnapshot()
			default:
			}
			rf.mu.Lock()
			rf.applyCond.Wait() // 等待别处唤醒去apply，避免了并发冲突
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		// commitIndex领先了
		applyIndex := rf.lastApplied + 1
		commitIndex := rf.commitIndex
		entries := rf.lm.split(applyIndex, commitIndex+1) // 本轮要apply的所有log
		rf.mu.Unlock()
		// 这里用不到索引, 所以爆红
		for _, log := range entries {
			if applyIndex <= rf.lm.lastTrimmedIndex { // applyIndex落后快照了
				break
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: applyIndex,
				CommandTerm:  log.Term, // 为了Lab3加的
			}
			rf.applyCh <- msg
			rf.mu.Lock()
			if rf.lastApplied > applyIndex { // 说明snapshot抢先一步了
				rf.mu.Unlock()
				break
			}
			rf.lastApplied = applyIndex
			applyIndex++
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) trim(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 确保 trim 的索引是合法的
	if index <= rf.lm.lastTrimmedIndex {
		return // 如果裁剪索引已经被裁剪过，直接返回
	}

	// 裁剪日志
	relativeIndex := index - rf.lm.lastTrimmedIndex - 1
	if relativeIndex >= len(rf.lm.logs) {
		// 如果要裁剪的范围大于日志数组，清空日志
		rf.lm.logs = nil
	} else {
		// 从裁剪点之后的日志开始保留
		rf.lm.logs = rf.lm.logs[relativeIndex:]
	}

	// 更新最后裁剪的索引
	rf.lm.lastTrimmedIndex = index

	// 持久化裁剪后的状态
	rf.persist()
}

// todo 实现快照 的apply
func (rf *Raft) applySnapshot() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查快照数据是否有效
	if rf.snapshot == nil || len(rf.snapshot) == 0 {
		return // 没有快照可应用
	}

	// 解码快照数据, 先将元数据搞出来
	var snapshotState StateMachineState
	if err := decodeSnapshot(rf.snapshot, &snapshotState); err != nil {
		log.Fatalf("Failed to decode snapshot")
	}

	// 更新 Raft 的元数据
	rf.commitIndex = snapshotState.LastIncludedIndex
	rf.lastApplied = snapshotState.LastIncludedIndex
	rf.lm.lastTrimmedIndex = snapshotState.LastIncludedIndex

	// 恢复状态机
	rf.stateMachine.Restore(snapshotState.Data)

	// 持久化更新
	rf.persist()
}

// StateMachineState todo 存储快照的元数据和信息的状态机
type StateMachineState struct {
	LastIncludedIndex int         // 快照包含的最后一个日志条目的索引
	LastIncludedTerm  int         // 快照包含的最后一个日志条目的任期
	Data              interface{} // 快照中的状态机数据
}

// 讲序列化的快照解码为 一个个这样的struct
func decodeSnapshot(snapshot []byte, state *StateMachineState) error {
	if snapshot == nil || len(snapshot) == 0 {
		return fmt.Errorf("snapshot is empty")
	}

	r := bytes.NewBuffer(snapshot) // 创建读取缓冲区
	d := labgob.NewDecoder(r)      // 创建解码器

	// 解码快照数据
	if err := d.Decode(&state.LastIncludedIndex); err != nil {
		return fmt.Errorf("failed to decode LastIncludedIndex: %v", err)
	}
	if err := d.Decode(&state.LastIncludedTerm); err != nil {
		return fmt.Errorf("failed to decode LastIncludedTerm: %v", err)
	}
	if err := d.Decode(&state.Data); err != nil {
		return fmt.Errorf("failed to decode Data: %v", err)
	}

	return nil
}

type StateMachine struct {
	state interface{} // 用于存储状态机的数据
}

// Restore 将快照数据恢复到状态机中
func (sm *StateMachine) Restore(data interface{}) {
	sm.state = data
}
