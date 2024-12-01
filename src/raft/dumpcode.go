package raft //func (rf *Raft) replicateOneRound(peer int) {
//	rf.mu.RLock()
//	if rf.state != LEADER {
//		// 只有leader能调用
//		rf.mu.RUnlock()
//		return
//	}
//	prevLogIndex := rf.nextIndexes[peer] - 1
//	// 计算当前 fo 的 日志index
//	// 如果小于 leader 的第一个日志, 说明你差的也太多了
//	//  那么说明日志条目无法直接同步, 只能通过快照
//	if prevLogIndex < rf.lm.FirstIndex() {
//		// only snapshot can catch upetFirstLog().Index {
//
//		//产生快照请求
//		request := rf.genInstallSnapshotRequest()
//		rf.mu.RUnlock()
//		response := new(InstallSnapshotResponse)
//		if rf.sendInstallSnapshot(peer, request, response) {
//			//发送成功, 表示会有一个response, 此时接受response 并进行处理( leader
//
//			rf.mu.Lock()
//			rf.handleInstallSnapshotResponse(peer, request, response)
//			rf.mu.Unlock()
//		}
//	} else { // 否则  不通过快照, 而是通过 appendentries
//
//		request := rf.genAppendEntriesRequest(prevLogIndex)
//		rf.mu.RUnlock()
//		response := new(AppendEntriesReply)
//		if rf.sendAppendEntries(peer, request, response) {
//			rf.mu.Lock()
//			//处理送回的 response ,可能涉及多轮的 日志追加
//			rf.handleAppendEntriesResponse(peer, request, response)
//			rf.mu.Unlock()
//		}
//	}
//}

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

//// ChangeState 这里不能用锁, 确保外部 调用的时候有锁
//// ChangeState sets the node's state to the given state (e.g., FOLLOWER).
//func (rf *Raft) ChangeState(newState RaftState) {
//	//rf.mu.Lock()         // 锁定以确保线程安全
//	//defer rf.mu.Unlock() // 确保在函数返回时解锁
//
//	// 如果当前状态已经是目标状态，则不做任何操作
//
//	if rf.state == RaftState(newState) {
//		return
//	}
//
//	// 设置新的状态
//	rf.state = RaftState(newState)
//
//	// 如果目标状态是 FOLLOWER
//	if rf.state == FOLLOWER {
//		// 1. 重置选举计时器
//		rf.resetElectionTimer()
//
//		// 2. 清除投票信息，表示该节点不再参与当前选举
//		rf.votedFor = -1
//
//	}
//	//todo candidate 的内容
//	//其他的状态, 如 leader和leader 的初始化可以写在这里, initleader 等
//
//}

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

// 初始化leader, 一些leader才有的属性
//// 这里不用锁, 就要确保外部 调用的时候有锁 !
//func (rf *Raft) initLeader(RaftState) {
//	// 因为要访问 shared
//	rf.currentTerm++
//	rf.state = LEADER
//
//	// Initialize nextIndex and matchIndex for each follower
//	rf.nextIndexes = make([]int, len(rf.peers))
//	rf.matchIndexes = make([]int, len(rf.peers))
//
//	// Set nextIndex to the index after the last log entry
//	lastLogIndex := rf.getLastLogIndex()
//
//	//更新两个数组, 反正后续会通过 心跳和一致性检查 进行维护的, 根据follower 的进度
//	for i := range rf.peers {
//		rf.nextIndexes[i] = lastLogIndex + 1
//		rf.matchIndexes[i] = 0
//	}
//
//	// 重置 心跳
//	if rf.heartbeatTimer != nil {
//		rf.heartbeatTimer.Stop()
//	}
//
//	if rf.electionTimer != nil {
//		rf.electionTimer.Stop() // Stop election timer for the leader
//	}
//
//	// rf.resetHeartbeatTimer()
//
//	// 别忘记leader开始发送心跳,不过不是这个函数的功能
//}
// 判断当前日志是否更新
//func (rf *Raft) isLogUpToDate(lastLogTerm int, lastLogIndex int) bool {
//	// 获取当前节点的最后一条日志的索引和 term
//	currentLastLogIndex := rf.lm.LastIndex()
//	currentLastLogTerm := rf.lm.GetEntry(currentLastLogIndex).Term
//
//	// 比较日志的 term 和 index
//	if lastLogTerm > currentLastLogTerm {
//		// 请求中的日志 term 更大，说明更新
//		return true
//	} else if lastLogTerm == currentLastLogTerm {
//		// 如果 term 相同，比较索引
//		return lastLogIndex >= currentLastLogIndex
//	}
//	// 否则，日志较旧
//	return false
//}

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
//for rf.killed() == false {
//	select {
//	// candidate 有的选举timer , 存在每个candidate内部
//	case <-rf.electionTimer.C:
//		// 从选举 timer中去取消息, 这里的timer类似于一个chan
//		//定时器一旦超时, 就会发送一个消息 到这个里边, 所以如果接受到消息, 就说明 选举超市了
//		// 选举超时处理
//		rf.mu.Lock()
//		if rf.state != LEADER { // 只有 FOLLOWER 或 CANDIDATE 状态才需要重新选举
//			// todo  对于candidate需要做什么? 并且锁的获取?
//			// 内部不能再有锁的获取了
//
//			rf.currentTerm += 1 // todo .... 你选举的时候忘记 +1了, 把自己的term
//
//			rf.ChangeState(CANDIDATE)
//
//			//
//			rf.StartElection()
//		}
//
//		rf.resetElectionTimer()
//		// todo 这里的锁应该释放呢, 因为还有不进入 election 的情况呢
//		rf.mu.Unlock()
//		// 重置选举计时器
//		//rf.resetElectionTimer()
//
//	case <-rf.heartbeatTimer.C:
//		// 即 心跳超时,leader  此时需要重新发送 心跳 , leader独有的 心跳timer( 需要定期的发送心跳
//		// 存在leader 的内部
//		rf.mu.Lock()
//		if rf.state == LEADER {
//			rf.BroadcastHeartbeat(true) // Leader 定期发送心跳
//			// 重置心跳计时器
//			rf.resetHeartbeatTimer()
//		}
//		rf.mu.Unlock()
//
//	}
//}
