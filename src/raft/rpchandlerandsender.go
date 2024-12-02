package raft

import "fmt"

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	return args
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
		Term:     rf.currentTerm, // 当前任期
		LeaderID: rf.me,          // 领导者 ID
		// 代码逻辑写错了 LastIncludedIndex: rf.lm.LastIndex(),                      // 快照的索引
		LastIncludedIndex: rf.lm.FirstIndex(),
		LastIncludedTerm:  rf.lm.GetEntry(rf.lm.FirstIndex()).Term, // 快照的任期
		SnapshotData:      rf.persister.ReadSnapshot(),             // 拷贝一份 , 而不是直接 使用, 防止并发的情况
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

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing RequestVote,  RequestVoteArgs %v and RequestVoteReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)
	// Reply false if term < currentTerm(§5.1)
	// if the term is same as currentTerm, and the votedFor is not null and not the candidateId, then reject the vote(§5.2)
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.ChangeState(FOLLOWER)
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	// if candidate's log is not up-to-date, reject the vote(§5.4)
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int        // 即自己的id
	PrevLogIndex int        // 前一个日志的日志号
	PrevLogTerm  int        //
	Entries      []LogEntry //  这是一个数组存放的是指向日志体的指针,
	LeaderCommit int        //leader已经提交的日志 数

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

//// Leader处理Follower 的 ae response
//func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesArgs, response *AppendEntriesReply) {
//	if rf.state == LEADER && rf.currentTerm == request.Term {
//		// 如果当前是leader, 并且 任期一致,  则进行处理
//		if response.Success { // 如果 ae成功, leader 进行
//			// 根据返回结果, 维护对应的 matchidnex和nextindex,
//
//			rf.matchIndexes[peer] = request.PrevLogIndex + len(request.Entries)
//			//match是 追随者同步的最大索引位置
//			// 而 next是要发送的下一个索引的位置, 即match 的下一个咯
//			rf.nextIndexes[peer] = rf.matchIndexes[peer] + 1
//			rf.advanceCommitIndexForLeader() // 推进leader 的 commitindex ( 根据follower
//		} else {
//			//失败的follower ae response , leader要检查哪里有问题
//			// 有可能是自己不再是leader了, 所以检查一下是否有新的 leader , 即term 的检查
//			if response.Term > rf.currentTerm {
//				rf.ChangeState(FOLLOWER)
//				// 将自己变回fo , 并重启 选举计时器等
//				rf.currentTerm, rf.votedFor = response.Term, -1
//				rf.persist() // 保持自己的follower状态
//
//			} else if response.Term == rf.currentTerm {
//				// 确定当前是同一个任期  , 需要重新定位, 即还没找到 缺失的位置, 需要继续定位
//				// 更新nextindex 为冲突的index( 这里的nextindex是逻辑上的哦, 不是实际的
//
//				rf.nextIndexes[peer] = response.XIndex
//
//				if response.XTerm != -1 { // 说明 是日志的xterm出现了冲突, 即虽然节点在一个 term上,但是日志进度不在
//					// 需要锁定到对应的term, 这里可以使用fastbackup方法
//
//					firstIndex := rf.lm.FirstIndex()
//
//					// 找到冲突的 任期, 通过leader 的第一个 index for直接找( 一个个的找, 这也是论文给出的方案
//					// 为什么用for? 因为差的不太多, for 不会很复杂的
//					// leader从previndex往前找, 直到找到对应的 冲突的term为止
//					// 从request里边找自己的 上一个index, 因为request也是leader发的,包含了leader 的信息
//					for i := request.PrevLogIndex; i >= firstIndex; i-- {
//
//						//因为之前判断用ae的话, 肯定是差很多, 所以肯定在 leader 的firstindex之前
//						// 通过i- firstindex, 确定实际 leader中的索引位置
//						if rf.lm.logs[i-firstIndex].Term == response.XTerm {
//							rf.nextIndexes[peer] = i + 1
//							break
//						}
//					}
//				}
//			}
//		}
//	}
//	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lm.GetEntry(rf.lm.FirstIndex()), rf.lm.GetEntry(rf.lm.LastIndex()), rf.lastIncludedTerm, rf.lastIncludedIndex)
//}

////
//// 从一个peer来的请求, 告诉你要按照快照了
//// 传入request, 通过response返回信息
//func (rf *Raft) handleInstallSnapshotResponse(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
//	if response.Term > rf.currentTerm {
//		// 如果返回的任期大于当前任期，更新 Raft 节点的任期并转换为 Follower
//		rf.enterNewTerm(response.Term)
//		rf.state = FOLLOWER
//	}
//
//	// 如果响应成功，更新该 peer 的 matchIndex 和 nextIndex
//	if response.Success {
//		rf.matchIndexes[peer] = request.LastIncludedIndex
//		rf.nextIndexes[peer] = request.LastIncludedIndex + 1
//		rf.applyCond.Signal() // 唤醒 apply goroutine，应用新的快照
//	} else {
//		// 处理失败的响应，通常会进行重试或调整 nextIndex
//		rf.nextIndexes[peer] = rf.matchIndexes[peer] // 调整 nextIndex
//	}
//}

// Follwer 接收 Leader日志同步 处理

func (rf *Raft) AppendEntries(request *AppendEntriesArgs, response *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lm.GetEntry(rf.lm.FirstIndex()), rf.lm.GetEntry(rf.lm.LastIndex()), request, response)

	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
		rf.persist()
	}

	rf.ChangeState(FOLLOWER)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// 若leader安装了snapshot，会出现rf.log.getFirstLog() > PrevLogIndex的情况。
	if request.PrevLogIndex < rf.lm.FirstIndex() {
		response.Term, response.Success = rf.currentTerm, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId, request.PrevLogIndex, rf.lm.GetEntry(rf.lm.FirstIndex()))
		return
	}
	// 判断PrevLog存不存在

	//if !rf.LogMatched(request.PrevLogTerm, request.PrevLogIndex) { todo你这个失误是否点...把 term和index 写反了.......
	if !rf.LogMatched(request.PrevLogIndex, request.PrevLogTerm) {

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
			//	response.XTerm = rf.lm.logs[request.PrevLogIndex-firstIndex].Term

			// index := request.PrevLogIndex - 1 ? todo这里逻辑是不是错了,不应该-1 ?
			index := request.PrevLogIndex
			// 这里逻辑也错了, 是为了找到对应的index , 所以要的是 找到 当前fo里边的term和args 的 prevlogterm , 即当前termp匹配的
			for index >= firstIndex && rf.lm.logs[index-firstIndex].Term == request.PrevLogTerm {
				index--
			}
			response.XIndex, response.XTerm = index+1, request.PrevLogTerm
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

	//todo 逻辑上的遗漏 , 如果leader commit 大于 了 当前的commit index , 就需要将 commit 更新为 min commit index 和 数组最后一个的索引
	// 因为超过数组长度的不能提交啊, 实际就是通知commitindex follower  逻辑

	//通知上层可以apply主节点已经commit的日志。
	rf.advanceCommitIndexForFollower(request.LeaderCommit)

	response.Term, response.Success = rf.currentTerm, true
}
