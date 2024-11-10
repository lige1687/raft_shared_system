package raft

import (
	"6.5840/labgob"
	"bytes"
	"sync"
)

// LogEntry represents a single log entry in Raft.
type LogEntry struct {
	Term    int         // 任期号
	Command interface{} // 客户端命令
}

// LogManager handles log entries and related operations.
type LogManager struct {
	mu                sync.Mutex
	logs              []LogEntry // 日志条目数组
	lastIncludedIndex int        // 快照包含的最后一个日志条目的索引
	lastIncludedTerm  int        // 快照包含的最后一个日志条目的任期
	persister         *Persister // 持久化管理器
}

// NewLogManager creates a new LogManager.
func NewLogManager(persister *Persister) *LogManager {
	lm := &LogManager{
		logs:      make([]LogEntry, 1), // 初始包含一个空日志项，用于索引偏移
		persister: persister,
	}
	lm.Restore(persister.ReadRaftState()) // 从持久化状态恢复日志和快照
	return lm
}

// AppendEntry appends a new log entry to the log.
func (lm *LogManager) AppendEntry(term int, command interface{}) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	entry := LogEntry{Term: term, Command: command}
	lm.logs = append(lm.logs, entry)
	lm.persist()
}

// GetEntry returns the log entry at a specific index.
func (lm *LogManager) GetEntry(index int) (LogEntry, bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if index <= lm.lastIncludedIndex || index >= lm.LastIndex() {
		return LogEntry{}, false
	}
	return lm.logs[index-lm.lastIncludedIndex], true
}

// LastIndex returns the index of the last log entry.
func (lm *LogManager) LastIndex() int {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.lastIncludedIndex + len(lm.logs) - 1
}

// LastTerm returns the term of the last log entry.
func (lm *LogManager) LastTerm() int {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if len(lm.logs) > 0 {
		return lm.logs[len(lm.logs)-1].Term
	}
	return lm.lastIncludedTerm
}

// Persist saves the current log entries and snapshot metadata to stable storage.
func (lm *LogManager) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w) // 获取 实验提供的 编码器

	// Encode snapshot metadata
	e.Encode(lm.lastIncludedIndex)
	e.Encode(lm.lastIncludedTerm)

	// Encode log entries
	e.Encode(lm.logs)

	raftstate := w.Bytes()
	lm.persister.Save(raftstate, lm.persister.ReadSnapshot())
}

// ApplySnapshot replaces the log with the snapshot up to lastIncludedIndex and lastIncludedTerm.
func (lm *LogManager) ApplySnapshot(snapshot []byte, lastIncludedIndex, lastIncludedTerm int) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.lastIncludedIndex = lastIncludedIndex
	lm.lastIncludedTerm = lastIncludedTerm
	lm.logs = nil // 清空日志，只保留快照之后的日志条目

	// 持久化快照元数据
	lm.persist()
	lm.persister.Save(lm.persister.ReadRaftState(), snapshot)
}

// Restore restores log and snapshot metadata from persisted state.
func (lm *LogManager) Restore(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var lastIncludedIndex int
	var lastIncludedTerm int
	var logs []LogEntry

	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&logs) != nil {
		// handle error
		return
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.lastIncludedIndex = lastIncludedIndex
	lm.lastIncludedTerm = lastIncludedTerm
	lm.logs = logs
}

// TruncateLog removes entries before a given index, typically after taking a snapshot.
func (lm *LogManager) TruncateLog(lastIncludedIndex int, lastIncludedTerm int) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lastIncludedIndex < lm.lastIncludedIndex {
		return
	}

	lm.logs = lm.logs[lastIncludedIndex-lm.lastIncludedIndex:]
	lm.lastIncludedIndex = lastIncludedIndex
	lm.lastIncludedTerm = lastIncludedTerm
	lm.persist()
}
