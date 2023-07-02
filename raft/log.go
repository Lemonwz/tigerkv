// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	nilIndex uint64

	nilTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	raftLog := &RaftLog {
		storage: storage,
		entries: make([]pb.Entry, 0),
	}

	lastLogIndex, _ := storage.LastIndex()
	firstLogIndex, _ := storage.FirstIndex()

	raftLog.committed = firstLogIndex - 1
	raftLog.applied = firstLogIndex - 1
	raftLog.stabled = lastLogIndex
	raftLog.nilIndex = firstLogIndex - 1
	raftLog.nilTerm, _ = storage.Term(raftLog.nilIndex)

	if entries, err := storage.Entries(firstLogIndex, lastLogIndex + 1); err == nil {
		raftLog.appendEntries(entries)
	}
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if l.stabled == l.LastIndex() {
		return make([]pb.Entry, 0)
	}
	unstable, err := l.getEntries(l.stabled + 1, l.LastIndex() + 1)
	if err != nil {
		return nil
	}
	return unstable
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	left := max(l.applied + 1, l.FirstIndex())
	if l.committed >= left {
		entries, err := l.getEntries(left, l.committed + 1)
		if err == nil {
			return entries
		}
	}
	return nil
}

// getEnts return entries between entries[left, right)
func (l *RaftLog) getEntries(left, right uint64) ([]pb.Entry, error) {
	if left > right {
		logf("get entries fail: left side %d is greater than right side %d", left, right)
		panic("get entries fail")
	}
	
	firstIndex := l.FirstIndex()
	if left < firstIndex {
		logf("get entries fail: left side %d is less than current first index %d", left, firstIndex)
		return nil, ErrCompacted
	}

	if right > l.LastIndex() + 1 {
		logf("get entries fail: right side %d is greater than current last index %d", right, l.LastIndex())
		panic("get entries fail")
	}

	return l.entries[left - firstIndex : right - firstIndex], nil
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		return l.nilIndex
	}

	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1
	}

	return l.entries[0].GetIndex()
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) == 0 {
		return l.nilIndex
	}
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i == l.nilIndex {
		return l.nilTerm, nil
	}
	if l.pendingSnapshot != nil && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}
	if i < l.FirstIndex() {
		return 0, ErrCompacted
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	return l.entries[i - l.FirstIndex()].Term, nil
}

// LastTerm return the term of the last entry in log entries
func (l *RaftLog) LastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		return 0;
	}
	return term
}

func (l *RaftLog) findLastIndexInTerm(term uint64) uint64 {
	for i := l.LastIndex(); i > l.nilIndex; i-- {
		if t, _ := l.Term(i); t == term {
			return i
		}
	}
	return None
}

// hasPendingSnapshot return if l.pendingSnapshot is nil or empty
func (l *RaftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil && IsEmptySnap(l.pendingSnapshot)
}

// snapshot return a pendingSnapshot(if exist) or a stabled snapshot in storage.
func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	if l.hasPendingSnapshot() {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

// call stableSnapshot means current pendingSnapshot has be saved to stable storage.
func (l *RaftLog) stableSnapshot(i uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		l.pendingSnapshot = nil
	}
}

func (l *RaftLog) stableEntries(i, t uint64) {
	if i == 0 {
		return
	}
	if i < l.stabled || i > l.LastIndex() {
		logf("stable entry %d is greater than current last index %d or less than current stabled index %d",
			i, l.LastIndex(), l.stabled)
		return
	}
	term, err := l.Term(i)
	if err != nil {
		return
	}
	if t != term {
		logf("term of stable entry %d is %d, not %d", i, term, t)
		return
	}
	l.stabled = i
}

func (l *RaftLog) applyEntries(i uint64) {
	if i == 0 {
		return
	}
	if i < l.applied || i > l.committed {
		logf("apply entry %d is greater than current committed index %d or less than current applied index %d",
			i, l.committed, l.applied)
		panic("apply entry fail")
	}
	l.applied = i
}

func (l *RaftLog) appendEntries(entries []pb.Entry) {
	if len(entries) == 0 {
		return
	}
	l.entries = append(l.entries, entries...)
}

func (l *RaftLog) deleteEntries(start uint64) {
	if start > l.LastIndex() || start < l.FirstIndex(){
		return
	}
	l.entries = l.entries[:start-l.FirstIndex()]
	if l.stabled > l.LastIndex() {
		l.stabled = l.LastIndex()
	}
}

func (l *RaftLog) commitEntries(i uint64) {
	if l.committed < i {
		if l.LastIndex() < i {
			logf("commit index %d is greater than current last index %d", i, l.LastIndex())
			panic("commitEntries fail")
		}
		l.committed = i
	}
}