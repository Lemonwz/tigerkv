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

import (
	"errors"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64

	// PeerGroupNum is the number of peer groups in raft group. It should only be
	// set when starting a new raft cluster. The value of PeerGroupNum should be
	// less than the number of raft nodes in cluster. Each raft node belongs to
	// a peer group. There is no intersection between different peer groups.
	PeerGroupNum uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// random election interval
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// number of peer groups in raft group.
	peerGroupNum uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		logf("s%x initial state fail", c.ID)
		panic(err)
	}
	prs := make(map[uint64]*Progress)
	for _, id := range c.peers {
		prs[id] = &Progress{}
	}
	raftLog := newLog(c.Storage)
	r := &Raft {
		id: c.ID,
		RaftLog: raftLog,
		Prs: prs,
		Lead: None,
		electionTimeout: c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		peerGroupNum: c.PeerGroupNum,
	}

	// load hard state
	if !IsEmptyHardState(hs) {
		if hs.Commit < r.RaftLog.committed || hs.Commit > r.RaftLog.LastIndex() {
			logf("s%x hardstate committed index %d is out of range [%d, %d]",
				r.id, hs.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
				panic("hardstate committed index out of range")
		}
		r.Term = hs.Term
		r.Vote = hs.Vote
		r.RaftLog.committed = hs.Commit;
	}

	// load conf state
	if !IsEmptyConfState(cs) {
		prs := make(map[uint64]*Progress)
		for _, id := range cs.Nodes {
			prs[id] = &Progress{}
		}
		r.Prs = prs
		r.peerGroupNum = cs.PeerGroupNum
	}

	// apply entries when restarting raft
	if c.Applied > 0 {
		r.RaftLog.applyEntries(c.Applied)
	}

	r.becomeFollower(r.Term, None)
	rand.Seed(time.Now().Unix())
	logf("new raft node s%x: peers[%s], term[%d], committed[%d], applied[%d], lastIndex[%d], lastTerm[%d]",
		r.id, c.peers, r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.LastTerm())
	return r
}

// getSoftState return current raft softState
func (r *Raft) getSoftState() *SoftState {
	return &SoftState{
		Lead: r.Lead,
		RaftState: r.State,
	}
}

// getHardState return current raft hardState
func (r *Raft) getHardState() pb.HardState {
	return pb.HardState{
		Term: r.Term,
		Vote: r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// initProgress initialize the replication progresses of all followers
func (r *Raft) initProgress() {
	for id := range r.Prs {
		r.Prs[id] = &Progress{
			Match: 0,
			Next: r.RaftLog.LastIndex() + 1,
		}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
}

// resetElectionTimeout set randomized election timeout
func (r *Raft) resetElectionTimeout() {
	randomTimeout := rand.Intn(r.electionTimeout) + r.electionTimeout
	r.randomElectionTimeout = randomTimeout
	r.electionElapsed = 0
}

// resetTerm reset term(if necessary) and relevant states
func (r *Raft) resetTerm(term uint64) {
	// r.Term < term means current vote is out of date
	if r.Term < term {
		r.Term = term
		r.Vote = 0
	}
	r.Lead = None
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)
	r.PendingConfIndex = None
	r.resetElectionTimeout()
	r.initProgress()
}

// electionCheck verifies whether current node can launch election and become leader
func (r *Raft) elecetionCheck() bool {
	if r.State == StateLeader {
		logf("[term %d] s%d is already leader, ignore election request", r.Term, r.id)
		return false
	}
	if r.Prs[r.id] == nil || r.RaftLog.hasPendingSnapshot() {
		logf("[term %d] s%d is unpromotable and can`t launch election", r.Term, r.id)
	}
	// TODO: pending configuration changes check
	return true
}

// launchElection starts an election and sends RequestVote message to each peer
func (r *Raft) launchElection() {
	if r.State == StateLeader {
		logf("[term %d] s%d is already leader, ignore election request", r.Term, r.id)
		return
	}
	if r.State == StateFollower {
		logf("[term %d] s%d is a follower now, can`t launch election", r.Term, r.id)
		return
	}
	logf("[term %d] s%d is starting a new election", r.Term, r.id)

	// only candidate itself, become leader directly
	if len(nodes(r)) == 1 {
		r.becomeLeader()
		return
	}

	for _, peerId := range nodes(r) {
		if peerId == r.id {
			continue
		}
		requestVoteMsg := pb.Message {
			MsgType:	pb.MessageType_MsgRequestVote,
			To: 		peerId,
			From: 		r.id,
			Term: 		r.Term,
			Index: 		r.RaftLog.LastIndex(),
			LogTerm: 	r.RaftLog.LastTerm(),
		}
		r.msgs = append(r.msgs, requestVoteMsg)
		logf("[term %d] s%d -> s%d MsgRequestVote: prevlog[index: %d, term: %d]",
				requestVoteMsg.Term, requestVoteMsg.From, requestVoteMsg.To, requestVoteMsg.Index, requestVoteMsg.LogTerm)
	}
}

// checkVotes checks current vote status
// if candidate get majority of votes, it convert to leader, or convert to follower
func (r *Raft) checkVotes() {
	counter := 0
	reject := 0
	for _, vote := range r.votes {
		if vote {
			counter++
			if counter > len(r.Prs) / 2 {
				r.becomeLeader()
				return
			}
		} else {
			reject++
			if reject > len(r.Prs) / 2 {
				r.becomeFollower(r.Term, None)
				return
			}
		}
	}
}

// advance notifies raft node that application has applied and saved
// progress in ready. raft node should update raftLog.
func (r *Raft) advance(rd Ready) {
	committedLen := len(rd.CommittedEntries)
	stabledLen := len(rd.Entries)
	if committedLen > 0 {
		r.RaftLog.applyEntries(rd.CommittedEntries[committedLen - 1].GetIndex())
	}
	if stabledLen > 0 {
		idx, term := rd.Entries[stabledLen - 1].GetIndex(), rd.Entries[stabledLen - 1].GetTerm()
		r.RaftLog.stableEntries(idx, term)
	}
	if !IsEmptySnap(&rd.Snapshot) {
		r.RaftLog.stableSnapshot(rd.Snapshot.Metadata.Index)
	}
	logf("s%x apply ready: committed entries[%v], applied entries[%v]",
			r.id, rd.Entries, rd.CommittedEntries)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) {
	r.sendAppendOrEmpty(to, true)
}

// isEmpty argument controls whether messages with no entries will be sent.
// "empty" messages (without entries) are useful to convey updated committed
// indexes, but are undesirable when we're sending multiple messages in a batch.
func (r *Raft) sendAppendOrEmpty(to uint64, isEmpty bool) bool {
	appendMsg := pb.Message{
		To: to,
		From: r.id,
		Term: r.Term,
	}

	nextIndex := r.Prs[to].Next
	if nextIndex == None {
		nextIndex = r.RaftLog.nilIndex + 1
	}
	prevLogIndex := nextIndex - 1
	prevLogTerm, err1 := r.RaftLog.Term(prevLogIndex)
	entries, err2 := r.RaftLog.getEntries(nextIndex, r.RaftLog.LastIndex() + 1)
	if len(entries) == 0 && !isEmpty {
		return false
	}
	if err1 != nil || err2 != nil {
		// if leader fails to get prevLogTerm or get [nextIndex, lastIndex] entries.
		// leader need to send a snapshot to follower for a quick catch-up.
		snapshot, err := r.RaftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				logf("[term %d] s%x -> s%x: snapshot is temporarily unavailable",
					r.Term, r.id, to)
				return false
			}
			panic(err)
		}
		if IsEmptySnap(&snapshot) {
			logf("[term %d] s%x -> s%x: get an empty snapshot", r.Term, r.id, to)
			panic("empty snapshot")
		}
		appendMsg.MsgType = pb.MessageType_MsgSnapshot
		appendMsg.Snapshot = &snapshot
		logf("[term %d] s%x -> s%x MsgSnapshot: metadata[index: %d, term: %d]",
				r.Term, r.id, to, snapshot.Metadata.Index, snapshot.Metadata.Term)
	} else {
		// send append message with entries
		ents := make([]*pb.Entry, len(entries))
		for idx := range entries {
			ents[idx] = &entries[idx]
		}
		appendMsg.MsgType = pb.MessageType_MsgAppend
		appendMsg.Index = prevLogIndex
		appendMsg.LogTerm = prevLogTerm
		appendMsg.Entries = ents
		appendMsg.Commit = r.RaftLog.committed
		logf("[term %d] s%x -> s%x MsgAppend: prevlog[index: %d, term: %d], commit[%d], entries[%v]", 
			appendMsg.Term, appendMsg.From, appendMsg.To, appendMsg.Index, appendMsg.LogTerm, appendMsg.Commit, appendMsg.Entries)
	}
	r.msgs = append(r.msgs, appendMsg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	heartbestMsg := pb.Message {
		MsgType: pb.MessageType_MsgHeartbeat,
		To: to,
		From: r.id,
		Term: r.Term,
		Commit: min(r.Prs[to].Match, r.RaftLog.committed),
	}
	r.msgs = append(r.msgs, heartbestMsg)
	logf("[term %d] s%x -> s%x: MsgType[MsgHeartbeat]", heartbestMsg.Term, heartbestMsg.From, heartbestMsg.To)
}

// sendTimeoutNow sends a timeout local message to the given peer.
func (r *Raft) sendTimeoutNow(to uint64) {
	timeoutMsg := pb.Message {
		MsgType: pb.MessageType_MsgTimeoutNow,
		To: to,
		From: r.id,
	}
	r.msgs = append(r.msgs, timeoutMsg)
	logf("[term %d] s%x -> s%x: MsgType[MsgTimeoutNow]", timeoutMsg.Term, timeoutMsg.From, timeoutMsg.To)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	if r.State == StateLeader {
		r.heartbeatElapsed++
		r.electionElapsed++

		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			if r.State == StateLeader && r.leadTransferee != None {
				r.leadTransferee = None
			}
		}

		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			msg := pb.Message { MsgType: pb.MessageType_MsgBeat }
			// local message, handle it directly
			r.Step(msg)
		}
	} else {
		r.electionElapsed++

		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			msg := pb.Message { MsgType: pb.MessageType_MsgHup }
			// local message, handle it directly
			r.Step(msg)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.resetTerm(term)
	r.Lead = lead
	r.State = StateFollower
	logf("[term %d] s%d convert to follower", r.Term, r.id)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State == StateLeader {
		logf("[term %d] s%d try to convert from leader to candidate", r.Term, r.id)
		panic("becomeCandidate fail")
	}
	r.resetTerm(r.Term + 1)
	// candidate vote itself
	r.Vote = r.id
	r.votes[r.id] = true
	r.State = StateCandidate
	logf("[term %d] s%x convert to candidate", r.Term, r.id)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateFollower {
		logf("[term %d] s%d try to convert from follower to leader", r.Term, r.id)
		panic("becomeLeader fail")
	}
	r.resetTerm(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	logf("[term %d] s%x convert to leader", r.Term, r.id)

	// propose a noop entry
	noop := &pb.Entry{ Data: nil }
	r.leaderAppendEntries(noop)
	r.broadcastAppend()
}

// leaderAppendEntries append giving entries to leader`s log
func (r *Raft) leaderAppendEntries(entries ...*pb.Entry) {
	if r.State != StateLeader {
		logf("[term %d] s%x is not leader, can`t append entries", r.Term, r.id)
		panic("leader append entries fail")
	}
	if (len(entries) == 0) {
		logf("[term %d] append empty entries to leader s%x", r.Term, r.id)
		panic("leader append entries fail")
	}
	appendEntries := make([]pb.Entry, len(entries))
	for i, e := range entries {
		appendEntries[i].EntryType = e.EntryType
		appendEntries[i].Index = r.RaftLog.LastIndex() + uint64(i) + 1
		appendEntries[i].Term = r.Term
		appendEntries[i].Data = e.Data
	}
	r.RaftLog.appendEntries(appendEntries)
	logf("[term %d] leader s%x appned entries[%v] to log", r.Term, r.id, appendEntries)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	// if leader is the only one node in raft group, commit entries
	r.checkCommit()
}

// broadcastBeat sends heartbeat RPC from leader to each follower
func (r *Raft) broadcastBeat() {
	if r.State != StateLeader {
		logf("[term %d] s%d is not a leader but try to broadcast heartbeat")
		panic("broadcastBeat fail")
	}

	for _, peerId := range nodes(r) {
		if peerId == r.id {
			continue
		}
		r.sendHeartbeat(peerId)
	}
}

// broadcastBeat sends append RPC from leader to each follower
func (r *Raft) broadcastAppend() {
	if r.State != StateLeader {
		logf("[term %d] s%d is not a leader but try to broadcast appendMsg")
		panic("broadcastAppend fail")
	}

	for _, peerId := range nodes(r) {
		if peerId == r.id {
			continue
		}
		r.sendAppend(peerId)
	}
}

// handleProposal appends entries in proposal to leader`s log and
// broadcast it to all followers
func (r *Raft) handleProposal(m pb.Message) {
	if r.State != StateLeader {
		logf("[term %d] s%d is not a leader but try to handle proposal", r.Term, r.id)
		panic("handleProposal fail")
	}
	if len(m.Entries) == 0 {
		logf("[term %d] no entries to handle in proposal", r.Term)
		panic("handleProposal fail")
	}
	for i, e := range m.Entries {
		var cc pb.ConfChange
		if e.EntryType == pb.EntryType_EntryConfChange {
			if err := cc.Unmarshal(e.Data); err != nil {
				panic(err)
			}
			if r.PendingConfIndex > r.RaftLog.applied {
				logf("[term %d] only one conf change can be pending", r.Term)
				m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
			} else {
				r.PendingConfIndex = r.RaftLog.LastIndex()
			}
		}
	}
	r.leaderAppendEntries(m.Entries...)
	r.broadcastAppend()
}

// handleLeaderTransfer transfers leader to given peer (m.from)
func (r *Raft) handleLeaderTransfer(m pb.Message) {

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch r.State {
	case StateFollower:
		switch {
		case m.Term == 0:
			// local message
			switch m.MsgType {
			case pb.MessageType_MsgHup, pb.MessageType_MsgTimeoutNow:
				if r.elecetionCheck() {
					r.becomeCandidate()
					r.launchElection()
				}
			case pb.MessageType_MsgPropose:
				// redirect to leader
				if r.Lead == None {
					logf("[term %d] no leader in raft group now, ignore proposal", r.Term)
					return ErrProposalDropped
				}
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
				logf("[term %d] s%x redirect proposal from s%x to leader s%x", r.Term, r.id, m.From, m.To)
			case pb.MessageType_MsgTransferLeader:
				// redirect to leader
				if r.Lead == None {
					logf("[term %d] no leader in raft group now, ignore transfer leader request", r.Term)
				}
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
				logf("[term %d] s%x redirect transfer leader request from s%x to leader s%x", r.Term, r.id, m.From, m.To)
			}
		case m.Term > r.Term:
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				r.becomeFollower(m.Term, None)
				r.handleRequestVote(m)
			case pb.MessageType_MsgHeartbeat:
				r.becomeFollower(m.Term, m.From)
				r.handleRequestVote(m)
			case pb.MessageType_MsgAppend:
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			case pb.MessageType_MsgSnapshot:
				r.becomeFollower(m.Term, m.From)
				r.handleSnapshot(m)
			case pb.MessageType_MsgRequestVoteResponse, pb.MessageType_MsgHeartbeatResponse, pb.MessageType_MsgAppendResponse:
				r.becomeFollower(m.Term, None)
			case pb.MessageType_MsgHup:
				r.becomeFollower(m.Term, None)
				if r.elecetionCheck() {
					r.becomeCandidate()
					r.launchElection()
				}
			}
		case m.Term < r.Term:
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				respMsg := pb.Message {
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To: m.From,
					From: r.id,
					Term: r.Term,
					Reject: true,
				}
				r.msgs = append(r.msgs, respMsg)
				logf("[term %d] s%x reject vote to s%x: s%x`s term is higher than vote request",
					respMsg.Term, respMsg.From, respMsg.To, respMsg.From)
			case pb.MessageType_MsgHeartbeat:
				respMsg := pb.Message {
					MsgType: pb.MessageType_MsgHeartbeatResponse,
					To: m.From,
					From: r.id,
					Term: r.Term,
				}
				r.msgs = append(r.msgs, respMsg)
			case pb.MessageType_MsgAppend:
				respMsg := pb.Message {
					MsgType: pb.MessageType_MsgAppendResponse,
					To: m.From,
					From: r.id,
					Term: r.Term,
				}
				r.msgs = append(r.msgs, respMsg)
				logf("[term %d] s%x -> s%x AppendResponse: s%x`s term is higher than append request", 
					respMsg.Term, respMsg.From, respMsg.To, respMsg.From)
			}
		case m.Term == r.Term:
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				r.handleRequestVote(m)
			case pb.MessageType_MsgHeartbeat:
				r.resetElectionTimeout()
				r.Lead = m.From
				r.handleHeartbeat(m)
			case pb.MessageType_MsgAppend:
				r.resetElectionTimeout()
				r.Lead = m.From
				r.handleAppendEntries(m)
			case pb.MessageType_MsgSnapshot:
				r.resetElectionTimeout()
				r.Lead = m.From
				r.handleSnapshot(m)
			}
		}
	case StateCandidate:
		switch {
		case m.Term == 0:
			// local message
			switch m.MsgType {
			case pb.MessageType_MsgHup, pb.MessageType_MsgTimeoutNow:
				// election timeout, relaunch election
				if r.elecetionCheck() {
					r.becomeCandidate()
					r.launchElection()
				}
			case pb.MessageType_MsgPropose:
				logf("[term %d] s%x is candidate, ignore proposal", r.Term, r.id)
				return ErrProposalDropped
			case pb.MessageType_MsgTransferLeader:
				logf("[term %d] s%x is candidate, ignore transfer leader request", r.Term, r.id)
			}
		case m.Term > r.Term:
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				r.becomeFollower(m.Term, None)
				r.handleRequestVote(m)
			case pb.MessageType_MsgHeartbeat:
				r.becomeFollower(m.Term, m.From)
				r.handleHeartbeat(m)
			case pb.MessageType_MsgAppend:
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			case pb.MessageType_MsgSnapshot:
				r.becomeFollower(m.Term, m.From)
				r.handleSnapshot(m)
			case pb.MessageType_MsgRequestVoteResponse, pb.MessageType_MsgHeartbeatResponse, pb.MessageType_MsgAppendResponse:
				r.becomeFollower(m.Term, None)
			}
		case m.Term < r.Term:
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				respMsg := pb.Message {
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To: m.From,
					From: r.id,
					Term: r.Term,
					Reject: true,
				}
				r.msgs = append(r.msgs, respMsg)
				logf("[term %d] s%x reject vote to s%x: s%x`s term is higher than vote request",
					respMsg.Term, respMsg.From, respMsg.To, respMsg.From)
			case pb.MessageType_MsgHeartbeat:
				respMsg := pb.Message {
					MsgType: pb.MessageType_MsgHeartbeatResponse,
					To: m.From,
					From: m.To,
					Term: r.Term,
				}
				r.msgs = append(r.msgs, respMsg)
			case pb.MessageType_MsgAppend:
				respMsg := pb.Message {
					MsgType: pb.MessageType_MsgAppendResponse,
					To: m.From,
					From: r.id,
					Term: r.Term,
				}
				r.msgs = append(r.msgs, respMsg)
				logf("[term %d] s%x -> s%x AppendResponse: s%x`s term is higher than append request", 
					respMsg.Term, respMsg.From, respMsg.To, respMsg.From)
			}
		case m.Term == r.Term:
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				// candidate only vote itself
				respMsg := pb.Message {
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To: m.From,
					From: r.id,
					Reject: true,
				}
				r.msgs = append(r.msgs, respMsg)
				logf("[term %d] s%x reject vote to s%x: s%x is candidate, only vote itself",
					respMsg.Term, respMsg.From, respMsg.To, respMsg.From)
			case pb.MessageType_MsgHeartbeat:
				r.becomeFollower(m.Term, m.From)
				r.handleHeartbeat(m)
			case pb.MessageType_MsgAppend:
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			case pb.MessageType_MsgSnapshot:
				r.becomeFollower(m.Term, m.From)
				r.handleSnapshot(m)
			case pb.MessageType_MsgRequestVoteResponse:
				r.votes[m.From] = !m.Reject
				r.checkVotes()
			}
		}
	case StateLeader:
		switch {
		case m.Term == 0:
			// local message
			switch m.MsgType {
			case pb.MessageType_MsgBeat:
				r.broadcastBeat()
			case pb.MessageType_MsgPropose:
				if r.leadTransferee != None {
					logf("[term %d] leadership transfer from s%x -> s%x is in progress, ignore proposal", r.Term, r.id, r.leadTransferee)
					return ErrProposalDropped
				}
				// If current node is not a member of the range (i.e. this node
				// was removed from the configuration while serving as leader),
				// drop any new proposals.
				if r.Prs[r.id] == nil {
					return ErrProposalDropped
				}
				r.handleProposal(m)
			case pb.MessageType_MsgTransferLeader:
				r.handleLeaderTransfer(m)
			}
		case m.Term > r.Term:
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				r.becomeFollower(m.Term, None)
				r.handleRequestVote(m)
			case pb.MessageType_MsgHeartbeat:
				r.becomeFollower(m.Term, m.From)
				r.handleHeartbeat(m)
			case pb.MessageType_MsgAppend:
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			case pb.MessageType_MsgSnapshot:
				r.becomeFollower(m.Term, m.From)
				r.handleSnapshot(m)
			case pb.MessageType_MsgRequestVoteResponse, pb.MessageType_MsgHeartbeatResponse, pb.MessageType_MsgAppendResponse:
				r.becomeFollower(m.Term, None)
			}
		case m.Term < r.Term:
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				respMsg := pb.Message {
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To: m.From,
					From: r.id,
					Term: r.Term,
					Reject: true,
				}
				r.msgs = append(r.msgs, respMsg)
				logf("[term %d] s%x reject vote to s%x: s%x is the leader in this term", 
						respMsg.Term, respMsg.From, respMsg.To, respMsg.Reject, respMsg.From)
			case pb.MessageType_MsgHeartbeatResponse, pb.MessageType_MsgAppendResponse, pb.MessageType_MsgRequestVoteResponse:
				// leader receive response from a lower term, ignore it
			}
		case m.Term == r.Term:
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				respMsg := pb.Message {
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To: m.From,
					From: r.id,
					Term: r.Term,
					Reject: true,
				}
				r.msgs = append(r.msgs, respMsg)
				logf("[term %d] s%x reject vote to s%x: s%x is the leader in this term", 
						respMsg.Term, respMsg.From, respMsg.To, respMsg.Reject, respMsg.From)
			case pb.MessageType_MsgHeartbeatResponse:
				r.handleHeartbeatResponse(m)
			case pb.MessageType_MsgAppendResponse:
				r.handleAppendResponse(m)
			}
		}
	}
	return nil
}

// isUpToDate checks if the given (lastIndex, term) log is more up-to-date
// by comparing with the last entry in current log.
func (r *Raft) isUptoDate(i, t uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm := r.RaftLog.LastTerm()
	return t > lastTerm || (t == lastTerm && i >= lastIndex)
}

// handleRequestVote handles vote request from candidate
func (r *Raft) handleRequestVote(m pb.Message) {
	respMsg := pb.Message {
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To: m.From,
		From: r.id,
		Term: r.Term,
	}
	valid := r.isUptoDate(m.Index, m.LogTerm)
	if (r.Vote == None || r.Vote == m.From) && valid {
		r.Vote = m.From
		r.resetElectionTimeout()
		r.msgs = append(r.msgs, respMsg)
		logf("[term %d] s%x vote to s%x", respMsg.Term, respMsg.From, respMsg.To)
		return
	} else {
		respMsg.Reject = true
		r.msgs = append(r.msgs, respMsg)
		logf("[term %d] s%x reject vote to s%x: s%x has already voted or has an updated entry", 
				respMsg.Term, respMsg.From, respMsg.To, respMsg.From)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	respMsg := pb.Message {
		MsgType: pb.MessageType_MsgAppendResponse,
		To: m.From,
		From: r.id,
		Term: r.Term,
	}
	// update progress in leader
	if r.RaftLog.committed > m.Index {
		respMsg.Index = r.RaftLog.committed
		r.msgs = append(r.msgs, respMsg)
		logf("[term %d] s%x -> s%x AppendResponse: reject[%t], match index[%d]", 
				respMsg.Term, respMsg.From, respMsg.To, respMsg.Reject, respMsg.Index)
		return
	}
	// can`t find prevlog
	if r.RaftLog.LastIndex() < m.Index {
		respMsg.Reject = true
		respMsg.HintIndex = r.RaftLog.LastIndex() + 1
		r.msgs = append(r.msgs, respMsg)
		logf("[term %d] s%x -> s%x AppendResponse: reject[%t], hint index[%d]", 
				respMsg.Term, respMsg.From, respMsg.To, respMsg.Reject, respMsg.HintIndex)
		return
	}
	// find prevlog but term conflict
	term, err := r.RaftLog.Term(m.Index)
	if err != nil { panic("find term of prevlog fail") }
	if term != m.LogTerm {
		respMsg.Reject = true
		respMsg.HintTerm = term
		// find first entry that term equal to hint term
		for i := m.Index; i < r.RaftLog.stabled + 1; i-- {
			if t, _ := r.RaftLog.Term(i - 1); t != m.LogTerm {
				respMsg.HintIndex = i
				break
			}
		}
		r.msgs = append(r.msgs, respMsg)
		logf("[term %d] s%x -> s%x AppendResponse: reject[%t], hint index[%d], hint term[%d]", 
				respMsg.Term, respMsg.From, respMsg.To, respMsg.Reject, respMsg.HintIndex, respMsg.HintTerm)
		return
	}
	// find prevlog and no conflict, compare and compact entry (if necessary)
	for _, entry := range m.Entries {
		if entry.Index <= r.RaftLog.LastIndex() {
			term, err := r.RaftLog.Term(entry.Index)
			if term != entry.Term && err == nil {
				r.RaftLog.deleteEntries(entry.Index)
			}
		}
		if entry.Index > r.RaftLog.LastIndex() {
			m.Entries = m.Entries[entry.Index - m.Index - 1:]
			// TODO: 把下面这部分打包为一个函数
			appendEntries := make([]pb.Entry, len(m.Entries))
			for i, e := range m.Entries {
				appendEntries[i] = *e
			}
			r.RaftLog.appendEntries(appendEntries)
			logf("[term %d] s%x append entries[%v] to log", r.Term, r.id, appendEntries)
			break
		}
	}
	// update commit index
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index + uint64(len(m.Entries)))
	}
	// response to leader
	respMsg.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, respMsg)
	logf("[term %d] s%x -> s%x AppendResponse: reject[%t], match index[%d]", 
			respMsg.Term, respMsg.From, respMsg.To, respMsg.Reject, respMsg.Index)
}

// handleAppendResponse handle AppendEntries RPC response
func (r *Raft) handleAppendResponse(m pb.Message) {
	if r.Prs[m.From] == nil {
		logf("[term %d] s%x is removed", r.Term, r.id)
		return
	}
	if !m.Reject {
		r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
		r.Prs[m.From].Next = max(r.Prs[m.From].Next ,m.Index + 1)
		logf("[term %d] leader s%x update s%x`s progress: match index[%d], next index[%d]", 
				r.Term, r.id, m.From, r.Prs[m.From].Match, r.Prs[m.From].Next)
		r.checkCommit()
		// transferee has the latest log after append entries, send it MsgTimeoutNow to launch election
		if r.leadTransferee == m.From && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
			logf("[term %d] transferee s%x has the lastest log after append entries, send it MsgTimeoutNow to launch election",
				r.Term, r.leadTransferee)
			r.sendTimeoutNow(m.From)
		}
	} else {
		if m.HintTerm == 0 {
			// reject reason is can`t find prevlog
			r.Prs[m.From].Next = m.HintIndex
			r.sendAppendOrEmpty(m.From, false)
		} else {
			// reject reason is prevlog term conflict
			i := r.RaftLog.findLastIndexInTerm(m.HintTerm)
			if i > 0 {
				r.Prs[m.From].Next = i + 1
			} else {
				r.Prs[m.From].Next = m.HintIndex
			}
			r.sendAppendOrEmpty(m.From, false)
		}
	}
}

// checkCommit checks if there are entries can be commited
func (r *Raft) checkCommit() {
	if r.State != StateLeader {
		logf("[term %d] s%x is not leader, can`t check commit status", r.Term, r.id)
		return
	}
	for i := r.RaftLog.committed + 1; i <= r.RaftLog.LastIndex(); i++ {
		t, _ := r.RaftLog.Term(i)
		// only entries in current term can be committed
		if t != r.Term {
			continue
		}
		counter := 1
		for _, peerId := range nodes(r) {
			if peerId != r.id && r.Prs[peerId].Match >= i {
				counter++
			}
			if counter > len(r.Prs) / 2 {
				logf("[term %d] leader s%x commit entry [index: %d, term: %d]",
						r.Term, r.id, i, t)
				r.RaftLog.commitEntries(i)
				r.broadcastAppend()
				break
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.RaftLog.committed = max(r.RaftLog.committed, m.Commit)
	respMsg := pb.Message {
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To: m.From,
		From: r.id,
		Term: r.Term,
	}
	r.msgs = append(r.msgs, respMsg)
}

// handleHeartbeat handle Heartbeat RPC response
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.Prs[m.From] == nil {
		logf("[term %d] s%x is removed", r.Term, r.id)
		return
	}
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// applySnapshot recovers the state machine from given snapshot. It restore
// the log and the configuration of state machine.
func (r *Raft) applySnapshot(s pb.Snapshot) bool {
	return true
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
