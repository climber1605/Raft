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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	Follower = iota
	Candidate
	Leader
	HeartbeatInterval = 50 * time.Millisecond
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all server
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leader
	nextIndex  []int
	matchIndex []int

	state     int // indicate the state of the server, i.e. follower, candidate or leader
	voteCount int // state for candidate

	//channels
	chanGrantVote     chan bool     // indicate a follower has voted for a candidate
	chanAppendEntries chan bool     // indicate a follower receives AppendEntries RPC from current leader. or a candidate receives AppendEntries RPC from new leader
	chanWinElection   chan bool     // indicate a candidate wins the election
	chanApply         chan ApplyMsg // will be used in Make() and config.go to get committed log entries of the Raft server
	chanExit          chan bool     // will be used in Kill() and Make() to kill the goroutine in Make()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.state == Leader
	return term, isleader
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) apply() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		msg := ApplyMsg{Index: rf.log[rf.lastApplied].Index, Command: rf.log[rf.lastApplied].Command}
		rf.chanApply <- msg
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

func getRandomElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(150)) * time.Millisecond
}

func dropAndSet(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date
	upToDate := false
	if args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
		upToDate = true
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		dropAndSet(rf.chanGrantVote)
		reply.Term = args.Term
		reply.VoteGranted = true
		//DPrintf("server %v grantVote to server %v in term %v", rf.me, args.CandidateID, args.Term)
	} else {
		reply.Term = args.Term
		reply.VoteGranted = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// A candidate starts election by sending RequestVote RPCs to all other servers and then handles the reply.
func (rf *Raft) startLeaderElection() {
	rf.mu.Lock()
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogTerm = rf.getLastLogTerm()
	args.LastLogIndex = rf.getLastLogIndex()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state != Candidate {
			//DPrintf("candidate converted to follower")
			return
		}
		go func(i int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, args, &reply)
			//DPrintf("candidate %v send RequestVote RPC to server %v int term %v", rf.me, i, args.Term)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				//fmt.Printf("currentTerm of server %d is %d, replyTerm from server %d is %d\n", rf.me, rf.currentTerm, i, reply.Term)
				rf.becomeFollower(reply.Term)
				return
			}
			// If the candidate has converted to follower or leader, or current term has changed, there is no need
			// to count votes for current term any more.
			if rf.state != Candidate || rf.currentTerm != args.Term {
				//fmt.Printf("state of server %d is %d, currentTerm is %d, argsTerm is %d\n", rf.me, rf.state, rf.currentTerm, args.Term)
				return
			}
			if reply.VoteGranted {
				rf.voteCount++
				//DPrintf("Candidate %v gets %v votes in term %v", rf.me, rf.voteCount, rf.currentTerm)
				if rf.voteCount > len(rf.peers)/2 {
					rf.becomeLeader()
					dropAndSet(rf.chanWinElection) // tell the candidate to reset it's election timeout
					//DPrintf("candidate %v becomes leader in term %v", rf.me, rf.currentTerm)
				}
			}
		}(i)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// optimized to reduce the number of rejected AppendEntries RPCs
	ConflictTerm  int // the term of the conflicting entry
	ConflictIndex int // the first index it stores for conflict term
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		//DPrintf("server %v becomes follower in term %v due to outdated term when receiving AppendEntries RPC", rf.me, rf.currentTerm)
	}
	//Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		//DPrintf("server %v reply false because term %v < %v", rf.me, args.Term, rf.currentTerm)
		return
	}
	//DPrintf("candidate %v becomes follower in term %v due to receiving AppendEntries RPC", rf.me, rf.currentTerm)
	// If a candidate reveives this AppendEntries RPC from a leader in the same term, then it should conver to a follower
	rf.state = Follower
	dropAndSet(rf.chanAppendEntries)
	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.getLastLogIndex() || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Term = args.Term
		reply.Success = false
		if args.PrevLogIndex > rf.getLastLogIndex() {
			reply.ConflictTerm = 0
			reply.ConflictIndex = len(rf.log)
		} else {
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			index := args.PrevLogIndex
			for index > 0 && rf.log[index].Term == rf.log[args.PrevLogIndex].Term {
				index--
			}
			reply.ConflictIndex = index + 1
		}
		//DPrintf("server %v replys false because log match fails", rf.me)
		return
	}

	reply.Term = args.Term
	reply.Success = true

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow i
	// Append any new entries not already in the log
	index := args.PrevLogIndex + 1
	for i := range args.Entries {
		if index > rf.getLastLogIndex() {
			rf.log = append(rf.log, args.Entries[i])
			//DPrintf("Server %v appends an entry{%v, %v, %v}", rf.me, args.Entries[i].Index, args.Entries[i].Term, args.Entries[i].Command)
		} else if rf.log[index].Term != args.Entries[i].Term {
			rf.log = rf.log[0:index]
			rf.log = append(rf.log, args.Entries[i]) // Append any new entries not already in the log
			//DPrintf("Server %v deletes entries from index %v and appends an entry{%v, %v, %v}", rf.me, index, args.Entries[i].Index, args.Entries[i].Term, args.Entries[i].Command)
		}
		index++
	}
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex := rf.getLastLogIndex()
		if args.LeaderCommit < lastLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastLogIndex
		}
		//DPrintf("Server %v applys logs between %v and %v", rf.me, rf.lastApplied+1, rf.commitIndex)
		rf.apply()
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// A leader starts log replication by sending AppendEntries RPCs to all other servers and then handles the reply
func (rf *Raft) startLogReplication() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderID = rf.me
		args.PrevLogIndex = rf.nextIndex[i] - 1
		//DPrintf("prevLogIndex of server %v is %v", i, args.PrevLogIndex)
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		entries := make([]LogEntry, 0) // the log entries are empty for assignment 1
		args.Entries = append(entries, rf.log[rf.nextIndex[i]:]...)
		args.LeaderCommit = rf.commitIndex
		//DPrintf("server %v send AppendEntries RPC to server %v, lentgh of log entries is %v", rf.me, i, len(args.Entries))
		if rf.state != Leader {
			//DPrintf("leader converted to follower")
			return
		}
		rf.mu.Unlock()

		go func(i int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(i, args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.state != Leader || rf.currentTerm != args.Term {
				return
			}
			// If successful: update nextIndex and matchIndex for follower
			if reply.Success {
				rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[i] = rf.matchIndex[i] + 1
				// If there exists an N such that N > commitIndex, a majority of matchIndex[k] ≥ N,
				// and log[N].term == currentTerm: set commitIndex = N
				N := rf.commitIndex
				for j := rf.commitIndex + 1; j <= rf.getLastLogIndex(); j++ {
					count := 1 // count the number of matchIndex that satisfies matchIndex[k] >= N
					for k := range rf.matchIndex {
						if k != rf.me && rf.matchIndex[k] >= j {
							count++
						}
					}
					if count > len(rf.matchIndex)/2 && rf.log[j].Term == rf.currentTerm {
						N = j
					}
				}
				if N != rf.commitIndex {
					rf.commitIndex = N
					//DPrintf("Leader %v of term %v applys logs between %v and %v", rf.me, rf.currentTerm, rf.lastApplied+1, rf.commitIndex)
					// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
					// may apply more than an entry at a time
					rf.apply()
				}

			} else {
				// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
				// Here I use the optimization method mentioned in the paper $5.3
				index := rf.nextIndex[i] - 1
				for index >= reply.ConflictIndex {
					if rf.log[index].Term == reply.ConflictTerm {
						break
					}
					index--
				}
				//DPrintf("leader %v backs nextIndex[%v] from %v to %v", rf.me, i, rf.nextIndex[i], index + 1)
				rf.nextIndex[i] = index + 1
			}
		}(i, args)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		index = rf.getLastLogIndex() + 1
		entry := LogEntry{Index: index, Term: term, Command: command}
		rf.log = append(rf.log, entry)
		//DPrintf("Leader %v of term %v appends an entry{%v, %v, %v}", rf.me, rf.currentTerm, index, term, command)
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	dropAndSet(rf.chanExit)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0})

	rf.state = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanAppendEntries = make(chan bool, 1)
	rf.chanGrantVote = make(chan bool, 1)
	rf.chanWinElection = make(chan bool, 1)
	rf.chanExit = make(chan bool, 1)
	rf.chanApply = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// seed for random election timeout
	rand.Seed(time.Now().UnixNano())

	go func() {
		for {
			select {
			case <-rf.chanExit:
				return
			default:
			}
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			switch state {
			case Follower:
				electionTimeout := getRandomElectionTimeout()
				// If election timeout elapses without receiving AppendEntries RPC from current leader
				// or granting vote to candidate: convert to candidate
				select {
				case <-rf.chanGrantVote:
				case <-rf.chanAppendEntries:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.becomeCandidate()
					rf.mu.Unlock()
					//DPrintf("follower %v becomes candidate in term %v after %v electionTimeout", rf.me, rf.currentTerm, electionTimeout)
				}
			case Candidate:
				electionTimeout := getRandomElectionTimeout()
				go rf.startLeaderElection()
				//DPrintf("candidate %v issues leader election in term %v", rf.me, rf.currentTerm)
				//A candidate continues in this state until one of three things happens:
				//(a) it wins the election, (b) another server establishes itself as leader, or (c) a period of time goes by with no winner
				select {
				case <-rf.chanWinElection:
					//DPrintf("candidate %v win election in term %v", rf.me, rf.currentTerm)
				case <-rf.chanAppendEntries:
					//DPrintf("candidate %v receives append entries RPC in term %v", rf.me, rf.currentTerm)
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.becomeCandidate()
					rf.mu.Unlock()
					//DPrintf("candidate %v keeps as candidate in term %v after %v electionTimeout", rf.me, rf.currentTerm, electionTimeout)
				}
			case Leader:
				rf.startLogReplication()
				time.Sleep(HeartbeatInterval)
			}
		}
	}()

	return rf
}
