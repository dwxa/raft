package main

import (
	"context"
	"cuhk/asgn/raft"
	"fmt"
	"log"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

func main() {
	ports := os.Args[2]
	myport, _ := strconv.Atoi(os.Args[1])
	nodeID, _ := strconv.Atoi(os.Args[3])
	heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	electionTimeout, _ := strconv.Atoi(os.Args[5])

	portStrings := strings.Split(ports, ",")

	// A map where
	// 		the key is the node id
	//		the value is the {hostname:port}
	nodeidPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeidPortMap[i] = port
	}

	// Create and start the Raft Node.
	_, err := NewRaftNode(myport, nodeidPortMap,
		nodeID, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever.
	select {}
}

type raftNode struct {
	mu                sync.Mutex
	currentTerm       int32
	votedFor          int32
	log               []*raft.LogEntry
	commitIndex       int32
	kvstore           map[string]int32
	serverState       raft.Role
	electionTimeout   int32
	heartBeatInterval int32
	finishChan        chan bool
	resetChan         chan bool
	commitChan        chan int32
	majoritySize      int
	matchIndex        []int32
	nextIndex         []int32
}

// Desc:
// NewRaftNode creates a new RaftNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes.
//
// Params:
// myport: the port of this new node. We use tcp in this project.
//
//	Note: Please listen to this port rather than nodeidPortMap[nodeId]
//
// nodeidPortMap: a map from all node IDs to their ports.
// nodeId: the id of this node
// heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
// electionTimeout: The election timeout for this node. In millisecond.
func NewRaftNode(myport int, nodeidPortMap map[int]int, nodeId, heartBeatInterval,
	electionTimeout int) (raft.RaftNodeServer, error) {

	//remove myself in the hostmap
	delete(nodeidPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	rn := raftNode{
		mu:                sync.Mutex{},
		currentTerm:       0,
		votedFor:          -1,
		log:               nil,
		commitIndex:       0,
		kvstore:           make(map[string]int32),
		serverState:       raft.Role_Follower,
		electionTimeout:   int32(electionTimeout),
		heartBeatInterval: int32(heartBeatInterval),
		finishChan:        make(chan bool, 1),
		resetChan:         make(chan bool, 1),
		commitChan:        make(chan int32, 1),
		majoritySize:      (len(nodeidPortMap)+1)/2 + 1,
		nextIndex:         make([]int32, len(nodeidPortMap)+1),
		matchIndex:        make([]int32, len(nodeidPortMap)+1),
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myport))

	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, &rn)

	log.Printf("Start listening to port: %d", myport)
	go s.Serve(l)

	//Try to connect nodes
	for tmpHostId, hostPorts := range nodeidPortMap {
		hostId := int32(tmpHostId)
		numTry := 0
		for {
			numTry++

			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			//defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[hostId] = client
				break
			}
		}
	}
	log.Printf("Successfully connect all nodes")
	ctx := context.Background()
	rn.log = append(rn.log, &raft.LogEntry{})

	go func() {
		for {
			switch rn.serverState {
			case raft.Role_Follower:
				flag := true
				for flag {
					select {
					case <-time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
						{
							fmt.Printf("id: %v,timeout,become candidate\n", nodeId)
							// If timer times out, the raft node becomes the candidate
							rn.serverState = raft.Role_Candidate
							rn.finishChan <- true

						}
					case <-rn.resetChan:
						// Do nothing
					case <-rn.finishChan:
						flag = false
					}
				}
			case raft.Role_Candidate:
				flag := true
				for flag {
					rn.currentTerm++
					rn.votedFor = int32(nodeId)
					voteNum := 1
					for hostId, client := range hostConnectionMap {
						go func(hostId int32, client raft.RaftNodeClient) {
							r, err := client.RequestVote(
								ctx,
								&raft.RequestVoteArgs{
									From:         int32(nodeId),
									To:           int32(hostId),
									Term:         rn.currentTerm,
									CandidateId:  int32(nodeId),
									LastLogIndex: int32(0),
									LastLogTerm:  int32(0),
								},
							)
							if err == nil && r.VoteGranted == true && r.Term == rn.currentTerm && rn.serverState == raft.Role_Candidate {
								// TODO: Lock
								rn.mu.Lock()
								voteNum++
								if voteNum >= rn.majoritySize && rn.serverState == raft.Role_Candidate {
									rn.serverState = raft.Role_Leader
									fmt.Printf("nodeId: %vbecome leader\n", nodeId)
									rn.finishChan <- true
								}
								// TODO: Unlock
								rn.mu.Unlock()
							} else if r.Term > rn.currentTerm {
								rn.currentTerm = r.Term
								rn.serverState = raft.Role_Follower
								rn.votedFor = -1
								//todo
								// What if the other node has larger term?
							}
						}(hostId, client)
					}
					select {
					case <-time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
						fmt.Println("********************************************************************")
						// If the election times out, start a new election
					case <-rn.resetChan:
						if rn.serverState == raft.Role_Candidate {
							flag = true
						} else {
							flag = false
						}
						fmt.Printf("resetnodeId: %v当前重置\n", nodeId)
						// Do nothing
					case <-rn.finishChan:
						flag = false
					}
				}
			case raft.Role_Leader:
				//nextIndex[]对于每台服务器，要发送到该服务器的下一个日志条目的索引（初始化为leaderlast日志索引+1）
				// Initialize the nextIndex and matchIndex
				flag := true
				initial := true
				commitNum := 1
				interval := int32(0)
				last := false
				//初始化nextIndex和matchIndex
				for m := range rn.nextIndex {
					rn.matchIndex[m] = 0
					rn.nextIndex[m] = rn.commitIndex + 1
				}
				for flag {
					select {
					//send the heartbeat
					case <-time.After(time.Duration(interval) * time.Millisecond):
						if initial {
							initial = false
							interval = rn.heartBeatInterval
						}
						// commitNum := 1
						minCommitIndex := int32(0)
						for k := range rn.matchIndex {
							if rn.matchIndex[k] != 0 {
								if minCommitIndex == 0 {
									minCommitIndex = rn.matchIndex[k]
								} else if rn.matchIndex[k] < minCommitIndex {
									minCommitIndex = rn.matchIndex[k]
								}
							}
						}
						if commitNum != 1 {
							last = true
						}
						fmt.Printf("=====\n")
						fmt.Printf("leader's kv:%v\n", rn.kvstore)
						fmt.Printf("=====\n")
						for hostId, client := range hostConnectionMap {
							// Get prevLogIndex and prevLogTerm
							//prevlogindex 上一次发送的日志index
							// prevLogIndex := int32(0)
							// prevLogTerm := int32(0)
							prevLogIndex := rn.nextIndex[hostId] - 1
							prevLogTerm := rn.log[prevLogIndex].Term
							sendLog := []*raft.LogEntry{}
							if !initial && int32(len(rn.log)) >= prevLogIndex+1 {
								//leader需要发送的log
								sendLog = rn.log[prevLogIndex+1:]
							}
							leaderCommit := rn.commitIndex
							fmt.Printf("node %v prevLogIndex is %v\n", hostId, prevLogIndex)

							//If last log index ≥ nextIndex for a follower: sendAppendEntries RPC with log entries starting at nextIndex
							//If successful: update nextIndex and matchIndex for follower
							//If AppendEntries fails because of log inconsistency:decrement nextIndex and retry
							go func(hostId int32, client raft.RaftNodeClient) {

								r, err := client.AppendEntries(
									ctx,
									&raft.AppendEntriesArgs{
										From:         int32(nodeId),
										To:           int32(hostId),
										Term:         rn.currentTerm,
										LeaderId:     int32(nodeId),
										PrevLogIndex: prevLogIndex,
										PrevLogTerm:  prevLogTerm,
										Entries:      sendLog,
										LeaderCommit: leaderCommit,
									},
								)
								fmt.Printf("test\n")
								if err == nil && r.Success == true && int32(len(sendLog)) != 0 {
									rn.mu.Lock()
									// Count how many nodes have committed the log
									//leader的日志追加到follower上，计数+1

									if r.MatchIndex == int32(len(rn.log)-1) {
										commitNum++
									}
									rn.mu.Unlock()
									//追加到大多数节点上，leader提交日志
									if commitNum >= rn.majoritySize && rn.serverState == raft.Role_Leader {
										//leader commit the log
										//向通道发送commit的日志索引
										fmt.Printf("leader has committed\n")
										fmt.Printf("last is %v\n", last)
										commitNum = 1
										if last {
											fmt.Printf("log[%v] has been commited\n", minCommitIndex)
											last = false
											rn.commitChan <- minCommitIndex
										} else {
											fmt.Printf("log[%v] has been commited\n", r.MatchIndex)
											rn.commitChan <- r.MatchIndex
										}
										//n.finishChan <- true
									}
									// Update nextIndex and matchIndex
									rn.matchIndex[hostId] = r.MatchIndex
									rn.nextIndex[hostId] = rn.matchIndex[hostId] + 1
									fmt.Printf("node %v nextLogIndex is %v\n", hostId, rn.nextIndex[hostId])

									// rn.commitChan <- true
								} else if err == nil && r.Success == true && int32(len(sendLog)) != 0 {

								} else if err == nil && r.Success == false {
									//nextIndex 值减 1 并重试，直到 AppendEntries 成功
									rn.mu.Lock()
									rn.nextIndex[hostId]--

									rn.mu.Unlock()
								}
							}(hostId, client)
						}
					// No need to reset timer for the leader
					case <-rn.finishChan:
						flag = false
					}

				}
			}
		}
	}()

	return &rn, nil
}

// Desc:
// Propose initializes proposing a new operation, and replies with the
// result of committing this operation. Propose should not return until
// this operation has been committed, or this node is not leader now.
//
// If the we put a new <k, v> pair or deleted an existing <k, v> pair
// successfully, it should return OK; If it tries to delete an non-existing
// key, a KeyNotFound should be returned; If this node is not leader now,
// it should return WrongNode as well as the currentLeader id.
//
// Params:
// args: the operation to propose
// reply: as specified in Desc
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	log.Printf("Receive propose from client")
	var ret raft.ProposeReply
	//only the leader can handel the permission
	if rn.serverState == raft.Role_Leader {
		ret.CurrentLeader = rn.votedFor
		if args.Op == raft.Operation_Delete {
			//check whether the key exist
			//if existed,set ok to true, otherwise false
			if _, ok := rn.kvstore[args.Key]; ok {
				ret.Status = raft.Status_OK
			} else {
				ret.Status = raft.Status_KeyNotFound
			}
		} else {
			ret.Status = raft.Status_OK
		}
	} else {
		ret.CurrentLeader = rn.votedFor
		ret.Status = raft.Status_WrongNode
	}
	if ret.Status == raft.Status_OK || ret.Status == raft.Status_KeyNotFound {
		rn.log = append(rn.log, &raft.LogEntry{Term: rn.currentTerm, Op: args.Op, Key: args.Key, Value: args.V})

		<-rn.commitChan
		if ret.Status == raft.Status_KeyNotFound && args.Op == raft.Operation_Delete {
			if _, ok := rn.kvstore[args.Key]; ok {
				ret.Status = raft.Status_OK
			} else {
				ret.Status = raft.Status_KeyNotFound
			}
		}
		//lock
		rn.mu.Lock()
		if ret.Status == raft.Status_OK {
			if args.Op == raft.Operation_Put {
				rn.kvstore[args.Key] = args.V
			} else if args.Op == raft.Operation_Delete {
				delete(rn.kvstore, args.Key)
			}
		}
		//unlock
		rn.mu.Unlock()
		rn.commitIndex++
	}

	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	var ret raft.GetValueReply
	rn.mu.Lock()
	if val, ok := rn.kvstore[args.Key]; ok {
		ret.V = val
		ret.Status = raft.Status_KeyFound
	} else {
		ret.V = 0
		ret.Status = raft.Status_KeyNotFound
	}
	rn.mu.Unlock()
	return &ret, nil
}

// Desc:
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the RequestVote Reply Message
func (rn *raftNode) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	reply.From = args.To
	reply.To = args.From
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		fmt.Printf("args.To: %v节点\n", args.To)
		rn.serverState = raft.Role_Follower
		fmt.Printf("rn.serverState: %v\n", rn.serverState)
		reply.VoteGranted = false
		rn.votedFor = -1
	}
	// Add more conditions here
	if args.Term == rn.currentTerm && (rn.votedFor == -1 || rn.votedFor == args.CandidateId) {
		rn.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
	if args.Term < rn.currentTerm {
		reply.VoteGranted = false
	}

	if reply.VoteGranted == true {
		rn.resetChan <- true
		// reset the timer to avoid timeout
	}
	reply.Term = rn.currentTerm

	return &reply, nil
}

// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	//term < currentterm 时返回false
	//日志中不包含index 值和 Term ID 与 prevLogIndex 和 prevLogTerm 相同的记录，返回 false
	var reply raft.AppendEntriesReply
	reply.From = args.To
	reply.To = args.From
	reply.Success = true
	//receive heartbeat from the new leader
	//领导者term大于当前term
	if args.Term >= rn.currentTerm {
		rn.votedFor = args.From
		rn.currentTerm = args.Term
		//当前不是follower
		if rn.serverState != raft.Role_Follower {
			rn.serverState = raft.Role_Follower
			//todo
			//当前是follower，重置timer
		} else if rn.serverState == raft.Role_Follower {
			rn.resetChan <- true
			//todo
		}
	}
	reply.Term = rn.currentTerm
	//日志中不包含index 值和 Term ID 与 prevLogIndex 和 prevLogTerm 相同的记录，返回 false

	//当不是心跳时，如果prevlogindex<rn.loglen(leader不知道你已经append log了)，核对前面的appendlog，将prevlogindex指向rn.log最后一条
	if int32(len(args.Entries)) != 0 { //不是心跳
		if int32(len(rn.log)-1) > args.PrevLogIndex {
			for j := int32(args.PrevLogIndex); j < int32(len(rn.log)-1); j++ {

				if reflect.DeepEqual(rn.log[j+1], args.Entries[j]) {

					args.PrevLogIndex = j + 1
					args.PrevLogTerm = rn.log[args.PrevLogIndex].Term
				}
			}
			args.Entries = args.Entries[args.PrevLogIndex:]
		}

	}

	if int32(len(rn.log)) == 1 {
		if args.Term < rn.currentTerm {
			reply.Success = false

		}
	} else if int32(len(rn.log)) > 1 {
		if int32(len(rn.log)-1) < args.PrevLogIndex {
			reply.Success = false

			//todo
		} else if rn.log[int32(len(rn.log)-1)].Term != args.PrevLogTerm {
			reply.Success = false

		}
	}
	// fmt.Printf("node %v len(rn.log)-1 is %v, log[(len(rn.log)-1)].Term is %v, PrevLogTerm is %v,PrevLogIndex is %v, appendentries is %v\n", reply.From, int32(len(rn.log)-1), rn.log[int32(len(rn.log)-1)].Term, args.PrevLogTerm, args.PrevLogIndex, args.Entries)

	//follower在args.PrevLogIndex处与leader的日志匹配
	//如果日志中存在与正在备份的日志记录相冲突的记录（有相同的 index 值但 Term ID 不同），删除该记录以及之后的所有记录

	if reply.Success {
		if int32(len(rn.log)) == 1 { //表示现在node log是空表
			rn.log = append(rn.log, args.Entries...)

		} else if int32(len(rn.log)) > 1 { //非空
			if args.PrevLogTerm == rn.log[int32(len(rn.log)-1)].Term && args.PrevLogIndex == int32(len(rn.log)-1) {
				rn.log = rn.log[0 : args.PrevLogIndex+1]
				rn.log = append(rn.log, args.Entries...)

			}

		}
	}
	reply.MatchIndex = int32(len(rn.log) - 1)

	fmt.Printf("node %v old commitIndex %v\n", reply.From, rn.commitIndex)
	if rn.commitIndex < args.LeaderCommit && args.LeaderCommit <= int32(len(rn.log)-1) {
		for rn.commitIndex < args.LeaderCommit {

			rn.commitIndex++
			if rn.log[rn.commitIndex].Op == raft.Operation_Put {
				rn.kvstore[rn.log[rn.commitIndex].Key] = rn.log[rn.commitIndex].Value
			} else if rn.log[rn.commitIndex].Op == raft.Operation_Delete {
				delete(rn.kvstore, rn.log[rn.commitIndex].Key)
			}
		}

		// rn.mu.Lock()
	}
	// fmt.Printf("node %v new loglen is%v, new log is %v\n", reply.From, int32(len(rn.log)), rn.log)
	fmt.Printf("node %v new commitIndex %v\n", reply.From, rn.commitIndex)
	fmt.Printf("node %v kvstore %v\n", reply.From, rn.kvstore)
	return &reply, nil
}

// Desc:
// Set electionTimeOut as args.Timeout milliseconds.
// You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.

// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetElectionTimeout(ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	var reply raft.SetElectionTimeoutReply
	rn.electionTimeout = args.Timeout
	rn.resetChan <- true
	return &reply, nil
}

// Desc:
// Set heartBeatInterval as args.Interval milliseconds.
// You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetHeartBeatInterval(ctx context.Context, args *raft.SetHeartBeatIntervalArgs) (*raft.SetHeartBeatIntervalReply, error) {
	var reply raft.SetHeartBeatIntervalReply
	rn.heartBeatInterval = args.Interval
	rn.resetChan <- true
	return &reply, nil
}

// NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}
