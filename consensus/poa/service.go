// SPDX-License-Identifier: LGPL-3.0-or-later
// Copyright 2019 DNA Dev team
//
/*
 * Copyright (C) 2018 The ontology Authors
 * This file is part of The ontology library.
 *
 * The ontology is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The ontology is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with The ontology.  If not, see <http://www.gnu.org/licenses/>.
 */

package poa

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/DNAProject/DNA/account"
	"github.com/DNAProject/DNA/common"
	"github.com/DNAProject/DNA/common/log"
	actorTypes "github.com/DNAProject/DNA/consensus/actor"
	"github.com/DNAProject/DNA/consensus/vbft/config"
	"github.com/DNAProject/DNA/core/ledger"
	"github.com/DNAProject/DNA/core/payload"
	"github.com/DNAProject/DNA/core/types"
	"github.com/DNAProject/DNA/core/utils"
	"github.com/DNAProject/DNA/events"
	"github.com/DNAProject/DNA/events/message"
	p2pmsg "github.com/DNAProject/DNA/p2pserver/message/types"
	gover "github.com/DNAProject/DNA/smartcontract/service/native/governance"
	ninit "github.com/DNAProject/DNA/smartcontract/service/native/init"
	nutils "github.com/DNAProject/DNA/smartcontract/service/native/utils"
	"github.com/DNAProject/DNA/validator/increment"
	"github.com/ontio/ontology-crypto/keypair"
	"github.com/ontio/ontology-crypto/vrf"
	"github.com/ontio/ontology-eventbus/actor"
)

type ServiceActionType uint8

const (
	ProposeBlock ServiceActionType = iota
	PrepareBlock
	CommitBlock
	SealBlock
	FastForward // for syncer catch up
	ReBroadcast
	SubmitBlock
	MakeResponse
)

const (
	CAP_MESSAGE_CHANNEL  = 64
	CAP_ACTION_CHANNEL   = 1024
	CAP_MSG_SEND_CHANNEL = 16
	CAP_ROUNDS_IN_VOTE   = 16
)

const (
	UNKNOWN_VIEW  = math.MaxUint32
	UNKNOWN_EPOCH = math.MaxUint32
)

type ServiceAction struct {
	Type     ServiceActionType
	BlockNum uint32
	View     uint32
}

type BlockParticipantConfig struct {
	BlockNum    uint32
	VrfSeed     vconfig.VRFValue
	ChainConfig *vconfig.ChainConfig
	Leader      uint32
	Proactors   []uint32
}

type p2pMsgPayload struct {
	fromPeer uint32
	payload  *p2pmsg.ConsensusPayload
}

type Server struct {
	Index         uint32
	account       *account.Account
	poolActor     *actorTypes.TxPoolActor
	p2p           *actorTypes.P2PActor
	ledger        *ledger.Ledger
	incrValidator *increment.IncrementValidator
	pid           *actor.PID

	// some config
	msgHistoryDuration uint32

	//
	// Note:
	// 1. locking priority: metaLock > blockpool.Lock > peerpool.Lock
	// 2. should never take exclusive lock on both blockpool and peerpool at the same time.
	// 3. msgpool.Lock is independent, should have no exclusive overlap with other locks.
	//
	metaLock           sync.RWMutex
	completedBlockNum  uint32 // ledger SaveBlockCompleted block num
	LastConfigBlockNum uint32
	//config                   *vconfig.ChainConfig
	//currentParticipantConfig *BlockParticipantConfig

	chainStore *ChainStore // block store
	msgPool    *MsgPool    // consensus msg pool
	rspMsgs    *ResponseMsgPool
	blockPool  *BlockPool  // received block proposals
	peerPool   *PeerPool   // consensus peers
	configPool *ConfigPool // chain config pool
	syncer     *Syncer
	stateMgr   *StateMgr
	timer      *EventTimer

	msgRecvC map[uint32]chan *p2pMsgPayload
	msgC     chan ConsensusMsg
	actionC  chan *ServiceAction
	msgSendC chan *SendMsgEvent
	sub      *events.ActorSubscriber
	quitC    chan struct{}
	quit     bool
	quitWg   sync.WaitGroup
}

func NewPoAServer(account *account.Account, txpool, p2p *actor.PID) (*Server, error) {
	server := &Server{
		msgHistoryDuration: 64,
		account:            account,
		poolActor:          &actorTypes.TxPoolActor{Pool: txpool},
		p2p:                &actorTypes.P2PActor{P2P: p2p},
		ledger:             ledger.DefLedger,
		incrValidator:      increment.NewIncrementValidator(20),
	}
	server.stateMgr = newStateMgr(server)

	props := actor.FromProducer(func() actor.Actor {
		return server
	})

	pid, err := actor.SpawnNamed(props, "consensus_poa")
	if err != nil {
		return nil, err
	}
	server.pid = pid
	server.sub = events.NewActorSubscriber(pid)

	if err := server.initialize(); err != nil {
		return nil, fmt.Errorf("vbft server start failed: %s", err)
	}
	return server, nil
}

func (self *Server) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *actor.Restarting:
		log.Info("poa server restarting")
	case *actor.Stopping:
		log.Info("poa server stopping")
	case *actor.Stopped:
		log.Info("poa server stopped")
	case *actor.Started:
		log.Info("poa server started")
	case *actor.Restart:
		log.Info("poa server restart")
	case *actorTypes.StartConsensus:
		log.Info("poa server start consensus")
	case *actorTypes.StopConsensus:
		self.stop()
	case *message.SaveBlockCompleteMsg:
		log.Infof("poa server SaveBlockCompleteMsg receives block complete event. block height=%d, numtx=%d",
			msg.Block.Header.Height, len(msg.Block.Transactions))
		self.handleBlockPersistCompleted(msg.Block)
	case *message.BlockConsensusComplete:
		log.Infof("poa server  BlockConsensusComplete receives block complete event. block height=%d, numtx=%d",
			msg.Block.Header.Height, len(msg.Block.Transactions))
		self.handleBlockPersistCompleted(msg.Block)
	case *p2pmsg.ConsensusPayload:
		self.NewConsensusPayload(msg)

	default:
		log.Info("poa server: Unknown msg ", msg, "type", reflect.TypeOf(msg))
	}
}

func (self *Server) GetPID() *actor.PID {
	return self.pid
}

func (self *Server) Start() error {
	return self.start()
}

func (self *Server) Halt() error {
	self.pid.Tell(&actorTypes.StopConsensus{})
	return nil
}

func (self *Server) handleBlockPersistCompleted(block *types.Block) {
	log.Infof("persist block: %d, %x", block.Header.Height, block.Hash())

	// 节点变化经过两个共识周期，在治理合约中修改当前共识节点
	// 当前共识中，共识chain config的逻辑可以保留

	if block.Header.Height <= self.completedBlockNum {
		log.Infof("server %d, persist block %d, vs completed %d",
			self.Index, block.Header.Height, self.completedBlockNum)
		return
	}
	self.completedBlockNum = block.Header.Height
	self.incrValidator.AddBlock(block)
	if self.nonConsensusNode() {
		self.chainStore.ReloadFromLedger()
	}

	err := self.configPool.updateChainConfig()
	if err != nil {
		log.Errorf("updateChainConfig failed:%s", err)
	}
}

func (self *Server) NewConsensusPayload(payload *p2pmsg.ConsensusPayload) {
	peerID := vconfig.PubkeyID(payload.Owner)
	peerIdx, present := self.peerPool.GetPeerIndex(peerID)
	if !present {
		log.Debugf("invalid consensus node: %s", peerID)
		return
	}
	if self.peerPool.isNewPeer(peerIdx) {
		self.peerPool.peerConnected(peerIdx)
	}
	p2pid, present := self.peerPool.getP2pId(peerIdx)
	if !present || p2pid != payload.PeerId {
		self.peerPool.addP2pId(peerIdx, payload.PeerId)
	}

	if C, present := self.msgRecvC[peerIdx]; present {
		C <- &p2pMsgPayload{
			fromPeer: peerIdx,
			payload:  payload,
		}
	} else {
		log.Errorf("consensus msg without receiver: %d node: %s", peerIdx, peerID)
		return
	}
}

func (self *Server) nonConsensusNode() bool {
	return self.Index == math.MaxUint32
}

func (self *Server) initialize() error {
	selfNodeId := vconfig.PubkeyID(self.account.PublicKey)
	log.Infof("server: %s starting", selfNodeId)

	store, err := OpenBlockStore(self.ledger, self.pid)
	if err != nil {
		log.Errorf("failed to open block store: %s", err)
		return fmt.Errorf("failed to open block store: %s", err)
	}
	self.chainStore = store
	log.Info("block store opened")

	self.blockPool, err = newBlockPool(self, self.msgHistoryDuration, store)
	if err != nil {
		log.Errorf("init blockpool: %s", err)
		return fmt.Errorf("init blockpool: %s", err)
	}
	self.configPool, err = newConfigPool(self, self.blockPool)
	if err != nil {
		return fmt.Errorf("init config pool: %s", err)
	}
	self.msgPool = newMsgPool(self, self.msgHistoryDuration)
	self.rspMsgs = newResponseMsgPool(self, self.msgHistoryDuration)
	self.peerPool = NewPeerPool(0, self) // FIXME: maxSize
	self.timer = NewEventTimer(self)
	self.syncer = newSyncer(self)

	self.msgRecvC = make(map[uint32]chan *p2pMsgPayload)
	self.msgC = make(chan ConsensusMsg, CAP_MESSAGE_CHANNEL)
	self.actionC = make(chan *ServiceAction, CAP_ACTION_CHANNEL)
	self.msgSendC = make(chan *SendMsgEvent, CAP_MSG_SEND_CHANNEL)

	self.quitC = make(chan struct{})

	chainedBlkNum := store.GetChainedBlockNum()
	currentChainCfg := self.configPool.getChainConfig(chainedBlkNum)
	if currentChainCfg != nil {
		return fmt.Errorf("failed to load config (%d)", chainedBlkNum)
	}
	log.Infof("chain config loaded from local, current blockNum: %d", self.blockPool.getChainedBlockNumber())

	// update msg delays
	peerHandshakeTimeout = time.Duration(currentChainCfg.PeerHandshakeTimeout)
	newviewTimeout = time.Duration(currentChainCfg.BlockMsgDelay * 3)

	// add all consensus peers to peer_pool
	for _, p := range currentChainCfg.Peers {
		// check if peer pubkey support VRF
		if pk, err := vconfig.Pubkey(p.ID); err != nil {
			return fmt.Errorf("failed to parse peer %d PeerID: %s", p.Index, err)
		} else if !vrf.ValidatePublicKey(pk) {
			return fmt.Errorf("peer %d: invalid peer pubkey for VRF", p.Index)
		}

		if err := self.peerPool.addPeer(p); err != nil {
			return fmt.Errorf("failed to add peer %d: %s", p.Index, err)
		}
		log.Infof("added peer: %s", p.ID)
	}

	//index equal math.MaxUint32  is noconsensus node
	id := vconfig.PubkeyID(self.account.PublicKey)
	index, present := self.peerPool.GetPeerIndex(id)
	if present {
		self.Index = index
	} else {
		self.Index = math.MaxUint32
	}
	self.sub.Subscribe(message.TOPIC_SAVE_BLOCK_COMPLETE)
	go self.syncer.run()
	go self.stateMgr.run()
	go self.msgSendLoop()
	go self.timerLoop()
	go self.actionLoop()
	go func() {
		self.quitWg.Add(1)
		defer self.quitWg.Done()

		for {
			if err := self.processMsgEvent(); err != nil {
				log.Errorf("server %d: %s", self.Index, err)
			}
			if self.quit {
				break
			}
		}
	}()

	self.stateMgr.StateEventC <- &StateEvent{
		Type: ConfigLoaded,
	}

	log.Infof("peer %d started", self.Index)

	return nil
}

func (self *Server) start() error {
	// check if server pubkey support VRF
	if !vrf.ValidatePrivateKey(self.account.PrivateKey) || !vrf.ValidatePublicKey(self.account.PublicKey) {
		return fmt.Errorf("server %d consensus start failed: invalid account key for VRF", self.Index)
	}

	currentChainCfg := self.configPool.getChainConfig(self.blockPool.getChainedBlockNumber())
	if currentChainCfg != nil {
		return fmt.Errorf("failed to load config")
	}

	// start heartbeat ticker
	self.timer.startPeerTicker(math.MaxUint32)

	// start peers msg handlers
	for _, p := range currentChainCfg.Peers {
		peerIdx := p.Index
		pk := self.peerPool.GetPeerPubKey(peerIdx)
		if _, present := self.msgRecvC[peerIdx]; !present {
			self.msgRecvC[peerIdx] = make(chan *p2pMsgPayload, 1024)
		}

		go func() {
			if err := self.run(pk); err != nil {
				log.Errorf("server %d, processor on peer %d failed: %s",
					self.Index, peerIdx, err)
			}
		}()
	}

	return nil
}

func (self *Server) stop() {

	self.incrValidator.Clean()
	self.sub.Unsubscribe(message.TOPIC_SAVE_BLOCK_COMPLETE)
	// stop syncer, statemgr, msgSendLoop, timer, actionLoop, msgProcessingLoop
	self.quit = true
	close(self.quitC)
	self.quitWg.Wait()

	self.syncer.stop()
	self.timer.stop()
	self.msgPool.clean()
	self.blockPool.clean()
	self.chainStore.close()
	self.peerPool.clean()
}

//
// go routine per net connection
//
func (self *Server) run(peerPubKey keypair.PublicKey) error {
	peerID := vconfig.PubkeyID(peerPubKey)
	peerIdx, present := self.peerPool.GetPeerIndex(peerID)
	if !present {
		return fmt.Errorf("invalid consensus node: %s", peerID)
	}

	// broadcast heartbeat
	self.heartbeat()

	// wait remote msgs
	self.peerPool.waitPeerConnected(peerIdx)

	defer func() {
		// TODO: handle peer disconnection here
		log.Warnf("server %d: disconnected with peer %d", self.Index, peerIdx)
		close(self.msgRecvC[peerIdx])
		delete(self.msgRecvC, peerIdx)

		self.peerPool.peerDisconnected(peerIdx)
		self.stateMgr.StateEventC <- &StateEvent{
			Type: UpdatePeerState,
			peerState: &PeerState{
				peerIdx:   peerIdx,
				connected: false,
			},
		}
	}()

	errC := make(chan error)
	go func() {
		for {
			fromPeer, msgData, err := self.receiveFromPeer(peerIdx)
			if err != nil {
				errC <- err
				return
			}
			msg, err := DeserializeConsensusMsg(msgData)

			if err != nil {
				log.Errorf("server %d failed to deserialize vbft msg (len %d): %s", self.Index, len(msgData), err)
			} else {
				pk := self.peerPool.GetPeerPubKey(fromPeer)
				if pk == nil {
					log.Errorf("server %d failed to get peer %d pubkey", self.Index, fromPeer)
					continue
				}

				if err := msg.Verify(peerIdx, self.peerPool.GetAllPubKeys()); err != nil {
					log.Errorf("server %d failed to verify msg, type %d, err: %s",
						self.Index, msg.Type(), err)
					continue
				}

				self.onConsensusMsg(fromPeer, msg, hashData(msgData))
			}
		}
	}()

	return <-errC
}

func (self *Server) getState() ServerState {
	return self.stateMgr.getState()
}

func (self *Server) updateParticipantConfig() error {
	return self.configPool.updateChainConfig()
}

func (self *Server) startNewProposal(blkNum, view uint32) {
	// make proposal
	if self.isProposer(blkNum, view, self.Index) {
		log.Infof("server %d, proposer for block %d", self.Index, blkNum)
		// FIXME: possible deadlock on channel
		self.actionC <- &ServiceAction{
			Type:     ProposeBlock,
			BlockNum: blkNum,
			View:     view,
		}
	}

	// TODO: if new round block proposal has received, go endorsing/committing directly

	if err := self.timer.startViewTimeout(blkNum, view); err != nil {
		log.Errorf("server %d, startViewTimer (%d,%d) err:%s", self.Index, blkNum, view, err)
	}
}

// verify consensus messsage, then send msg to processMsgEvent
func (self *Server) onConsensusMsg(peerIdx uint32, msg ConsensusMsg, msgHash common.Uint256) {

	if self.msgPool.HasMsg(msg, msgHash) {
		// dup msg checking
		log.Debugf("dup msg with msg type %d from %d", msg.Type(), peerIdx)
		return
	}

	switch msg.Type() {
	case VoteMessage:
		pMsg, ok := msg.(*VoteMsg)
		if !ok {
			log.Errorf("invalid msg with vote msg type from %d", peerIdx)
			return
		}
		if err := self.blockPool.validateVoteMsg(pMsg); err != nil {
			log.Errorf("invalid vote msg from %d: %s", peerIdx, err)
			return
		}

		if pMsg.GetStartHeight() > self.blockPool.getHighestBlockNum() {
			// for concurrency, support two active consensus round
			if err := self.msgPool.AddMsg(msg, msgHash); err != nil {
				if err != errDropFarFutureMsg {
					log.Errorf("failed to add vote msg (%d) to pool: %s", pMsg.GetStartHeight(), err)
				}
				return
			}

			if isReady(self.getState()) {
				// set the peer as syncing-check trigger from current round
				// start syncing check from current round
				self.syncer.syncCheckReqC <- &SyncCheckReq{
					msg:      msg,
					blockNum: self.blockPool.getHighestBlockNum(),
				}
			}
		} else if msg.GetHeight() < self.GetCommittedBlockNo() {
			if msg.GetHeight()+MAX_SYNCING_CHECK_BLK_NUM < self.GetCommittedBlockNo() {
				log.Infof("server %d get endorse msg for block %d, from %d, current committed %d",
					self.Index, msg.GetHeight(), pMsg.PeerID, self.GetCommittedBlockNo())
				self.timer.C <- &TimerEvent{
					evtType: EventPeerHeartbeat,
					blknum:  self.GetCommittedBlockNo(),
				}
			}
			return

		} else {
			// add to block pool
			if err := self.blockPool.addVoteMsg(pMsg); err != nil {
				log.Errorf("failed to add vote msg (%d) to pool", msg.GetHeight())
				return
			}
			self.processConsensusMsg(msg)
		}

	case ChangeViewMessage:
		pMsg, ok := msg.(*ChangeViewMsg)
		if !ok {
			log.Errorf("invalid msg with view msg type from %d", peerIdx)
			return
		}
		if err := self.blockPool.validateViewMsg(pMsg); err != nil {
			log.Errorf("invalid view msg from %d: %s", peerIdx, err)
			return
		}

		if err := self.processViewMsg(pMsg); err != nil {
			log.Errorf("failed to process view msg from %d: %s", peerIdx, err)
			return
		}
		if err := self.responseViewMsg(pMsg); err != nil {
			log.Errorf("failed to response view msg from %d: %s", peerIdx, err)
		}

	case PeerHeartbeatMessage:
		pMsg, ok := msg.(*peerHeartbeatMsg)
		if !ok {
			log.Errorf("invalid msg with heartbeat msg type")
			return
		}
		self.processHeartbeatMsg(peerIdx, pMsg)
		if pMsg.CommittedBlockNumber+MAX_SYNCING_CHECK_BLK_NUM < self.GetCommittedBlockNo() {
			// delayed peer detected, response heartbeat with our chain Info
			self.timer.C <- &TimerEvent{
				evtType: EventPeerHeartbeat,
				blknum:  peerIdx,
			}
		}

	case ProposalFetchMessage:
		pMsg, ok := msg.(*proposalFetchMsg)
		if !ok {
			log.Errorf("invalid msg with proposal fetch msg type")
			return
		}
		// get from block-pool
		// get from chain
		proposalBlk := self.blockPool.getProposal(pMsg.BlockNum, pMsg.View)
		proposalJustifies := self.blockPool.getProposalJustify(pMsg.BlockNum, pMsg.View)
		if proposalBlk == nil || proposalJustifies == nil {
			log.Infof("failed to get proposal block (%d,%d)", pMsg.BlockNum, pMsg.View)
			return
		}
		pmsg := &proposalFetchRespMsg{
			BlockNum: pMsg.BlockNum,
			View:     pMsg.View,
			Proposal: &ProposalMsg{
				ProposeJustify: &Justify{
					Sigs: proposalJustifies,
				},
				Block: proposalBlk,
			},
		}
		log.Infof("server %d, handle proposal fetch %d from %d",
			self.Index, pMsg.BlockNum, peerIdx)
		self.msgSendC <- &SendMsgEvent{
			ToPeer: peerIdx,
			Msg:    pmsg,
		}

	case BlockFetchMessage:
		// handle block fetch msg
		pMsg, ok := msg.(*blockFetchMsg)
		if !ok {
			log.Errorf("invalid msg with blockfetch msg type")
			return
		}
		blk, err := self.blockPool.getSealedBlock(pMsg.BlockNum)
		if err != nil {
			log.Infof("process block fetch failed: %s", err)
			return
		}
		msg := self.constructBlockFetchRespMsg(pMsg.BlockNum, blk, blk.Hash())
		log.Infof("server %d, handle blockfetch %d from %d",
			self.Index, pMsg.BlockNum, peerIdx)
		self.msgSendC <- &SendMsgEvent{
			ToPeer: peerIdx,
			Msg:    msg,
		}

	case BlockFetchRespMessage:
		self.syncer.syncMsgC <- &SyncMsg{
			fromPeer: peerIdx,
			msg:      msg,
		}

	case BlockInfoFetchMessage:
		// handle block Info fetch msg
		pMsg, ok := msg.(*BlockInfoFetchMsg)
		if !ok {
			log.Errorf("invalid msg with blockinfo fetch msg type")
			return
		}
		maxCnt := 64
		blkInfos := make([]*BlockInfo_, 0)
		targetBlkNum := self.GetCommittedBlockNo()
		for blkNum := pMsg.StartBlockNum; blkNum <= targetBlkNum; blkNum++ {
			blk, _ := self.blockPool.getSealedBlock(blkNum)
			if blk == nil {
				break
			}
			view, _, err := getBlockView(blk)
			if err != nil {
				log.Errorf("failed to blockview %d: %s", blkNum, err)
				break
			}
			blkInfos = append(blkInfos, &BlockInfo_{
				BlockNum:   blkNum,
				Round:      view,
				Signatures: blk.Header.SigData,
			})
			if len(blkInfos) >= maxCnt {
				break
			}
		}
		msg := self.constructBlockInfoFetchRespMsg(blkInfos)
		log.Infof("server %d, response blockinfo fetch to %d, blk %d, len %d",
			self.Index, peerIdx, pMsg.StartBlockNum, len(blkInfos))
		self.msgSendC <- &SendMsgEvent{
			ToPeer: peerIdx,
			Msg:    msg,
		}

	case BlockInfoFetchRespMessage:
		self.syncer.syncMsgC <- &SyncMsg{
			fromPeer: peerIdx,
			msg:      msg,
		}
	case BlockSubmitMessage:
		pMsg, ok := msg.(*blockSubmitMsg)
		if !ok {
			log.Error("invalid msg with submit msg type")
			return
		}
		msgBlkNum := pMsg.GetBlockNum()
		if self.blockPool.getSealedBlockNum() > msgBlkNum+1 {
			return
		}
		if err := self.msgPool.AddMsg(msg, msgHash); err != nil {
			if err != errDropFarFutureMsg {
				log.Errorf("failed to add submit msg (%d) to pool: %s", msgBlkNum, err)
			}
			return
		}
		if self.CheckSubmitBlock(msgBlkNum, pMsg.BlockStateRoot) {
			self.actionC <- &ServiceAction{
				Type:     SubmitBlock,
				BlockNum: msgBlkNum,
			}
		}
	}
}

func (self *Server) processRoundVoteMsg(msg *RoundVoteMsg) error {

	// verify view
	activeV := self.blockPool.getActiveView(msg.Height)
	if activeV != msg.View {
		log.Infof("skipped processing (%d,%d), active view: %d", msg.Height, msg.View, activeV)
		return nil
	}

	if msg.NewView != nil {
		if err := self.processNewView(msg); err != nil {
			return err
		}
	}
	if msg.Result != nil {
		if err := self.processExecMerkle(msg); err != nil {
			return err
		}
	}
	if msg.Commit != nil {
		if err := self.processCommit(msg); err != nil {
			return err
		}
	}
	if msg.Prepare != nil {
		if err := self.processPrepare(msg); err != nil {
			return err
		}
	}

	if msg.Proposal != nil {
		if err := self.processPropose(msg); err != nil {
			return err
		}
	}

	return nil
}

func (self *Server) processNewView(msg *RoundVoteMsg) error {
	activeView := self.blockPool.getActiveView(msg.Height)
	if activeView >= msg.View {
		log.Infof("current active view %d, skipped new view %d", activeView, msg.View)
		return nil
	}

	self.blockPool.processNewView(msg.Height, msg.View)
	// check active view again
	if self.blockPool.getActiveView(msg.Height) > msg.View {
		// if new-view reached QC
		v := msg.View + 1
		if self.blockPool.isLeaderOf(msg.Height, v, self.Index) {
			if self.blockPool.isProposalReady(msg.Height, v) {
				return nil
			}
			self.makeProposal(msg.Height, v)
		}
	}

	return nil
}

func (self *Server) processExecMerkle(msg *RoundVoteMsg) error {
	if self.blockPool.getFinalizedBlockNum() >= msg.Height {
		return nil
	}
	if err := self.blockPool.processNewExecMerkle(msg.Height, msg.View); err != nil {
		return fmt.Errorf("new exec merkle (%d,%d) failed: %s", msg.Height, msg.View, err)
	}
	return nil
}

func (self *Server) processCommit(msg *RoundVoteMsg) error {
	// TODO: check if vote is in same fork with server
	if self.blockPool.getSealedBlockNum() >= msg.Height {
		return nil
	}
	if err := self.blockPool.processCommit(msg.Height, msg.View); err != nil {
		return fmt.Errorf("new commit (%d,%d) failed: %s", msg.Height, msg.View, err)
	}
	return nil
}

func (self *Server) processPrepare(msg *RoundVoteMsg) error {
	// TODO: check if vote is in same fork with server
	if err := self.blockPool.processPrepare(msg.Height, msg.View); err != nil {
		return fmt.Errorf("new prepare (%d,%d) failed: %s", msg.Height, msg.View, err)
	}
	return nil
}

func (self *Server) processPropose(msg *RoundVoteMsg) error {
	// TODO: check if vote is in same fork with server
	if err := self.blockPool.processProposal(msg.Height, msg.View); err != nil {
		return fmt.Errorf("new proposal (%d,%d) failed: %s", msg.Height, msg.View, err)
	}
	return nil
}

func (self *Server) responseVoteMsg(blknum uint32) error {
	// get start height
	h := self.rspMsgs.getFirstResponseHeight()

	// loop constructing round-vote msgs, make sure continuous round-vote
	roundVotes := make([]*RoundVoteMsg, 0)
	for {
		// FIXME: make sure all round-rsp in same fork
		rsp := self.rspMsgs.getRoundResponse(h)
		if rsp == nil {
			break
		}
		vote, err := self.constructRoundVoteMsg(h, rsp)
		if err != nil {
			log.Errorf("failed to construct roundvote msg (%d): %s", h, err)
			break
		}
		if vote == nil {
			break
		}
		roundVotes = append(roundVotes, vote)
		h += 1
	}
	if len(roundVotes) == 0 {
		return nil
	}

	votemsg, err := self.constructVoteMsg(roundVotes)
	if err != nil {
		return fmt.Errorf("failed to constrcut vote msg: %s", err)
	}
	self.broadcast(votemsg)
	return nil
}

func (self *Server) processViewMsg(msg *ChangeViewMsg) error {
	// TODO
	return nil
}

func (self *Server) startNewRound() error {
	targetBlknum := self.blockPool.getHighestBlockNum()
	for n := self.blockPool.getFinalizedBlockNum() + 1; n <= targetBlknum+1; n++ {
		v := self.blockPool.getView(n)
		if v == math.MaxUint32 {
			break
		}
		if self.blockPool.isLeaderOf(n, v, self.Index) {
			self.makeProposal(n, v)
		} else if self.blockPool.isProactorOf(n, v, self.Index) {
			self.makePrepare(n, v)
		} else if self.blockPool.isProPrepared(n, v) {
			self.makePrepare(n, v)
		}
	}
	return nil
}

func (self *Server) responseViewMsg(msg *ChangeViewMsg) error {
	// TODO
	return nil
}

func (self *Server) processConsensusMsg(msg ConsensusMsg) {
	if isReady(self.getState()) {
		self.msgC <- msg
	}
}

func (self *Server) processMsgEvent() error {
	select {
	case msg := <-self.msgC:

		log.Debugf("server %d process msg, block %d, type %d, current blk %d",
			self.Index, msg.GetHeight(), msg.Type(), self.blockPool.getSealedBlockNum())

		switch msg.Type() {
		case VoteMessage:
			pMsg := msg.(*VoteMsg)
			for _, r := range pMsg.Rounds {
				if err := self.processRoundVoteMsg(r); err != nil {
					log.Errorf("failed to process round vote msg from %d: %d:%s", pMsg.PeerID, r.Height, err)
					break
				}
			}
			if self.blockPool.getSafePreparedBlockNum() >= pMsg.GetHeight() {
				if err := self.startNewRound(); err != nil {
					log.Errorf("failed to start new round after processed commit %d: %s", pMsg.GetHeight(), err)
				}
			}
			// make response
			self.actionC <- &ServiceAction{
				Type:     MakeResponse,
				BlockNum: pMsg.GetHeight(),
			}
		case ChangeViewMessage:

		}

	case <-self.quitC:
		return fmt.Errorf("server %d, processMsgEvent loop quit", self.Index)
	}
	return nil
}

func (self *Server) actionLoop() {
	self.quitWg.Add(1)
	defer self.quitWg.Done()

	for {
		select {
		case action := <-self.actionC:
			switch action.Type {
			case ProposeBlock:
				if action.View < self.blockPool.getActiveView(action.BlockNum) {
					continue
				}
				if err := self.proposeBlock(action.BlockNum, action.View); err != nil {
					log.Errorf("server %d failed to making proposal (%d,%d): %s",
						self.Index, action.BlockNum, action.View, err)
				}

			case PrepareBlock:
				// prepare for block
				if err := self.prepareBlock(action.BlockNum, action.View); err != nil {
					log.Errorf("server %d failed to prepare for block (%d,%d): %s",
						self.Index, action.BlockNum, action.View, err)
				}

			case CommitBlock:
				if err := self.commitBlock(action.BlockNum, action.View); err != nil {
					log.Errorf("server %d failed to commit block (%d,%d): %s",
						self.Index, action.BlockNum, action.View, err)
				}

			case SealBlock:
				if err := self.sealProposal(action.BlockNum, action.View); err != nil {
					log.Errorf("server %d failed to seal block (%d,%d): %s",
						self.Index, action.BlockNum, action.View, err)
				}
			case FastForward:
				// 1. from current block num, check commit msgs in msg pool
				// 2. if commit consensused, seal the proposal
				for {
					blkNum := self.GetCurrentBlockNo()
					cfg := self.configPool.getChainConfig(blkNum)
					C := int(self.config.C)
					N := int(self.config.N)

					if err := self.updateParticipantConfig(); err != nil {
						log.Errorf("server %d update config failed in forwarding: %s", self.Index, err)
					}

					// get pending msgs from msgpool
					pMsgs := self.msgPool.GetProposalMsgs(blkNum)
					for _, msg := range pMsgs {
						p := msg.(*blockProposalMsg)
						if p != nil {
							if err := self.blockPool.newBlockProposal(p); err != nil {
								log.Errorf("server %d failed add proposal in fastforwarding: %s",
									self.Index, err)
							}
						}
					}

					cMsgs := self.msgPool.GetCommitMsgs(blkNum)
					commitMsgs := make([]*blockCommitMsg, 0)
					for _, msg := range cMsgs {
						c := msg.(*blockCommitMsg)
						if c != nil {
							if err := self.blockPool.newBlockCommitment(c); err == nil {
								commitMsgs = append(commitMsgs, c)
							} else {
								log.Errorf("server %d failed to add commit in fastforwarding: %s",
									self.Index, err)
							}
						}
					}

					log.Infof("server %d fastforwarding from %d, (%d, %d)",
						self.Index, self.GetCurrentBlockNo(), len(cMsgs), len(pMsgs))
					if len(pMsgs) == 0 && len(cMsgs) == 0 {
						log.Infof("server %d fastforward done, no msg", self.Index)
						self.startNewRound()
						break
					}

					// check if consensused
					proposer, forEmpty := getCommitConsensus(commitMsgs, C, N)
					if proposer == math.MaxUint32 {
						if err := self.catchConsensus(blkNum); err != nil {
							log.Infof("server %d fastforward done, catch consensus: %s", self.Index, err)
						}
						log.Infof("server %d fastforward done at blk %d, no consensus", self.Index, blkNum)
						break
					}

					// get consensused proposal
					var proposal *blockProposalMsg
					for _, m := range pMsgs {
						p, ok := m.(*blockProposalMsg)
						if !ok {
							continue
						}
						if p.Block.getProposer() == proposer {
							proposal = p
							break
						}
					}
					if proposal == nil {
						log.Infof("server %d fastforward stopped at blk %d, no proposal", self.Index, blkNum)
						self.fetchProposal(blkNum, proposer)
						if err := self.timer.StartCommitTimer(blkNum); err != nil {
							log.Errorf("server %d fastforward startcommittimer at blk %d, err:%s", self.Index, blkNum, err)
						}
						break
					}

					log.Infof("server %d fastforwarding block %d, proposer %d",
						self.Index, blkNum, proposal.Block.getProposer())

					// fastforward the block
					if err := self.sealBlock(proposal.Block, forEmpty, true); err != nil {
						log.Errorf("server %d fastforward stopped at blk %d, seal failed: %s",
							self.Index, blkNum, err)
						break
					}
				}

			case ReBroadcast:
				blkNum := self.GetCurrentBlockNo()
				if blkNum > action.BlockNum {
					continue
				}

				proposals := make([]*blockProposalMsg, 0)
				for _, msg := range self.msgPool.GetProposalMsgs(blkNum) {
					p := msg.(*blockProposalMsg)
					if p != nil {
						proposals = append(proposals, p)
					}
				}

				for _, p := range proposals {
					if p.Block.getProposer() == self.Index {
						log.Infof("server %d rebroadcast proposal, blk %d",
							self.Index, p.Block.getBlockNum())
						self.broadcast(p)
					}
				}
				if self.isEndorser(blkNum, self.Index) {
					rebroadcasted := false
					endorseFailed := self.blockPool.endorseFailed(blkNum, self.config.C)
					eMsgs := self.msgPool.GetEndorsementsMsgs(blkNum)
					for _, msg := range eMsgs {
						e := msg.(*blockEndorseMsg)
						if e != nil && e.Endorser == self.Index && e.EndorseForEmpty == endorseFailed {
							log.Infof("server %d rebroadcast endorse, blk %d for %d, %t",
								self.Index, e.GetBlockNum(), e.EndorsedProposer, e.EndorseForEmpty)
							self.broadcast(e)
							rebroadcasted = true
						}
					}
					if !rebroadcasted {
						proposal := self.getHighestRankProposal(blkNum, proposals)
						if proposal != nil {
							if err := self.prepareBlock(proposal, false); err != nil {
								log.Errorf("server %d rebroadcasting failed to endorse (%d): %s",
									self.Index, blkNum, err)
							}
						} else {
							log.Errorf("server %d rebroadcasting failed to endorse(%d), no proposal found(%d)",
								self.Index, blkNum, len(proposals))
						}
					}
				} else if proposal, forEmpty := self.blockPool.getEndorsedProposal(blkNum); proposal != nil {
					// construct endorse msg
					if endorseMsg, _ := self.constructPrepareMsg(proposal, forEmpty); endorseMsg != nil {
						self.broadcast(endorseMsg)
					}
				}
				if self.isCommitter(blkNum, self.Index) {
					committed := false
					cMsgs := self.msgPool.GetCommitMsgs(self.GetCurrentBlockNo())
					for _, msg := range cMsgs {
						c := msg.(*blockCommitMsg)
						if c != nil && c.Committer == self.Index {
							log.Infof("server %d rebroadcast commit, blk %d for %d, %t",
								self.Index, c.GetBlockNum(), c.BlockProposer, c.CommitForEmpty)
							self.broadcast(msg)
							committed = true
						}
					}
					if !committed {
						if proposer, forEmpty, done := self.blockPool.endorseDone(blkNum, self.config.C); done {
							proposal := self.findBlockProposal(blkNum, proposer, forEmpty)

							// consensus ok, make endorsement
							if proposal == nil {
								self.fetchProposal(blkNum, proposer)
								// restart endorsing timer
								if err := self.timer.StartEndorsingTimer(blkNum); err != nil {
									log.Errorf("server %d endorse %d done, startendorsingtimer err:%s", self.Index, blkNum, err)
								}
								log.Errorf("server %d endorse %d done, but no proposal", self.Index, blkNum)
							} else if err := self.makeCommitment(proposal, blkNum, forEmpty); err != nil {
								log.Errorf("server %d failed to commit block %d on rebroadcasting: %s",
									self.Index, blkNum, err)
							}
						} else if self.blockPool.endorseFailed(blkNum, self.config.C) {
							// endorse failed, start empty endorsing
							self.timer.C <- &TimerEvent{
								evtType:  EventEndorseBlockTimeout,
								blockNum: blkNum,
							}
						}
					}
				}
			case SubmitBlock:
				if action.BlockNum > self.blockPool.getSealedBlockNum() {
					continue
				}
				stateRoot, err := self.blockPool.getExecMerkleRoot(action.BlockNum)
				if err != nil {
					log.Infof("handleBlockSubmit failed:%s", err)
					continue
				}
				if self.CheckSubmitBlock(action.BlockNum, stateRoot) {
					if err := self.blockPool.submitBlock(action.BlockNum); err != nil {
						log.Errorf("SubmitBlock err:%s", err)
					}
				}
			case MakeResponse:
				if err := self.responseVoteMsg(action.BlockNum); err != nil {
					log.Errorf("failed to response vote msg (%d): %s", action.BlockNum, err)
				}
			}
		case <-self.quitC:
			log.Infof("server %d actionLoop quit", self.Index)
			return
		}
	}
}

func (self *Server) timerLoop() {
	self.quitWg.Add(1)
	defer self.quitWg.Done()

	for {
		select {
		case evt := <-self.timer.C:
			if err := self.processTimerEvent(evt); err != nil {
				log.Errorf("failed to process timer evt: %d, err: %s", evt.evtType, err)
			}

		case <-self.quitC:
			log.Infof("server %d timerLoop quit", self.Index)
			return
		}
	}
}

func (self *Server) processTimerEvent(evt *TimerEvent) error {
	switch evt.evtType {
	case EventPeerHeartbeat:
		self.heartbeat()

	case EventViewTimeout:
		// check if any progress on view
		if self.blockPool.getActiveView(evt.blknum) > evt.View {
			return nil
		}
		// send viewchange message if no progress
		changeMsg, err := self.constructViewChangeMsg(evt.blknum, evt.View)
		if err != nil {
			return err
		}
		self.broadcast(changeMsg)

	case EventTxPool:
		self.timer.stopTxTicker(evt.blknum)
		if self.completedBlockNum+1 == evt.blknum {
			validHeight := self.validHeight(evt.blknum)
			newProposal := false
			for _, e := range self.poolActor.GetTxnPool(true, validHeight) {
				if err := self.incrValidator.Verify(e.Tx, validHeight); err == nil {
					newProposal = true
					break
				}
			}
			if newProposal {
				self.startNewProposal(evt.blknum, 0)
			} else {
				//reset timer, continue waiting txs from txnpool
				self.timer.startTxTicker(evt.blknum)
			}
		} else {
			self.timer.startTxTicker(evt.blknum)
		}
	}
	return nil
}

func (self *Server) processHandshakeMsg(peerIdx uint32, msg *peerHandshakeMsg) error {
	self.peerPool.peerHandshake(peerIdx, msg)
	self.stateMgr.StateEventC <- &StateEvent{
		Type: UpdatePeerConfig,
		peerState: &PeerState{
			peerIdx:            peerIdx,
			connected:          true,
			chainConfigEpoch:   msg.ChainConfig.View,
			committedBlockNum:  msg.CommittedBlockNumber,
			committedBlockHash: msg.CommittedBlockHash,
		},
	}

	return nil
}

func (self *Server) processHeartbeatMsg(peerIdx uint32, msg *peerHeartbeatMsg) {
	self.peerPool.peerHeartbeat(peerIdx, msg)
	log.Debugf("server %d received heartbeat from peer %d, chainview %d, blkNum %d",
		self.Index, peerIdx, msg.Epoch, msg.CommittedBlockNumber)
	self.stateMgr.StateEventC <- &StateEvent{
		Type: UpdatePeerState,
		peerState: &PeerState{
			peerIdx:            peerIdx,
			connected:          true,
			chainConfigEpoch:   msg.Epoch,
			committedBlockNum:  msg.CommittedBlockNumber,
			committedBlockHash: msg.CommittedBlockHash,
		},
	}
}

func (self *Server) proposeBlock(blknum uint32, view uint32) error {
	if self.nonConsensusNode() {
		return fmt.Errorf("server %d quit consensus node", self.Index)
	}

	forEmpty := false
	validHeight := self.validHeight(blknum)
	sysTxs := make([]*types.Transaction, 0)
	userTxs := make([]*types.Transaction, 0)

	//check need upate chainconfig
	cfg := &vconfig.ChainConfig{}
	cfg = nil
	if self.checkNeedUpdateChainConfig(blknum) || self.checkUpdateChainConfig(blknum) {
		chainconfig := self.configPool.getChainConfig(blknum)
		if chainconfig == nil {
			return fmt.Errorf("failed to getChainConfig when proposing block (%d,%d)", blknum, view)
		}
		//add transaction invoke governance native commit_pos contract
		if self.checkNeedUpdateChainConfig(blknum) {
			tx, err := self.creategovernaceTransaction(blknum)
			if err != nil {
				return fmt.Errorf("construct governace transaction error: %v", err)
			}
			sysTxs = append(sysTxs, tx)
			chainconfig.View++
		}
		forEmpty = true
		cfg = chainconfig
	}
	if !forEmpty {
		for _, e := range self.poolActor.GetTxnPool(true, validHeight) {
			if err := self.incrValidator.Verify(e.Tx, validHeight); err == nil {
				userTxs = append(userTxs, e.Tx)
			}
		}
	}
	proposalMsg, err := self.constructProposalMsg(blknum, view, sysTxs, userTxs, cfg)
	if err != nil {
		return fmt.Errorf("failed to construct proposal: %s", err)
	}

	self.rspMsgs.addMsg(blknum, view, proposalMsg)
	log.Infof("server %d make proposal for block %d", self.Index, blknum)
	return nil
}

func (self *Server) prepareBlock(blknum, view uint32) error {
	// check if has endorsed
	if self.blockPool.checkCommittedOnForks(blknum, view) {
		log.Errorf("server %d, skip preparing for block (%d,%d), locked on forks", self.Index, blknum, view)
		return nil
	}

	// build prepare msg
	prepareMsg, err := self.constructPrepareMsg(blknum, view)
	if err != nil {
		return fmt.Errorf("failed to construct prepare msg(%d,%d): %s", blknum, view, err)
	}

	// set the block as self-endorsed-block
	self.rspMsgs.addMsg(blknum, view, prepareMsg)
	return nil
}

func (self *Server) commitBlock(blknum, view uint32) error {
	if self.blockPool.checkCommittedOnForks(blknum, view) {
		log.Errorf("server %d, skip committing for block (%d,%d), locked on forks", self.Index, blknum, view)
		return nil
	}

	// build commit msg
	commitMsg, err := self.constructCommitMsg(blknum, view)
	if err != nil {
		return fmt.Errorf("failed to construct commit msg(%d,%d): %s", blknum, view, err)
	}

	self.rspMsgs.addMsg(blknum, view, commitMsg)
	return nil
}

func (self *Server) sealBlock(blknum, view uint32) error {
	if blknum <= self.blockPool.getChainedBlockNumber() {
		return nil
	} else if blknum >= self.blockPool.getHighestBlockNum() {
		// we have lost sync, restarting syncing
		self.restartSyncing()
		return fmt.Errorf("future seal of %d, current blknum: %d", blknum, self.blockPool.getChainedBlockNumber())
	}

	if err := self.blockPool.setSealed(blknum, view); err != nil {
		return err
	}

	// block sealed, get merkle-root
	stateRoot, err := self.blockPool.getExecMerkleRoot(blknum)
	if err != nil {
		return err
	}
	if merkleMsg, err := self.constructMerkleResultMsg(blknum, view, stateRoot); err != nil {
		log.Errorf("failed to construct merkle-result msg (%d,%d): %s", blknum, view, err)
	} else {
		self.rspMsgs.addMsg(blknum, view, merkleMsg)
	}

	// notify other modules that block sealed
	self.timer.onBlockSealed(blknum)
	self.msgPool.onBlockSealed(blknum)
	self.blockPool.onBlockSealed(blknum)

	log.Infof("server %d, sealed block (%d, %d)", self.Index, blknum, view)
	return nil
}

//
// Note: sealProposal updates self.currentBlockNum, make sure not concurrency
// (only called by sealProposal action)
//
func (self *Server) sealProposal(blknum, view uint32) error {
	blk := self.blockPool.getProposal(blknum, view)
	if blk == nil {
		// TODO: fetch proposal
		return fmt.Errorf("server %d failed to get proposal(%d,%d) from blockpool", self.Index, blknum, view)
	}

	// for each round, we can only seal one block
	if err := self.sealBlock(blknum, view); err != nil {
		return err
	}

	if self.hasBlockConsensused(blknum+1) {
		return self.makeFastForward()
	} else {
		return self.startNewRound()
	}

	return nil
}

func (self *Server) fastForwardBlock(block *types.Block) error {

	// TODO: update chainconfig when forwarding

	if isActive(self.getState()) {
		return fmt.Errorf("server %d: invalid fastforward, current state: %d", self.Index, self.getState())
	}
	if self.blockPool.getSealedBlockNum() >= block.Header.Height {
		return nil
	}
	if self.blockPool.getSealedBlockNum()+1 == block.Header.Height {
		// FIXME:
		return self.sealBlock(block.Header.Height, UNKNOWN_VIEW)
	}
	return fmt.Errorf("server %d: fastforward blk %d failed, current blkNum: %d",
		self.Index, block.Header.Height, self.blockPool.getSealedBlockNum())
}

func (self *Server) msgSendLoop() {
	self.quitWg.Add(1)
	defer self.quitWg.Done()

	for {
		select {
		case evt := <-self.msgSendC:
			if self.nonConsensusNode() {
				continue
			}
			payload, err := SerializeConsensusMsg(evt.Msg)
			if err != nil {
				log.Errorf("server %d failed to serialized msg (type: %d): %s", self.Index, evt.Msg.Type(), err)
				continue
			}
			if evt.ToPeer == math.MaxUint32 {
				// broadcast
				if err := self.broadcastToAll(payload); err != nil {
					log.Errorf("server %d xmit msg (type %d): %s",
						self.Index, evt.Msg.Type(), err)
				}
			} else {
				if err := self.sendToPeer(evt.ToPeer, payload); err != nil {
					log.Errorf("server %d xmit to peer %d failed: %s", self.Index, evt.ToPeer, err)
				}
			}

		case <-self.quitC:
			log.Infof("server %d msgSendLoop quit", self.Index)
			return
		}
	}
}

//creategovernaceTransaction invoke governance native contract commit_pos
func (self *Server) creategovernaceTransaction(blkNum uint32) (*types.Transaction, error) {
	mutable := utils.BuildNativeTransaction(nutils.GovernanceContractAddress, gover.COMMIT_DPOS, []byte{})
	mutable.Nonce = blkNum
	tx, err := mutable.IntoImmutable()
	return tx, err
}

//checkNeedUpdateChainConfig use blockcount
func (self *Server) checkNeedUpdateChainConfig(blkNum uint32) bool {
	prevBlk, _ := self.blockPool.getSealedBlock(blkNum - 1)
	if prevBlk == nil {
		log.Errorf("failed to get prevBlock (%d)", blkNum-1)
		return false
	}
	lastConfigBlkNum, err := getBlockLastConfigHeight(prevBlk)
	if err != nil {
		log.Errorf("failed to get lastConfigHeight of block (%d)", blkNum-1)
		return false
	}
	chaincfg := self.configPool.getChainConfig(blkNum)
	if chaincfg != nil {
		log.Errorf("failed to chainconfig of block %d", blkNum)
		return false
	}
	return (blkNum - lastConfigBlkNum) >= chaincfg.MaxBlockChangeView
}

//checkUpdateChainConfig query leveldb check is force update
func (self *Server) checkUpdateChainConfig(blkNum uint32) bool {
	chaincfg := self.configPool.getChainConfig(blkNum)
	if chaincfg != nil {
		return false
	}
	force, err := isUpdate(self.blockPool.getExecWriteSet(blkNum-1), chaincfg.View)
	if err != nil {
		log.Errorf("checkUpdateChainConfig err:%s", err)
		return false
	}
	log.Debugf("checkUpdateChainConfig force: %v", force)
	return force
}

func (self *Server) validHeight(blkNum uint32) uint32 {
	height := blkNum - 1
	validHeight := height
	start, end := self.incrValidator.BlockRange()
	if height+1 == end {
		validHeight = start
	} else {
		self.incrValidator.Clean()
	}
	return validHeight
}

func (self *Server) nonSystxs(sysTxs []*types.Transaction, blkNum uint32) bool {
	if self.checkNeedUpdateChainConfig(blkNum) && len(sysTxs) == 1 {
		invoke := sysTxs[0].Payload.(*payload.InvokeCode)
		if invoke == nil {
			log.Errorf("nonSystxs invoke is nil,blocknum:%d", blkNum)
			return true
		}
		if bytes.Compare(invoke.Code, ninit.COMMIT_DPOS_BYTES) == 0 {
			return false
		}
	}
	return true
}

func (self *Server) makeProposal(blknum, view uint32) {
	self.actionC <- &ServiceAction{
		Type:     ProposeBlock,
		BlockNum: blknum,
		View:     view,
	}
}

// invoked from block-processor, non-blocking
func (self *Server) makePrepare(blknum, view uint32) {
	self.actionC <- &ServiceAction{
		Type:     PrepareBlock,
		BlockNum: blknum,
		View:     view,
	}
}

// invoked from block-processor, non-blocking
func (self *Server) makeCommitment(blknum, view uint32) {
	self.actionC <- &ServiceAction{
		Type:     CommitBlock,
		BlockNum: blknum,
		View:     view,
	}
}

func (self *Server) makeSealed(blknum, view uint32, block *types.Block) error {
	if err := self.verifyPrevBlockHash(blknum, block.Header.PrevBlockHash); err != nil {
		// TODO: in-consistency with prev-blockhash, resync-required
		self.restartSyncing()
		return fmt.Errorf("verify prev block hash failed: %s", err)
	}

	log.Infof("server %d ready to seal block (%d,%d)", self.Index, blknum, view)

	// seal the block
	self.actionC <- &ServiceAction{
		Type:     SealBlock,
		BlockNum: blknum,
		View:     view,
	}
	return nil
}

func (self *Server) makeFinalized(blknum uint32, block *types.Block, merkleRoot common.Uint256) error {
	// TODO
	return nil
}

func (self *Server) makeFastForward() error {
	if len(self.actionC)+3 >= cap(self.actionC) {
		// FIXME:
		// some cases, such as burst of heartbeat msg, may do too much fast forward.
		// make bftActionC full, and bftActionLoop halted.
		// TODO: add throttling in state-mgmt
		return fmt.Errorf("server %d make fastforward skipped, %d vs %d",
			self.Index, len(self.actionC), cap(self.actionC))
	}

	self.actionC <- &ServiceAction{
		Type:     FastForward,
		BlockNum: self.GetCurrentBlockNo(),
	}
	return nil
}

func (self *Server) reBroadcastCurrentRoundMsgs() error {
	if len(self.actionC)+3 >= cap(self.actionC) {
		// FIXME:
		// some cases, such as burst of heartbeat msg, may do too much rebroadcast.
		// make bftActionC full, and bftActionLoop halted.
		// TODO: add throttling in state-mgmt
		return fmt.Errorf("server %d make rebroadcasting skipped, %d vs %d",
			self.Index, len(self.actionC), cap(self.actionC))
	}

	self.actionC <- &ServiceAction{
		Type:     ReBroadcast,
		BlockNum: self.GetCurrentBlockNo(),
	}
	return nil
}

func (self *Server) fetchProposal(blkNum uint32, proposer uint32) error {
	msg := self.constructProposalFetchMsg(blkNum, proposer)
	self.msgSendC <- &SendMsgEvent{
		ToPeer: math.MaxUint32,
		Msg:    msg,
	}
	return nil
}

// TODO: refactor this
func (self *Server) catchConsensus(blkNum uint32) error {
	if !self.isEndorser(blkNum, self.Index) && !self.isCommitter(blkNum, self.Index) {
		return nil
	}

	proposals := make(map[uint32]*blockProposalMsg)
	pMsgs := self.msgPool.GetProposalMsgs(blkNum)
	for _, msg := range pMsgs {
		p, ok := msg.(*blockProposalMsg)
		if !ok {
			continue
		}
		proposals[p.Block.getProposer()] = p
	}

	C := int(self.config.C)
	eMsgs := self.msgPool.GetEndorsementsMsgs(blkNum)
	var proposal *blockProposalMsg
	endorseDone := false
	endorseEmpty := false
	if len(eMsgs) > C {
		var maxProposer uint32
		emptyCnt := 0
		maxCnt := 0
		proposers := make(map[uint32]int)
		for _, msg := range eMsgs {
			c, ok := msg.(*blockEndorseMsg)
			if !ok {
				continue
			}
			if c.EndorseForEmpty {
				emptyCnt++
			}
			proposers[c.EndorsedProposer] += 1
			if proposers[c.EndorsedProposer] > maxCnt {
				maxProposer = c.EndorsedProposer
				maxCnt = proposers[c.EndorsedProposer]
			}
		}
		proposal = proposals[maxProposer]
		if maxCnt > C {
			endorseDone = true
		}
		if emptyCnt > C {
			endorseDone = true
			endorseEmpty = true
		}
	}
	if proposal != nil && self.isProposer(blkNum, proposal.Block.getProposer()) {
		self.processProposalMsg(proposal)
	}

	if self.isEndorser(blkNum, self.Index) && !endorseDone && proposal != nil {
		return self.prepareBlock(proposal, endorseEmpty)
	}

	if !endorseDone {
		return fmt.Errorf("server %d catch consensus with endorse failed", self.Index)
	}

	if !self.isCommitter(blkNum, self.Index) {
		return nil
	}

	var maxProposer uint32
	maxCnt := 0
	emptyCnt := 0
	proposers := make(map[uint32]int)
	cMsgs := self.msgPool.GetCommitMsgs(blkNum)
	for _, msg := range cMsgs {
		c, ok := msg.(*blockCommitMsg)
		if !ok {
			continue
		}
		if c.CommitForEmpty {
			emptyCnt++
		}
		proposers[c.BlockProposer] += 1
		if proposers[c.BlockProposer] > maxCnt {
			maxProposer = c.BlockProposer
		}
	}

	if p := proposals[maxProposer]; p != nil {
		return self.commitBlock(p, emptyCnt > 0)
	}

	return nil
}

func (self *Server) verifyPrevBlockHash(blkNum uint32, prevBlkHash common.Uint256) error {
	prevBlk, err := self.blockPool.getSealedBlock(blkNum - 1)
	if err != nil {
		// TODO: has no candidate proposals for prevBlock, should restart syncing
		return fmt.Errorf("failed to get prevBlock of current round (%d): %s", blkNum, err)
	}
	if prevBlk.Header.PrevBlockHash != prevBlkHash {
		// continue waiting for more proposals
		// FIXME
		return fmt.Errorf("inconsistent prev-block hash %s vs %s (blk %d)",
			prevBlkHash.ToHexString(), prevBlk.Header.PrevBlockHash.ToHexString(), blkNum)
	}

	return nil
}

func (self *Server) hasBlockConsensused(blknum uint32) bool {
	C := int(self.config.C)
	cMsgs := self.msgPool.GetCommitMsgs(blknum)
	proposers := make(map[uint32]int)
	for _, msg := range cMsgs {
		c, ok := msg.(*blockCommitMsg)
		if !ok {
			continue
		}
		proposers[c.BlockProposer] += 1
		if proposers[c.BlockProposer] > C {
			return true
		}
	}

	return false
}

func (self *Server) restartSyncing() {

	// send sync request to self.sync, go syncing-state immediately
	// stop all bft timers

	self.stateMgr.checkStartSyncing(self.GetCommittedBlockNo(), true)

}

func (self *Server) checkSyncing() {
	self.stateMgr.checkStartSyncing(self.GetCommittedBlockNo(), false)
}
