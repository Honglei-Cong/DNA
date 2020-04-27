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
	"fmt"
	"math"

	"github.com/DNAProject/DNA/common"
	"github.com/DNAProject/DNA/common/log"
	"github.com/DNAProject/DNA/consensus/vbft/config"
	"github.com/DNAProject/DNA/core/signature"
	"github.com/DNAProject/DNA/p2pserver/message/msg_pack"
	p2pmsg "github.com/DNAProject/DNA/p2pserver/message/types"
	"github.com/ontio/ontology-crypto/keypair"
	"github.com/DNAProject/DNA/core/types"
)

func (self *Server) GetCommittedBlockNo() uint32 {
	return self.chainStore.GetChainedBlockNum()
}

func (self *Server) isPeerAlive(peerIdx uint32, blockNum uint32) bool {

	// TODO
	if peerIdx == self.Index {
		return true
	}

	return self.peerPool.isPeerAlive(peerIdx)
}

func (self *Server) isPeerActive(peerIdx uint32, blockNum uint32) bool {
	if self.isPeerAlive(peerIdx, blockNum) {
		p := self.peerPool.getPeer(peerIdx)
		if p == nil {
			return false
		}

		if p.LatestInfo != nil {
			return p.LatestInfo.CommittedBlockNumber+MAX_SYNCING_CHECK_BLK_NUM*4 > self.GetCommittedBlockNo()
		}
		return true
	}

	return false
}

func (self *Server) isProposer(blockNum, view uint32, peerIdx uint32) bool {
	return self.blockPool.isLeaderOf(blockNum, view, peerIdx)
}

//
//  call this method with metaLock locked
//
func (self *Server) buildParticipantConfig(blkNum uint32, lastBlk *types.Block, chainCfg *vconfig.ChainConfig) (*BlockParticipantConfig, error) {

	if blkNum == 0 {
		return nil, fmt.Errorf("not participant config for genesis block")
	}

	seed := getParticipantSelectionSeed(lastBlk)
	if seed.IsNil() {
		return nil, fmt.Errorf("failed to calculate participant SelectionSeed")
	}

	cfg := &BlockParticipantConfig{
		BlockNum:    blkNum,
		VrfSeed:     seed,
		ChainConfig: chainCfg,
	}

	cfg.Proactors = calcParticipantPeers(cfg, chainCfg)
	cfg.Leader = cfg.Proactors[0]	// first one as leader
	log.Infof("server %d, blkNum: %d, state: %d, participants config: %v", self.Index, blkNum,
		self.getState(), cfg.Proactors)

	return cfg, nil
}

func calcParticipantPeers(cfg *BlockParticipantConfig, chain *vconfig.ChainConfig) []uint32 {

	peers := make([]uint32, 0)
	peerMap := make(map[uint32]bool)

	for i := 0; i < len(chain.PosTable); i++ {
		peerId := calcParticipant(cfg.VrfSeed, chain.PosTable, uint32(i))
		if peerId == math.MaxUint32 {
			break
		}
		if _, present := peerMap[peerId]; !present {
			peers = append(peers, peerId)
			peerMap[peerId] = true
		}
	}

	c := int(chain.C)
	if len(peers) > c*3 {
		return peers[:3*c+1]
	}

	for _, peer := range chain.Peers {
		if _, present := peerMap[peer.Index]; !present {
			peers = append(peers, peer.Index)
			peerMap[peer.Index] = true
		}
		if len(peerMap) > c*3 {
			break
		}
	}

	return peers
}

func calcParticipant(vrfSeed vconfig.VRFValue, dposTable []uint32, k uint32) uint32 {
	var v1, v2 uint32
	bIdx := k / 8
	bits1 := k % 8
	bits2 := 8 + bits1 // L - 8 + bits1
	if k >= 512 {
		return math.MaxUint32
	}
	// Note: take 16bits random variable from vrf, if len(dposTable) is not power of 2,
	// this algorithm will break the fairness of vrf. to be fixed
	v1 = uint32(vrfSeed[bIdx]) >> bits1
	if bIdx+1 < uint32(len(vrfSeed)) {
		v2 = uint32(vrfSeed[bIdx+1])
	} else {
		v2 = uint32(vrfSeed[0])
	}

	v2 = v2 & ((1 << bits2) - 1)
	v := (v2 << (8 - bits1)) + v1
	v = v % uint32(len(dposTable))
	return dposTable[v]
}

//
// check if commit msgs has reached consensus
// return
//		@ consensused proposer
//		@ consensused for empty commit
//
func getCommitConsensus(commitMsgs []*blockCommitMsg, N int) (uint32, bool) {
	emptyCommitCount := 0
	emptyCommit := false
	signCount := make(map[uint32]map[uint32]int)
	for _, c := range commitMsgs {
		if c.CommitForEmpty {
			emptyCommitCount++
			if emptyCommitCount > C && !emptyCommit {
				C += 1
				emptyCommit = true
			}
		}
		if _, present := signCount[c.BlockProposer]; !present {
			signCount[c.BlockProposer] = make(map[uint32]int)
		}
		signCount[c.BlockProposer][c.Committer] += 1
		for endorser := range c.EndorsersSig {
			signCount[c.BlockProposer][endorser] += 1
		}
		if len(signCount[c.BlockProposer])+1 >= N-(N-1)/3 {
			return c.BlockProposer, emptyCommit
		}
	}

	return math.MaxUint32, false
}

func (self *Server) findBlockProposal(blkNum uint32, proposer uint32, forEmpty bool) *blockProposalMsg {
	for _, p := range self.blockPool.getBlockProposals(blkNum) {
		if p.Block.getProposer() == proposer {
			return p
		}
	}

	for _, p := range self.msgPool.GetProposalMsgs(blkNum) {
		if pMsg := p.(*blockProposalMsg); pMsg != nil {
			if pMsg.Block.getProposer() == proposer {
				return pMsg
			}
		}
	}

	return nil
}

func (self *Server) validateTxsInProposal(proposal *blockProposalMsg) error {
	// TODO: add VBFT specific verifications
	return nil
}

func (self *Server) heartbeat() {
	//	build heartbeat msg
	msg, err := self.constructHeartbeatMsg()
	if err != nil {
		log.Errorf("failed to build heartbeat msg: %s", err)
		return
	}

	//	send to peer
	self.msgSendC <- &SendMsgEvent{
		ToPeer: math.MaxUint32,
		Msg:    msg,
	}
}

func (self *Server) receiveFromPeer(peerIdx uint32) (uint32, []byte, error) {
	if C, present := self.msgRecvC[peerIdx]; present {
		select {
		case payload := <-C:
			if payload != nil {
				return payload.fromPeer, payload.payload.Data, nil
			}

		case <-self.quitC:
			return 0, nil, fmt.Errorf("server %d quit", self.Index)
		}
	}

	return 0, nil, fmt.Errorf("nil consensus payload")
}

func (self *Server) sendToPeer(peerIdx uint32, data []byte) error {
	peer := self.peerPool.getPeer(peerIdx)
	if peer == nil {
		return fmt.Errorf("send peer failed: failed to get peer %d", peerIdx)
	}
	msg := &p2pmsg.ConsensusPayload{
		Data:  data,
		Owner: self.account.PublicKey,
	}

	sink := common.NewZeroCopySink(nil)
	msg.SerializationUnsigned(sink)
	msg.Signature, _ = signature.Sign(self.account, sink.Bytes())

	cons := msgpack.NewConsensus(msg)
	p2pid, present := self.peerPool.getP2pId(peerIdx)
	if present {
		self.p2p.Transmit(p2pid, cons)
	} else {
		log.Errorf("sendToPeer transmit failed index:%d", peerIdx)
	}
	return nil
}

func (self *Server) broadcast(msg ConsensusMsg) {
	self.msgSendC <- &SendMsgEvent{
		ToPeer: math.MaxUint32,
		Msg:    msg,
	}
}

func (self *Server) broadcastToAll(data []byte) error {
	msg := &p2pmsg.ConsensusPayload{
		Data:  data,
		Owner: self.account.PublicKey,
	}

	sink := common.NewZeroCopySink(nil)
	msg.SerializationUnsigned(sink)
	msg.Signature, _ = signature.Sign(self.account, sink.Bytes())

	self.p2p.Broadcast(msg)
	return nil
}

func (self *Server) CheckSubmitBlock(blkNum uint32, stateRoot common.Uint256) bool {
	cMsgs := self.msgPool.GetBlockSubmitMsgNums(blkNum)
	var stateRootCnt uint32
	for _, msg := range cMsgs {
		c := msg.(*blockSubmitMsg)
		if c != nil {
			if c.BlockStateRoot == stateRoot {
				stateRootCnt++
			} else {
				continue
			}
		}
	}
	m := self.config.N - (self.config.N-1)/3
	if stateRootCnt < uint32(m) {
		return false
	}
	return true
}

func (self *Server) GetChainConfig(blknum uint32) *vconfig.ChainConfig {
	// TODO
	return nil
}

func (self *Server) GetProQuorum(blknum uint32) uint32 {
	return 0
}

func (self *Server) GetPeerPublicKey(peerIdx uint32) keypair.PublicKey {
	return nil
}

func (self *Server) GetQuorum(blknum uint32) uint32 {
	chaincfg := self.GetChainConfig(blknum)
	n := len(chaincfg.Peers)
	return uint32(n - (n-1)/3)
}

func (self *Server) GetEpoch(blknum uint32) uint32 {
	// TODO
	return 0
}

// FIXME
func (self *Server) GetCurrentBlockNo() uint32 {
	return self.blockPool.getSealedBlockNum()+1
}

// FIXME
func (self *Server) GetCurrentEpoch() uint32 {
	return self.GetEpoch(self.GetCurrentBlockNo())
}

// FIXME
func (self *Server) GetCurrentChainConfig() *vconfig.ChainConfig {
	return self.GetChainConfig(self.GetCurrentBlockNo())
}

func (self *Server) canFastForward(targetBlkNum uint32) bool {
	if targetBlkNum > self.GetCommittedBlockNo()+MAX_SYNCING_CHECK_BLK_NUM*4 {
		return false
	}

	N := int(self.config.N)
	// one block less than targetBlkNum is also acceptable for fastforward
	for blkNum := self.GetCurrentBlockNo(); blkNum <= targetBlkNum; blkNum++ {
		// check if pending messages for targetBlkNum reached consensus
		commitMsgs := make([]*CommitMsg, 0)
		for _, msg := range self.blockPool.GetCommitMsgs(blkNum) {
			if c := msg.(*CommitMsg); c != nil {
				commitMsgs = append(commitMsgs, c)
			}
		}
		proposer, _ := getCommitConsensus(commitMsgs, N)
		if proposer == math.MaxUint32 {
			log.Infof("server %d check fastforward false, no consensus in %d commit msg for block %d",
				self.Index, len(commitMsgs), blkNum)
			return false
		}
		// check if the proposal message is available
		foundProposal := false
		for _, msg := range self.msgPool.GetProposalMsgs(blkNum) {
			if p := msg.(*ProposalMsg); p != nil && p.Block.getProposer() == proposer {
				foundProposal = true
				break
			}
		}
		if !foundProposal {
			log.Infof("server %d check fastforward false, no proposal for block %d",
				self.Index, blkNum)
			return false
		}
	}

	if self.syncer.isActive() {
		return false
	}

	return true
}
