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
	"errors"
	"fmt"
	"sync"

	"crypto/sha512"
	"encoding/json"
	"github.com/DNAProject/DNA/common"
	"github.com/DNAProject/DNA/common/log"
	"github.com/DNAProject/DNA/consensus/vbft/config"
	"github.com/DNAProject/DNA/core/store/overlaydb"
	"github.com/DNAProject/DNA/core/types"
	"math"
)

type BlockList []*Block

var errBlockPoolNotReady = errors.New("block pool not ready to validate sub-chain in msg")
var errDupProposal = errors.New("multi proposal from same proposer")
var errDupEndorse = errors.New("multi endorsement from same endorser")
var errDupCommit = errors.New("multi commit from same committer")

type BlockPool struct {
	lock       sync.RWMutex
	HistoryLen uint32

	server     *Server
	chainStore *ChainStore

	votePool        map[uint32][]*Vote            // indexed by height
	nodePool        map[common.Uint256]*BlockNode // indexed by block-hash
	candidateBlocks map[uint32][]*BlockNode       // indexed by height
}

func newBlockPool(server *Server, historyLen uint32, store *ChainStore) (*BlockPool, error) {
	pool := &BlockPool{
		server:          server,
		HistoryLen:      historyLen,
		chainStore:      store,
		votePool:        make(map[uint32][]*Vote),
		nodePool:        make(map[common.Uint256]*BlockNode),
		candidateBlocks: make(map[uint32][]*BlockNode),
	}

	var blkNum uint32
	if store.GetChainedBlockNum() > historyLen {
		blkNum = store.GetChainedBlockNum() - historyLen
	}

	// load history blocks from chainstore
	for ; blkNum <= store.GetChainedBlockNum(); blkNum++ {
		blk, err := store.getBlock(blkNum)
		if err != nil {
			return nil, fmt.Errorf("failed to load block %d: %s", blkNum, err)
		}
		execMerkleRoot, err := store.getExecMerkleRoot(blkNum)
		if err != nil {
			return nil, fmt.Errorf("failed to load exec merkle root %d: %s", blkNum, err)
		}
		node := &BlockNode{
			Height:         blkNum,
			View:           math.MaxUint32,
			BlockHash:      blk.Hash(),
			PrevBlockHash:  blk.Header.PrevBlockHash,
			NodeState:      FINALIZED,
			Proposal:       blk,
			ExecMerkleRoot: execMerkleRoot,
		}
		pool.nodePool[blk.Hash()] = node
		pool.candidateBlocks[blkNum] = []*BlockNode{node}
	}

	return pool, nil
}

func (pool *BlockPool) clean() {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	pool.votePool = make(map[uint32][]*Vote)
	pool.nodePool = make(map[common.Uint256]*BlockNode)
	pool.candidateBlocks = make(map[uint32][]*BlockNode)
}

func (pool *BlockPool) validateVoteMsg(msg *VoteMsg) error {
	if len(msg.Rounds) == 0 {
		return fmt.Errorf("vote msg without roundvote")
	}

	// validate: proposal msg is only at the tail
	// validate: epoch# in VoteMsg
	for idx, vote := range msg.Rounds {
		if idx != len(msg.Rounds)-1 && vote.Proposal != nil {
			return fmt.Errorf("proposal contained in non-tailed round vote")
		}
		epoch := pool.server.GetEpoch(vote.Height)
		if vote.Epoch != epoch {
			return fmt.Errorf("unmatch epoch in round vote (%d,%d), %d vs %d", vote.Height, vote.View, vote.Epoch, epoch)
		}
	}

	pool.lock.RLock()
	defer pool.lock.RUnlock()

	subChain, err := pool.getSubChainRLocked(msg)
	if err != nil {
		return err
	}

	// no new-view-justify from peer outside sub-chain
	// no other commit-msg from peer outside sub-chain
	for _, n := range subChain {
		candidates := pool.candidateBlocks[n.Height]
		for _, c := range candidates {
			if n == c {
				continue
			}
			if _, present := c.ChangeView[msg.PeerID]; present {
				return fmt.Errorf("node %d request change view at block %d", msg.PeerID, c.Height)
			}
			if _, present := c.Commit[msg.PeerID]; present {
				return fmt.Errorf("node %d request commit at another fork, block %d", msg.PeerID, c.Height)
			}
			if _, present := c.Result[msg.PeerID]; present {
				return fmt.Errorf("node %d has commit result at another fork, block %d", msg.PeerID, c.Height)
			}
		}
	}
	return nil
}

func (pool *BlockPool) addVoteMsg(msg *VoteMsg) error {
	// add votes to node-pool and candidate blocks
	pool.lock.RLock()
	subchain, err := pool.getSubChainRLocked(msg)
	pool.lock.RUnlock()
	if err != nil {
		return err
	}

	pool.lock.Lock()
	defer pool.lock.Unlock()

	// add msg to vote-pool
	for _, r := range msg.Rounds {
		if _, present := pool.votePool[r.Height]; !present {
			pool.votePool[r.Height] = make([]*Vote, 0)
		}
		vote := &Vote{
			Height: r.Height,
			Msg:    r,
		}
		pool.votePool[r.Height] = append(pool.votePool[r.Height], vote)

		blkHash := r.BlockHash
		if _, present := pool.nodePool[blkHash]; !present {
			// new node, find from subchain
			var node *BlockNode
			for _, n := range subchain {
				if n.BlockHash == r.BlockHash {
					node = n
					break
				}
			}
			if node == nil {
				return fmt.Errorf("add roundvote (%d,%d), failed to blocknode", r.Height, r.View)
			}
			// add to pool
			pool.nodePool[r.BlockHash] = node
			if _, present := pool.candidateBlocks[r.Height]; !present {
				pool.candidateBlocks[r.Height] = make([]*BlockNode, 0)
			}
			pool.candidateBlocks[r.Height] = append(pool.candidateBlocks[r.Height], node)

		}
		if err := pool.addVoteLocked(msg.PeerID, subchain, vote); err != nil {
			return fmt.Errorf("add roundvote (%d, %d): %s", r.Height, r.View, err)
		}
	}
	return nil
}

func (pool *BlockPool) addVoteLocked(peerFrom uint32, subchain []*BlockNode, vote *Vote) error {
	msg := vote.Msg
	node := pool.nodePool[msg.BlockHash]
	if msg.NewView != nil {
		node.addChangeView(peerFrom, vote)
	}
	if msg.Result != nil {
		if err := node.addResult(peerFrom, msg.Result); err != nil {
			return err
		}
	}
	if msg.Commit != nil {
		pool.addCommitVoteToSubchain(peerFrom, subchain, vote)
	}
	if msg.Prepare != nil {
		pool.addPrepareVoteToSubchain(peerFrom, subchain, vote)
	}
	if msg.Proposal != nil {
		if node.Proposal != nil {
			if node.Proposal.Hash() != msg.Proposal.Block.Hash() {
				return fmt.Errorf("proposal unmatched blockhash")
			}
		} else {
			if !node.isLeader(peerFrom) {
				return fmt.Errorf("proposal (%d,%d) from invalid leader %d", node.Height, node.View, peerFrom)
			}
			node.addProposal(msg.Proposal.Block)
			// TODO: update txs when processing proposal
		}
	}
	return nil
}

func (pool *BlockPool) addCommitVoteToSubchain(nodeFrom uint32, subchain []*BlockNode, vote *Vote) {
	for _, node := range subchain {
		node.addCommitVote(nodeFrom, vote)
	}
}

func (pool *BlockPool) addPrepareVoteToSubchain(nodeFrom uint32, subchain []*BlockNode, vote *Vote) {
	for _, node := range subchain {
		node.addPrepareVote(nodeFrom, vote)
	}
}

func (pool *BlockPool) getSubChainRLocked(msg *VoteMsg) ([]*BlockNode, error) {
	// pool.RLock must be held

	maxH := msg.Rounds[len(msg.Rounds)-1].Height
	startH := msg.Rounds[0].Height

	nodesInMsg := make([]*BlockNode, 0) // prev-block of vote included
	blkHash := msg.Rounds[len(msg.Rounds)-1].BlockHash
	beginBlkHash := msg.Rounds[0].PrevBlockHash
	for blkHash != beginBlkHash && uint32(len(nodesInMsg)) < (maxH-startH+2) {
		n := pool.nodePool[blkHash]
		if n != nil {
			// validate r
			for _, r := range msg.Rounds {
				if r.BlockHash == blkHash {
					if r.Epoch != n.Epoch || r.Height != n.Height || r.View != n.View || r.PrevBlockHash != n.PrevBlockHash {
						return nil, fmt.Errorf("vote msg unmatched with msg in pool (%d, %d)", r.Height, r.View)
					}
				}
			}
			nodesInMsg = append(nodesInMsg, n)
			blkHash = n.PrevBlockHash
			continue
		}
		for _, r := range msg.Rounds {
			if r.BlockHash == blkHash {
				n := newPoolNode(r.Epoch, r.Height, r.View, r.PrevBlockHash, r.BlockHash)
				if err := pool.updateParticipantConfigLocked(n); err != nil {
					return nil, fmt.Errorf("failed to update block (%d,%d) participant config: %s", r.Height, r.View, err)
				}
				nodesInMsg = append(nodesInMsg, n)
				blkHash = n.PrevBlockHash
				continue
			}
		}
		return nil, errBlockPoolNotReady
	}

	// all round in a vote should be in a chain
	subChain := make([]*BlockNode, len(nodesInMsg))
	for i := range subChain {
		subChain[i] = nodesInMsg[len(nodesInMsg)-1-i]
		if i > 0 {
			if subChain[i].PrevBlockHash != subChain[i-1].BlockHash {
				return nil, fmt.Errorf("invalid subchain in votemsg from %d", msg.PeerID)
			}
		}
	}

	return subChain, nil
}

func (pool *BlockPool) validateViewMsg(msg *ChangeViewMsg) error {
	return nil
}

func (pool *BlockPool) getActiveView(blknum uint32) uint32 {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	if nodes, present := pool.candidateBlocks[blknum]; present {
		maxPendingView := uint32(0)
		for _, n := range nodes {
			switch n.NodeState {
			case PENDING:
				// skip
			case READY:
				fallthrough
			case LOCKED:
				fallthrough
			case SEALED:
				fallthrough
			case FINALIZED:
				return n.View
			case FAILED:
				fallthrough
			case LOCK_FAILED:
				// next view
				if n.View+1 > maxPendingView {
					maxPendingView = n.View + 1
				}
			}
		}

		return maxPendingView
	}

	// no proposal of blknum
	return 0
}

func (pool *BlockPool) prepareBlockNodeLocked(n *BlockNode) error {
	n.NodeState = READY
	return nil
}

func (pool *BlockPool) commitBlockNodeLocked(n *BlockNode) error {
	n.NodeState = LOCKED
	return nil
}

func (pool *BlockPool) sealBlockNodeLocked(n *BlockNode) error {
	n.NodeState = SEALED
	return nil
}

func (pool *BlockPool) finalizeBlockNodeLocked(n *BlockNode) error {
	n.NodeState = FINALIZED
	return nil
}

func (pool *BlockPool) onBlockSealed(blockNum uint32) {
	if blockNum <= pool.HistoryLen {
		return
	}

	pool.lock.Lock()
	defer pool.lock.Unlock()

	toFreeCandidates := make([]uint32, 0)
	for n := range pool.candidateBlocks {
		if n < blockNum-pool.HistoryLen {
			toFreeCandidates = append(toFreeCandidates, n)
		}
	}
	for _, n := range toFreeCandidates {
		delete(pool.candidateBlocks, n)
	}
}

func (pool *BlockPool) getExecMerkleRoot(blkNum uint32) (common.Uint256, error) {
	pool.lock.RLock()
	defer pool.lock.RUnlock()
	return pool.chainStore.getExecMerkleRoot(blkNum)
}

func (pool *BlockPool) getExecWriteSet(blkNum uint32) *overlaydb.MemDB {
	pool.lock.RLock()
	defer pool.lock.RUnlock()
	return pool.chainStore.getExecWriteSet(blkNum)
}

func (pool *BlockPool) submitBlock(blkNum uint32) error {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	return pool.chainStore.submitBlock(blkNum)
}

func (pool *BlockPool) getSealedBlock(blkNum uint32) (*types.Block, error) {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	if blkNum <= pool.chainStore.GetChainedBlockNum() {
		return pool.chainStore.getBlock(blkNum)
	}

	if nodes, present := pool.candidateBlocks[blkNum]; present {
		for _, n := range nodes {
			if n.NodeState >= SEALED {
				if n.Proposal == nil {
					return nil, fmt.Errorf("no proposal of sealed block %d", blkNum)
				}
				return n.Proposal, nil
			}
		}
		return nil, fmt.Errorf("blocknum %d not sealed", blkNum)
	}

	return nil, fmt.Errorf("not found block %d", blkNum)
}

func (pool *BlockPool) getSealedBlockNum() uint32 {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	maxHeight := pool.chainStore.GetChainedBlockNum()
	for h, nodes := range pool.candidateBlocks {
		if h <= maxHeight {
			continue
		}
		for _, n := range nodes {
			if n.NodeState >= SEALED && n.NodeState < MAX_SUCCESS_STATE {
				maxHeight = h
			}
		}
	}

	return maxHeight
}

func (pool *BlockPool) getSafePreparedBlockNum() uint32 {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	maxHeight := pool.chainStore.GetChainedBlockNum()
	for h, nodes := range pool.candidateBlocks {
		if h <= maxHeight {
			continue
		}
		for _, n := range nodes {
			if n.NodeState >= LOCKED && n.NodeState < MAX_SUCCESS_STATE {
				maxHeight = h
			}
		}
	}

	return maxHeight
}

func (pool *BlockPool) getFinalizedBlockNum() uint32 {
	return pool.chainStore.GetChainedBlockNum()
}

func (pool *BlockPool) getView(blknum uint32) uint32 {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	if blknum <= pool.chainStore.GetChainedBlockNum() {
		return math.MaxUint32
	}

	var maxv uint32
	for _, n := range pool.candidateBlocks[blknum] {
		v := n.View
		if n.NodeState > MAX_SUCCESS_STATE {
			v += 1
		}
		if v > maxv {
			maxv = v
		}
	}
	return maxv
}

func (pool *BlockPool) getHighestBlockNum() uint32 {
	pool.lock.RLock()
	pool.lock.RUnlock()

	height := uint32(0)
	for h := range pool.candidateBlocks {
		if h > height {
			height = h
		}
	}

	return height
}

func (pool *BlockPool) isProposalReady(height, view uint32) bool {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	for _, n := range pool.candidateBlocks[height] {
		if n.View == view {
			return n.Proposal != nil
		}
	}

	return false
}

func (pool *BlockPool) isLeaderOf(height, view uint32, peerIdx uint32) bool {
	cfg, err := pool.GetParticipantConfig(height, view)
	if err != nil {
		log.Infof("get participant config (%d,%d): %s", height, view, err)
		return false
	}

	return peerIdx == cfg.Leader
}

func (pool *BlockPool) isProactorOf(height, view uint32, peerIdx uint32) bool {
	cfg, err := pool.GetParticipantConfig(height, view)
	if err != nil {
		log.Infof("get participant config (%d,%d): %s", height, view, err)
		return false
	}
	for _, p := range cfg.Proactors {
		if p == peerIdx {
		}
		return true
	}
	return false
}

func (pool *BlockPool) isProPrepared(height, view uint32) bool {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	return pool.isProPreparedLocked(height, view)
}

func (pool *BlockPool) isProPreparedLocked(height, view uint32) bool {
	n := pool.getBlockNodeLocked(height, view)
	if n == nil || n.ParticipantConfig == nil {
		log.Infof("failed to get participant config when check proprepared (%d,%d), %v", height, view, n)
		return false
	}
	cfg := n.ParticipantConfig
	peers := make(map[uint32]bool)
	for _, p := range cfg.Proactors {
		peers[p] = true
	}

	cnt := 0
	for p, _ := range n.Prepare {
		if peers[p] {
			cnt++
		}
	}
	return cnt >= len(cfg.Proactors)-(len(cfg.Proactors)-1)/3
}

type VrfSeedData struct {
	BlockNum uint32 `json:"block_num"`
	View     uint32 `json:"view"`
	VrfValue []byte `json:"vrf_value"`
}

func getBlockVrfValue(block *types.Block) ([]byte, []byte, error) {
	if block == nil {
		return nil, nil, fmt.Errorf("nil block in getBlockVrfValue")
	}

	blkInfo := vconfig.VbftBlockInfo{}
	if err := json.Unmarshal(block.Header.ConsensusPayload, blkInfo); err != nil {
		return nil, nil, fmt.Errorf("unmarshal blockinfo (%d): %s", block.Header.Height, err)
	}
	return blkInfo.VrfValue, blkInfo.VrfProof, nil
}


func (pool *BlockPool) GetParticipantConfig(height, view uint32) (*BlockParticipantConfig, error) {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	node := pool.getBlockNodeLocked(height, view)
	if node == nil {
		// not find blockNode
		return nil, fmt.Errorf("no blocknode for (%d,%d)", height, view)
	}
	if node.ParticipantConfig != nil {
		return node.ParticipantConfig, nil
	}

	if err := pool.updateParticipantConfigLocked(node); err != nil {
		return nil, err
	}

	return node.ParticipantConfig, nil
}

func (pool *BlockPool) updateParticipantConfigLocked(node *BlockNode) error {
	height, view := node.Height, node.View

	// get VrfValue of parent block
	parentBlock := pool.getPrevProposalLocked(height, view)
	parentBlockVrf, _, err := getBlockVrfValue(parentBlock)
	if err != nil {
		return fmt.Errorf("failed to get parent block (%d,%d) vrf: %s", height, view, err)
	}

	// get vrf value
	data, err := json.Marshal(&VrfSeedData{
		BlockNum: height,
		View:     view,
		VrfValue: parentBlockVrf,
	})

	t := sha512.Sum512(data)
	f := sha512.Sum512(t[:])
	v := vconfig.VRFValue(f)

	// construct ParticipantConfig
	chaincfg := pool.server.GetChainConfig(height)
	if chaincfg == nil {
		return fmt.Errorf("failed to get chainconfig of block (%d,%d)", height, view)
	}
	leader, proactors := getParticipantPeers(v, chaincfg)

	node.ParticipantConfig = &BlockParticipantConfig{
		BlockNum:    height,
		VrfSeed:     v,
		ChainConfig: chaincfg, // FIXME: copy
		Leader:      leader,
		Proactors:   proactors,
	}

	return nil
}

func getParticipantPeers(vrf vconfig.VRFValue, chainCfg *vconfig.ChainConfig) (uint32, []uint32) {
	peers := make([]uint32, 0)
	peerMap := make(map[uint32]bool)

	// 1. select peers as many as possible
	c := int(chainCfg.C)
	for i := 0; i < len(chainCfg.PosTable); i++ {
		peerId := calcParticipant(vrf, chainCfg.PosTable, uint32(i))
		if peerId == math.MaxUint32 {
			break
		}
		if _, present := peerMap[peerId]; !present {
			peers = append(peers, peerId)
			peerMap[peerId] = true
			if len(peerMap) > c {
				break
			}
		}
	}
	if len(peerMap) < c+1 {
		for _, peer := range chainCfg.Peers {
			if _, present := peerMap[peer.Index]; !present {
				peers = append(peers, peer.Index)
				peerMap[peer.Index] = true
			}
			if len(peerMap) > c {
				break
			}
		}
	}

	return peers[0], peers[1:]
}

func (pool *BlockPool) hasCommittedOnForksLocked(height, view uint32) bool {
	for _, node := range pool.candidateBlocks[height] {
		if node.View == view {
			continue
		}
		if node.NodeState >= SEALED && node.NodeState < MAX_SUCCESS_STATE {
			return true
		}
	}

	return false
}

func (pool *BlockPool) getProposalJustify(height, view uint32) map[uint32][]byte {
	// get new-view justify
	if view == 0 {
		return nil
	}

	just := make(map[uint32][]byte)
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	node := pool.getBlockNodeLocked(height, view-1)
	if node == nil {
		return nil
	}
	for peer, v := range node.ChangeView {
		if v.Msg != nil && v.Msg.NewView != nil && v.Msg.NewView.NewViewJustify != nil {
			if sig := v.Msg.NewView.NewViewJustify.Sigs[peer]; sig != nil {
				just[peer] = sig
			}
		}
	}

	return nil
}

func (pool *BlockPool) getPreparedJustify(height, view uint32) map[uint32][]byte {
	just := make(map[uint32][]byte)

	pool.lock.RLock()
	defer pool.lock.RUnlock()
	n := pool.getBlockNodeLocked(height, view)
	if n == nil {
		return nil
	}
	for peer, v := range n.Prepare {
		if v.Msg != nil && v.Msg.Prepare != nil {
			just[peer] = v.Msg.Prepare.Sig
		}
	}

	return just
}

func (pool *BlockPool) getCommittedJustify(height, view uint32) map[uint32][]byte {
	just := make(map[uint32][]byte)

	pool.lock.RLock()
	defer pool.lock.RUnlock()
	for _, n := range pool.candidateBlocks[height] {
		if n.View == view {
			for peer, v := range n.Commit {
				if v.Msg != nil && v.Msg.Commit != nil {
					just[peer] = v.Msg.Commit.Sig
				}
			}
		}
	}
	return nil
}

func (pool *BlockPool) setSealed(height, view uint32) error {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	if pool.getChainedBlockNumber() >= height {
		return nil
	}

	// get blocknode
	node := pool.getBlockNodeLocked(height, view)
	if node == nil {
		return fmt.Errorf("failed to get blocknode (%d,%d)", height, view)
	}

	blk := node.Proposal
	// get commit-justifies, update signatures
	if just := node.getCommitJustifies(); just != nil {
		sigdata := make([][]byte, 0)
		for _, s := range just.Sigs {
			sigdata = append(sigdata, s)
		}
		blk.Header.SigData = sigdata
	}

	// add to chainstore
	if err := pool.chainStore.AddBlock(blk); err != nil {
		return fmt.Errorf("failed to seal block (%d,%d) to chainstore: %s", height, view, err)
	}

	// submit
	stateRoot, err := pool.chainStore.getExecMerkleRoot(pool.chainStore.GetChainedBlockNum())
	if err != nil {
		log.Errorf("handleBlockSubmit failed:%s", err)
		return nil
	}
	if blocksubmitMsg, _ := pool.server.constructBlockSubmitMsg(pool.chainStore.GetChainedBlockNum(), stateRoot); blocksubmitMsg != nil {
		pool.server.broadcast(blocksubmitMsg)
		pool.server.bftActionC <- &BftAction{
			Type:     SubmitBlock,
			BlockNum: pool.chainStore.GetChainedBlockNum(),
		}
	}
	return nil
}

// return true if committed on forks
func (pool *BlockPool) checkCommittedOnForks(height, view uint32) bool {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	return pool.hasCommittedOnForksLocked(height, view)
}

func (pool *BlockPool) getProposal(height, view uint32) *types.Block {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	if n := pool.getBlockNodeLocked(height, view); n != nil {
		return n.Proposal
	}
	return nil
}

// CALLED WHEN CONSTRUCTING NEW PROPOSAL, NEED GET PREV-PROPOSAL FROM LAST-BLOCKHASH
func (pool *BlockPool) getPrevProposal(height, view uint32) *types.Block {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	return pool.getPrevProposalLocked(height, view)
}

func (pool *BlockPool) getPrevProposalLocked(height, view uint32) *types.Block {
	n := pool.getBlockNodeLocked(height, view)
	if n == nil || n.Proposal == nil {
		return nil
	}

	prevNode := pool.nodePool[n.Proposal.Header.PrevBlockHash]
	if prevNode == nil || prevNode.Proposal == nil {
		return nil
	}

	return prevNode.Proposal
}

func (pool *BlockPool) getChainedBlockNumber() uint32 {
	return pool.chainStore.GetChainedBlockNum()
}

func (pool *BlockPool) getBlockNodeLocked(height, view uint32) *BlockNode {
	for _, n := range pool.candidateBlocks[height] {
		if n.View == view {
			return n
		}
	}
	return nil
}

func (pool *BlockPool) getPendingTxLocked(node *BlockNode) (map[common.Uint256]bool, error) {
	pendingTxs := make(map[common.Uint256]bool)

	startHeight := pool.getChainedBlockNumber()
	for h := node.Height-1; h > startHeight; h-- {
		if node.Proposal == nil {
			return nil, fmt.Errorf("failed to get proposal in blocknode (%d)", h)
		}
		prevHash := node.Proposal.Header.PrevBlockHash
		node = pool.nodePool[prevHash]
		if node == nil {
			return nil, fmt.Errorf("failed to get blocknode (%d): %s", h-1, prevHash.ToHexString())
		}
		for _, tx := range node.Proposal.Transactions {
			pendingTxs[tx.Hash()] = true
		}
	}

	return pendingTxs, nil
}
