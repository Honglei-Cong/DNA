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
	"encoding/json"
	"fmt"
	"time"

	"github.com/DNAProject/DNA/common"
	"github.com/DNAProject/DNA/common/log"
	"github.com/DNAProject/DNA/consensus/vbft/config"
	"github.com/DNAProject/DNA/core/ledger"
	"github.com/DNAProject/DNA/core/signature"
	"github.com/DNAProject/DNA/core/types"
	"github.com/ontio/ontology-crypto/keypair"
)

type ConsensusMsgPayload struct {
	Type    MsgType `json:"type"`
	Len     uint32  `json:"len"`
	Payload []byte  `json:"payload"`
}

func DeserializeConsensusMsg(msgPayload []byte) (ConsensusMsg, error) {

	m := &ConsensusMsgPayload{}
	if err := json.Unmarshal(msgPayload, m); err != nil {
		return nil, fmt.Errorf("unmarshal consensus msg payload: %s", err)
	}
	if m.Len < uint32(len(m.Payload)) {
		return nil, fmt.Errorf("invalid payload length: %d", m.Len)
	}

	switch m.Type {
	case VoteMessage:
		t := &VoteMsg{}
		if err := t.Deserialize(common.NewZeroCopySource(m.Payload)); err != nil {
			return nil, fmt.Errorf("failed to unmarshal msg (type: %d): %s", m.Type, err)
		}
		return t, nil
	case ChangeViewMessage:
		t := &ChangeViewMsg{}
		if err := t.Deserialize(common.NewZeroCopySource(m.Payload)); err != nil {
			return nil, fmt.Errorf("failed to unmarshal msg (type: %d): %s", m.Type, err)
		}
		return t, nil
	case PeerHandshakeMessage:
		t := &peerHandshakeMsg{}
		if err := t.Deserialize(common.NewZeroCopySource(m.Payload)); err != nil {
			return nil, fmt.Errorf("failed to unmarshal msg (type: %d): %s", m.Type, err)
		}
		return t, nil
	case PeerHeartbeatMessage:
		t := &peerHeartbeatMsg{}
		if err := t.Deserialize(common.NewZeroCopySource(m.Payload)); err != nil {
			return nil, fmt.Errorf("failed to unmarshal msg (type: %d): %s", m.Type, err)
		}
		return t, nil
	case BlockInfoFetchMessage:
		t := &BlockInfoFetchMsg{}
		if err := t.Deserialize(common.NewZeroCopySource(m.Payload)); err != nil {
			return nil, fmt.Errorf("failed to unmarshal msg (type: %d): %s", m.Type, err)
		}
		return t, nil
	case BlockInfoFetchRespMessage:
		t := &BlockInfoFetchRespMsg{}
		if err := t.Deserialize(common.NewZeroCopySource(m.Payload)); err != nil {
			return nil, fmt.Errorf("failed to unmarshal msg (type: %d): %s", m.Type, err)
		}
		return t, nil
	case BlockFetchMessage:
		t := &blockFetchMsg{}
		if err := t.Deserialize(common.NewZeroCopySource(m.Payload)); err != nil {
			return nil, fmt.Errorf("failed to unmarshal msg (type: %d): %s", m.Type, err)
		}
		return t, nil
	case BlockFetchRespMessage:
		t := &BlockFetchRespMsg{}
		if err := t.Deserialize(common.NewZeroCopySource(m.Payload)); err != nil {
			return nil, fmt.Errorf("failed to Deserialize msg (type: %d): %s", m.Type, err)
		}
		return t, nil
	case ProposalFetchMessage:
		t := &proposalFetchMsg{}
		if err := t.Deserialize(common.NewZeroCopySource(m.Payload)); err != nil {
			return nil, fmt.Errorf("failed to unmarshal msg (type: %d): %s", m.Type, err)
		}
		return t, nil
	case BlockSubmitMessage:
		t := &blockSubmitMsg{}
		if err := t.Deserialize(common.NewZeroCopySource(m.Payload)); err != nil {
			return nil, fmt.Errorf("failed to unmarshal msg (type: %d): %s", m.Type, err)
		}
		return t, nil
	}

	return nil, fmt.Errorf("unknown msg type: %d", m.Type)
}

func SerializeConsensusMsg(msg ConsensusMsg) ([]byte, error) {

	sink := common.NewZeroCopySink(nil)
	if err := msg.Serialize(sink); err != nil {
		return nil, err
	}

	// TODO: change json
	return json.Marshal(&ConsensusMsgPayload{
		Type:    msg.Type(),
		Len:     uint32(len(sink.Bytes())),
		Payload: sink.Bytes(),
	})
}

func (self *Server) constructHandshakeMsg() (*peerHandshakeMsg, error) {

	blkNum := self.blockPool.getChainedBlockNumber()
	block, _ := self.blockPool.getSealedBlock(blkNum)
	if block == nil {
		return nil, fmt.Errorf("failed to get sealed block, current chained block: %d", blkNum)
	}

	chaincfg := self.configPool.getChainConfig(blkNum)
	if chaincfg != nil {
		return &peerHandshakeMsg{
			CommittedBlockNumber: blkNum,
			CommittedBlockHash:   block.Hash(),
			ChainConfig:          chaincfg,
		}, nil
	}

	return nil, fmt.Errorf("constructing handshake msg, failed to get chainconfig (%d)", blkNum)
}

func (self *Server) constructHeartbeatMsg() (*peerHeartbeatMsg, error) {

	blkNum := self.blockPool.getChainedBlockNumber()
	block, _ := self.blockPool.getSealedBlock(blkNum)
	if block == nil {
		return nil, fmt.Errorf("failed to get sealed block, current chained block: %d", blkNum)
	}

	chaincfg := self.configPool.getChainConfig(blkNum)
	if chaincfg != nil {
		return &peerHeartbeatMsg{
			CommittedBlockNumber: blkNum,
			CommittedBlockHash:   block.Hash(),
			Epoch:                chaincfg.View,
		}, nil
	}

	return nil, fmt.Errorf("constructing heartbeat, failed to get chainconfig (%d)", blkNum)
}

func (self *Server) constructBlock(blkNum uint32, prevBlkHash common.Uint256, txs []*types.Transaction, consensusPayload []byte, blocktimestamp uint32) (*types.Block, error) {
	txHash := []common.Uint256{}
	for _, t := range txs {
		txHash = append(txHash, t.Hash())
	}
	lastBlock, _ := self.blockPool.getSealedBlock(blkNum - 1)
	if lastBlock == nil {
		log.Errorf("constructBlock getlastblock failed blknum:%d", blkNum-1)
		return nil, fmt.Errorf("constructBlock getlastblock failed blknum:%d", blkNum-1)
	}

	// FIXME： compute merkle root with pending blocks in block-pool
	txRoot := common.ComputeMerkleRoot(txHash)
	blockRoot := ledger.DefLedger.GetBlockRootWithNewTxRoots(lastBlock.Header.Height, []common.Uint256{lastBlock.Header.TransactionsRoot, txRoot})

	blkHeader := &types.Header{
		PrevBlockHash:    prevBlkHash,
		TransactionsRoot: txRoot,
		BlockRoot:        blockRoot,
		Timestamp:        blocktimestamp,
		Height:           uint32(blkNum),
		ConsensusData:    common.GetNonce(),
		ConsensusPayload: consensusPayload,
	}
	blk := &types.Block{
		Header:       blkHeader,
		Transactions: txs,
	}
	blkHash := blk.Hash()
	sig, err := signature.Sign(self.account, blkHash[:])
	if err != nil {
		return nil, fmt.Errorf("sign block failed, block hash:%s, error: %s", blkHash.ToHexString(), err)
	}
	blkHeader.Bookkeepers = []keypair.PublicKey{self.account.PublicKey}
	blkHeader.SigData = [][]byte{sig}

	return blk, nil
}

func (self *Server) constructProposalMsg(blkNum, view uint32, sysTxs, userTxs []*types.Transaction, chainconfig *vconfig.ChainConfig) (*ProposalMsg, error) {
	prevBlk := self.blockPool.getPrevProposal(blkNum, view)
	if prevBlk == nil {
		return nil, fmt.Errorf("failed to get prevBlock (%d)", blkNum-1)
	}
	vbftInfo := getVbftInfo(prevBlk)
	if vbftInfo == nil {
		return nil, fmt.Errorf("failed to bet")
	}

	blocktimestamp := uint32(time.Now().Unix())
	if prevBlk.Header.Timestamp >= blocktimestamp {
		blocktimestamp = prevBlk.Header.Timestamp + 1
	}

	vrfValue, vrfProof, err := computeVrf(self.account.PrivateKey, blkNum, view, vbftInfo.VrfValue)
	if err != nil {
		return nil, fmt.Errorf("failed to get vrf and proof: %s", err)
	}

	lastConfigBlkNum := vbftInfo.LastConfigBlockNum
	if vbftInfo.NewChainConfig != nil {
		lastConfigBlkNum = blkNum - 1
	}
	if chainconfig != nil {
		lastConfigBlkNum = blkNum
	}
	vbftBlkInfo := &vconfig.VbftBlockInfo{
		Proposer:           self.Index,
		VrfValue:           vrfValue,
		VrfProof:           vrfProof,
		LastConfigBlockNum: lastConfigBlkNum,
		NewChainConfig:     chainconfig,
	}
	consensusPayload, err := json.Marshal(vbftBlkInfo)
	if err != nil {
		return nil, err
	}

	blk, err := self.constructBlock(blkNum, prevBlk.Hash(), append(sysTxs, userTxs...), consensusPayload, blocktimestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to constuct blk: %s", err)
	}

	msg := &ProposalMsg{
		Block: blk,
	}

	return msg, nil
}

func (self *Server) constructPrepareMsg(blknum, view uint32) (*PrepareMsg, error) {
	prepareJusts := self.blockPool.getPreparedJustify(blknum, view)
	if prepareJusts == nil {
		return nil, fmt.Errorf("construct prepare(%d,%d), failed to get prepare justs", blknum, view)
	}

	justData, err := json.Marshal(&ProposalJustifyData{
		Height:      blknum,
		View:        view,
		JustifyType: VotePrepare,
	})
	if err != nil {
		return nil, fmt.Errorf("construct prepare, failed to build just data: %s", err)
	}
	sig, err := signature.Sign(self.account, justData)
	if err != nil {
		return nil, fmt.Errorf("construct prepare, failed to sign block(%d,%d), err: %s", blknum, view, err)
	}

	msg := &PrepareMsg{
		PrepareJustify: &Justify{
			Sigs: prepareJusts,
		},
		Sig: sig,
	}

	return msg, nil
}

func (self *Server) constructCommitMsg(blknum, view uint32) (*CommitMsg, error) {
	prepareJusts := self.blockPool.getPreparedJustify(blknum, view)
	if prepareJusts == nil {
		return nil, fmt.Errorf("construct commit(%d,%d), failed to get prepare justs", blknum, view)
	}

	blk := self.blockPool.getProposal(blknum, view)
	if blk == nil {
		return nil, fmt.Errorf("construct commit(%d,%d), failed to get proposal", blknum, view)
	}
	blkhash := blk.Header.Hash()
	sig, err := signature.Sign(self.account, blkhash[:])
	if err != nil {
		return nil, fmt.Errorf("construct commit, failed to sign block(%d,%d), err: %s", blknum, view, err)
	}

	msg := &CommitMsg{
		PreparedJustify: &Justify{
			Sigs: prepareJusts,
		},
		Sig: sig,
	}

	return msg, nil
}

func (self *Server) constructMerkleResultMsg(blknum, view uint32, stateRoot common.Uint256) (*ExecMerkleMsg, error) {
	sig, err := signature.Sign(self.account, stateRoot[:])
	if err != nil {
		return nil, fmt.Errorf("construct merkle result (%d,%d), failed to sign: %s", blknum, view, err)
	}
	return &ExecMerkleMsg{
		ExecMerkleRoot: stateRoot,
		Sig:            sig,
	}, nil
}

func (self *Server) constructBlockFetchMsg(blkNum uint32) *blockFetchMsg {
	return &blockFetchMsg{
		BlockNum: blkNum,
	}
}

func (self *Server) constructBlockFetchRespMsg(blkNum uint32, blk *types.Block, blkHash common.Uint256) *BlockFetchRespMsg {
	return &BlockFetchRespMsg{
		BlockNumber: blkNum,
		BlockHash:   blkHash,
		BlockData:   blk,
	}
}

func (self *Server) constructBlockInfoFetchMsg(startBlkNum uint32) *BlockInfoFetchMsg {
	return &BlockInfoFetchMsg{
		StartBlockNum: startBlkNum,
	}
}

func (self *Server) constructBlockInfoFetchRespMsg(blockInfos []*BlockInfo_) *BlockInfoFetchRespMsg {
	return &BlockInfoFetchRespMsg{
		Blocks: blockInfos,
	}
}

func (self *Server) constructProposalFetchMsg(blkNum uint32, view uint32) *proposalFetchMsg {
	return &proposalFetchMsg{
		BlockNum: blkNum,
		View:     view,
	}
}

func (self *Server) constructBlockSubmitMsg(blkNum uint32, stateRoot common.Uint256) (*blockSubmitMsg, error) {
	submitSig, err := signature.Sign(self.account, stateRoot[:])
	if err != nil {
		return nil, fmt.Errorf("submit failed to sign stateroot hash:%x, err: %s", stateRoot, err)
	}
	msg := &blockSubmitMsg{
		BlockStateRoot: stateRoot,
		BlockNum:       blkNum,
		SubmitMsgSig:   submitSig,
	}
	return msg, nil
}

func (self *Server) constructRoundVoteMsg(blknum uint32, msg *RoundRspMsg) (*RoundVoteMsg, error) {
	// get highest view
	vw := uint32(0)
	for v := range msg.msgs {
		if v >= vw {
			vw = v
		}
	}
	if len(msg.msgs[vw]) == 0 {
		return nil, nil
	}

	// get epoch
	chaincfg := self.configPool.getChainConfig(blknum)
	if chaincfg == nil {
		return nil, fmt.Errorf("failed to get chainconfig when constructing roundvote %d", blknum)
	}
	roundVote := &RoundVoteMsg{
		Epoch:  chaincfg.View,
		Height: blknum,
		View:   vw,
	}

	// add msgs to roundvote
	var ok bool
	for _, m := range msg.msgs[vw] {
		switch m.Type() {
		case VoteNewView:
			roundVote.NewView, ok = m.(*NewViewMsg)
			if !ok {
				log.Errorf("invalid newview msg in construct roundvote (%d)", blknum)
			}
		case Proposal:
			roundVote.Proposal, ok = m.(*ProposalMsg)
			if ok {
				roundVote.ProposerID = self.Index
				roundVote.BlockHash = roundVote.Proposal.Block.Hash()
				roundVote.PrevBlockHash = roundVote.Proposal.Block.Header.PrevBlockHash
			} else {
				log.Errorf("invalid proposal msg in construct roundvote (%d)", blknum)
			}
		case VotePrepare:
			roundVote.Prepare, ok = m.(*PrepareMsg)
			if !ok {
				log.Errorf("invalid prepare msg in construct roundvote (%d)", blknum)
			}
		case VoteCommit:
			roundVote.Commit, ok = m.(*CommitMsg)
			if !ok {
				log.Errorf("invalid commit msg in construct roundvote (%d)", blknum)
			}
		case RoundMerkleResult:
			roundVote.Result, ok = m.(*ExecMerkleMsg)
			if !ok {
				log.Errorf("invalid merkle-result msg in construct roundvote (%d)", blknum)
			}
		}
	}

	// update blockhash if necessary
	if roundVote.PrevBlockHash == common.UINT256_EMPTY {
		if proposal := self.blockPool.getProposal(blknum, vw); proposal != nil {
			roundVote.BlockHash = proposal.Hash()
			roundVote.PrevBlockHash = proposal.Header.PrevBlockHash
		}
	}

	return roundVote, nil
}

func (self *Server) constructVoteMsg(roundVotes []*RoundVoteMsg) (*VoteMsg, error) {
	vote := &VoteMsg{
		PeerID: self.Index,
		Rounds: roundVotes,
	}
	sink := common.NewZeroCopySink(nil)
	if err := vote.Serialize(sink); err != nil {
		return nil, fmt.Errorf("failed to serialize votemsg: %s", err)
	}
	sig, err := signature.Sign(self.account, sink.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to sign votemsg: %s", err)
	}

	vote.Sig = sig
	return vote, nil
}
