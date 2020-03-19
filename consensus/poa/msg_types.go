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
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/DNAProject/DNA/common"
	vconfig "github.com/DNAProject/DNA/consensus/vbft/config"
	"github.com/DNAProject/DNA/core/types"
	"github.com/ontio/ontology-crypto/keypair"
	"github.com/ontio/ontology-crypto/signature"
)

type MsgType uint8

const (
	VoteMessage MsgType = iota
	ChangeViewMessage

	PeerHandshakeMessage
	PeerHeartbeatMessage

	BlockInfoFetchMessage
	BlockInfoFetchRespMessage
	ProposalFetchMessage
	ProposalFetchRespMessage
	BlockFetchMessage
	BlockFetchRespMessage
	BlockSubmitMessage
)

type SubMsgType uint8

const (
	VoteNewView SubMsgType = iota
	Proposal
	VotePrepare
	VoteCommit
	RoundMerkleResult
)

type ConsensusMsg interface {
	Type() MsgType
	GetHeight() uint32
	Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error
	Serialize(sink *common.ZeroCopySink) error
	Deserialize(source *common.ZeroCopySource) error
}

type SubMsg interface {
	Type() SubMsgType
	Verify(round *RoundVoteMsg, nodeID uint32, peers map[uint32]keypair.PublicKey) error
}

type Justify struct {
	Sigs map[uint32][]byte `json:"sigs"`
}

func (just *Justify) Verify(data []byte, peers map[uint32]keypair.PublicKey) error {
	for nodeID, sigdata := range just.Sigs {
		pk, _ := peers[nodeID]
		if pk == nil {
			return fmt.Errorf("invalid NodeID: %d", nodeID)
		}
		sig, err := signature.Deserialize(sigdata)
		if err != nil {
			return fmt.Errorf("invalid sig of nodeID %d: %s", nodeID, err)
		}
		if !signature.Verify(pk, data, sig) {
			return fmt.Errorf("invalid sid in justify from node %d", nodeID)
		}
	}

	return nil
}

func verifyPrepareJustify(height, view uint32, peers map[uint32]keypair.PublicKey, justifies *Justify) error {
	justData, err := json.Marshal(&ProposalJustifyData{
		Height:      height,
		View:        view,
		JustifyType: VotePrepare,
	})
	if err != nil {
		return fmt.Errorf("verify preparejust, failed to build just data: %s", err)
	}
	if err := justifies.Verify(justData, peers); err != nil {
		return fmt.Errorf("verify preparejust: %s", err)
	}

	return nil
}

type ProposalJustifyData struct {
	Height      uint32     `json:"height"`
	View        uint32     `json:"view"`
	JustifyType SubMsgType `json:"justify_type"`
}

type NewViewMsg struct {
	NewViewJustify *Justify `json:"new_view_justify"`
}

func (msg *NewViewMsg) Type() SubMsgType {
	return VoteNewView
}

func (msg *NewViewMsg) Verify(round *RoundVoteMsg, nodeID uint32, peers map[uint32]keypair.PublicKey) error {
	justData, err := json.Marshal(&ProposalJustifyData{
		Height:      round.Height,
		View:        round.View,
		JustifyType: VoteNewView,
	})
	if err != nil {
		return fmt.Errorf("verify newview-just, failed to build just data: %s", err)
	}
	if err := msg.NewViewJustify.Verify(justData, peers); err != nil {
		return fmt.Errorf("verify newview-just: %s", err)
	}

	return nil
}

type ProposalMsg struct {
	ProposeJustify *Justify     `json:"justify"`
	Block          *types.Block `json:"block"`
}

func (msg *ProposalMsg) UnmarshalJSON(data []byte) error {
	src := common.NewZeroCopySource(data)
	justData, _, irg, eof := src.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	just := &Justify{}
	if err := json.Unmarshal(justData, just); err != nil {
		return fmt.Errorf("unmarshal proposal just: %s", err)
	}

	blkData, _, irg, eof := src.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	blk := &types.Block{}
	if err := blk.Deserialization(common.NewZeroCopySource(blkData)); err != nil {
		return err
	}

	msg.ProposeJustify = just
	msg.Block = blk
	return nil
}

func (msg *ProposalMsg) MarshalJSON() ([]byte, error) {
	justData, err := json.Marshal(msg.ProposeJustify)
	if err != nil {
		return nil, fmt.Errorf("marshal just: %s", err)
	}
	blkSink := common.NewZeroCopySink(nil)
	msg.Block.Serialization(blkSink)

	sink := common.NewZeroCopySink(nil)
	sink.WriteVarBytes(justData)
	sink.WriteVarBytes(blkSink.Bytes())

	return sink.Bytes(), nil
}

func (msg *ProposalMsg) Type() SubMsgType {
	return Proposal
}

func (msg *ProposalMsg) Verify(roundMsg *RoundVoteMsg, nodeID uint32, peers map[uint32]keypair.PublicKey) error {
	if msg.Block == nil || len(msg.Block.Header.SigData) == 0 {
		return fmt.Errorf("no block or no sigdata in block from %d", nodeID)
	}
	pk, _ := peers[nodeID]
	if pk == nil {
		return fmt.Errorf("verify proposal, invalid nodeID: %d", nodeID)
	}
	hash := msg.Block.Hash()
	sig, err := signature.Deserialize(msg.Block.Header.SigData[0])
	if err != nil {
		return fmt.Errorf("deserialize block sig from %d: %s", nodeID, err)
	}
	if !signature.Verify(pk, hash[:], sig) {
		return fmt.Errorf("failed to verify proposal from %d", nodeID)
	}

	return verifyPrepareJustify(roundMsg.Height, roundMsg.View, peers, msg.ProposeJustify)
}

type PrepareMsg struct {
	PrepareJustify *Justify `json:"prepare_justify"`
	Sig            []byte   `json:"sig"`
}

func (msg *PrepareMsg) Type() SubMsgType {
	return VotePrepare
}

func (msg *PrepareMsg) Verify(round *RoundVoteMsg, nodeID uint32, peers map[uint32]keypair.PublicKey) error {
	if _, present := msg.PrepareJustify.Sigs[nodeID]; !present {
		msg.PrepareJustify.Sigs[nodeID] = msg.Sig
	} else {
		if bytes.Compare(msg.PrepareJustify.Sigs[nodeID], msg.Sig) != 0 {
			return fmt.Errorf("verify prepare, different sig from %d", nodeID)
		}
	}

	return verifyPrepareJustify(round.Height, round.View, peers, msg.PrepareJustify)
}

type CommitMsg struct {
	PreparedJustify *Justify `json:"prepared_justify"`
	Sig             []byte   `json:"sig"`
}

func (msg *CommitMsg) Type() SubMsgType {
	return VoteCommit
}

func (msg *CommitMsg) Verify(round *RoundVoteMsg, nodeID uint32, peers map[uint32]keypair.PublicKey) error {
	pk, _ := peers[nodeID]
	if pk == nil {
		return fmt.Errorf("invalid NodeID: %d", nodeID)
	}

	// verify commit justify
	if err := verifyPrepareJustify(round.Height, round.View, peers, msg.PreparedJustify); err != nil {
		return fmt.Errorf("verify commit: %s", err)
	}
	// verify commit sig
	sig, err := signature.Deserialize(msg.Sig)
	if err != nil {
		return fmt.Errorf("invalid sig of nodeID %d: %s", nodeID, err)
	}
	if !signature.Verify(pk, round.BlockHash[:], sig) {
		return fmt.Errorf("invalid sig in commit from node %d", nodeID)
	}
	return nil
}

type ExecMerkleMsg struct {
	// TODO: need commit justify?
	ExecMerkleRoot common.Uint256 `json:"exec_merkle_root"`
	Sig            []byte         `json:"sig"`
}

func (msg *ExecMerkleMsg) Type() SubMsgType {
	return RoundMerkleResult
}

func (msg *ExecMerkleMsg) Verify(round *RoundVoteMsg, nodeID uint32, peers map[uint32]keypair.PublicKey) error {
	pk, _ := peers[nodeID]
	if pk == nil {
		return fmt.Errorf("verify ExecMerkle, invalid nodeID: %d", nodeID)
	}
	sig, err := signature.Deserialize(msg.Sig)
	if err != nil {
		return fmt.Errorf("deserialize ExecMerkleMsg sig from %d: %s", nodeID, err)
	}
	if !signature.Verify(pk, msg.ExecMerkleRoot[:], sig) {
		return fmt.Errorf("failed to verify ExecMerkle from %d", nodeID)
	}

	return nil
}

type RoundVoteMsg struct {
	Epoch         uint32         `json:"epoch"`
	Height        uint32         `json:"height"`
	View          uint32         `json:"view"`
	PrevBlockHash common.Uint256 `json:"prev_block_hash"`
	BlockHash     common.Uint256 `json:"block_hash"`
	ProposerID    uint32         `json:"proposer_id"`
	NewView       *NewViewMsg    `json:"new_view_justify"`
	Proposal      *ProposalMsg   `json:"proposal"`
	Prepare       *PrepareMsg    `json:"prepare"`
	Commit        *CommitMsg     `json:"commit"`
	Result        *ExecMerkleMsg `json:"result"`
}

func (msg *RoundVoteMsg) Verify(nodeID uint32, peers map[uint32]keypair.PublicKey) error {
	// TODO: verify newview
	if msg.PrevBlockHash == msg.BlockHash {
		return errors.New("invalid prev block hash")
	}
	// verify proposal
	if msg.Proposal != nil {
		if msg.Proposal.Block.Header.Hash() != msg.BlockHash {
			return errors.New("unmatched proposal block hash")
		}
		if msg.Proposal.Block.Header.PrevBlockHash != msg.PrevBlockHash {
			return errors.New("unmatched proposal prev block hash")
		}
		if err := msg.Proposal.Verify(msg, nodeID, peers); err != nil {
			return err
		}
	}
	// verify prepare
	if msg.Prepare != nil {
		if err := msg.Prepare.Verify(msg, nodeID, peers); err != nil {
			return err
		}
	}
	// verify commit
	if msg.Commit != nil {
		if err := msg.Commit.Verify(msg, nodeID, peers); err != nil {
			return err
		}
	}
	// verify result
	if msg.Result != nil {
		if err := msg.Result.Verify(msg, nodeID, peers); err != nil {
			return err
		}
	}

	return nil
}

func (msg *RoundVoteMsg) GetHeight() uint32 {
	return msg.Height
}

type VoteMsg struct {
	PeerID uint32          `json:"peer_id"`
	Rounds []*RoundVoteMsg `json:"rounds"`
	Sig    []byte          `json:"sig"`
}

func (msg *VoteMsg) Type() MsgType {
	return VoteMessage
}

func (msg *VoteMsg) GetStartHeight() uint32 {
	if len(msg.Rounds) > 0 {
		return msg.Rounds[0].Height
	}
	return 0
}

func (msg *VoteMsg) GetHeight() uint32 {
	var maxHeight uint32
	if len(msg.Rounds) > 0 {
		maxHeight = msg.Rounds[0].Height
		for _, m := range msg.Rounds {
			if m.Height > maxHeight {
				maxHeight = m.Height
			}
		}
	}
	return maxHeight
}

func (msg *VoteMsg) Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error {
	if len(msg.Rounds) == 0 {
		return fmt.Errorf("vote msg without roundvote from %d", peerID)
	}
	if len(msg.Rounds) > CAP_ROUNDS_IN_VOTE {
		return fmt.Errorf("vote msg with roundvotes %d", len(msg.Rounds))
	}
	// validate all rounds in msg
	for _, r := range msg.Rounds {
		if r == nil {
			return fmt.Errorf("vote msg with nil roundvote from %d", peerID)
		}
	}
	// validate height order of round votes
	h := msg.Rounds[0].Height
	for _, r := range msg.Rounds[1:] {
		if r.Height <= h {
			return fmt.Errorf("invalid round msg seq %d vs %d", r.Height, h)
		}
		h = r.Height

		// verify round msg
		if err := r.Verify(peerID, peers); err != nil {
			return fmt.Errorf("invalid vote msg (round %d): %s", r.Height, err)
		}
	}
	return nil
}

func (msg *VoteMsg) Serialize(sink *common.ZeroCopySink) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to serialize vote msg: %s", err)
	}
	sink.WriteVarBytes(data)
	return nil
}

func (msg *VoteMsg) Deserialize(source *common.ZeroCopySource) error {
	data, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	return json.Unmarshal(data, msg)
}

type ChangeViewMsg struct {
	NodeID     uint32     `json:"node_id"`
	Height     uint32     `json:"height"`
	NewView    uint32     `json:"new_view"`
	LastHeight uint32     `json:"last_height"`
	LastView   uint32     `json:"last_view"`
	LastCommit *CommitMsg `json:"last_commit"`
	Sig        []byte     `json:"sig"`
}

func (msg *ChangeViewMsg) Type() MsgType {
	return ChangeViewMessage
}

func (msg *ChangeViewMsg) GetHeight() uint32 {
	return msg.Height
}

func (msg *ChangeViewMsg) Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error {
	// TODO:
	return nil
}

func (msg *ChangeViewMsg) Serialize(sink *common.ZeroCopySink) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to serialize vote msg: %s", err)
	}
	sink.WriteVarBytes(data)
	return nil
}

func (msg *ChangeViewMsg) Deserialize(source *common.ZeroCopySource) error {
	data, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	return json.Unmarshal(data, msg)
}

type peerHandshakeMsg struct {
	CommittedBlockNumber uint32               `json:"committed_block_number"`
	CommittedBlockHash   common.Uint256       `json:"committed_block_hash"`
	ChainConfig          *vconfig.ChainConfig `json:"chain_config"`
}

func (msg *peerHandshakeMsg) Type() MsgType {
	return PeerHandshakeMessage
}

func (msg *peerHandshakeMsg) GetHeight() uint32 {
	return 0
}

func (msg *peerHandshakeMsg) Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error {
	// TODO
	return nil
}

func (msg *peerHandshakeMsg) Serialize(sink *common.ZeroCopySink) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	sink.WriteVarBytes(data)
	return nil
}

func (msg *peerHandshakeMsg) Deserialize(source *common.ZeroCopySource) error {
	data, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	return json.Unmarshal(data, msg)
}

type peerHeartbeatMsg struct {
	CommittedBlockNumber uint32         `json:"committed_block_number"`
	CommittedBlockHash   common.Uint256 `json:"committed_block_hash"`
	ChainConfigView      uint32         `json:"chain_config_view"`
}

func (msg *peerHeartbeatMsg) Type() MsgType {
	return PeerHeartbeatMessage
}

func (msg *peerHeartbeatMsg) GetHeight() uint32 {
	return 0
}

func (msg *peerHeartbeatMsg) Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error {
	// TODO
	return nil
}

func (msg *peerHeartbeatMsg) Serialize(sink *common.ZeroCopySink) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	sink.WriteVarBytes(data)
	return nil
}

func (msg *peerHeartbeatMsg) Deserialize(source *common.ZeroCopySource) error {
	data, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	return json.Unmarshal(data, msg)
}

type BlockInfoFetchMsg struct {
	StartBlockNum uint32 `json:"start_block_num"`
}

func (msg *BlockInfoFetchMsg) Type() MsgType {
	return BlockInfoFetchMessage
}

func (msg *BlockInfoFetchMsg) GetHeight() uint32 {
	return 0
}

func (msg *BlockInfoFetchMsg) Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error {
	// TODO
	return nil
}

func (msg *BlockInfoFetchMsg) Serialize(sink *common.ZeroCopySink) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	sink.WriteVarBytes(data)
	return nil
}

func (msg *BlockInfoFetchMsg) Deserialize(source *common.ZeroCopySource) error {
	data, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	return json.Unmarshal(data, msg)
}

type BlockInfo_ struct {
	BlockNum   uint32            `json:"block_num"`
	Proposer   uint32            `json:"proposer"`
	Signatures map[uint32][]byte `json:"signatures"`
}

// to fetch committed block from neighbours
type BlockInfoFetchRespMsg struct {
	Blocks []*BlockInfo_ `json:"blocks"`
}

func (msg *BlockInfoFetchRespMsg) Type() MsgType {
	return BlockInfoFetchRespMessage
}

func (msg *BlockInfoFetchRespMsg) GetHeight() uint32 {
	return 0
}

func (msg *BlockInfoFetchRespMsg) Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error {
	// TODO
	return nil
}

func (msg *BlockInfoFetchRespMsg) Serialize(sink *common.ZeroCopySink) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	sink.WriteVarBytes(data)
	return nil
}

func (msg *BlockInfoFetchRespMsg) Deserialize(source *common.ZeroCopySource) error {
	data, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	return json.Unmarshal(data, msg)
}

// block fetch msg is to fetch block which could have not been committed or endorsed
type blockFetchMsg struct {
	BlockNum uint32 `json:"block_num"`
}

func (msg *blockFetchMsg) Type() MsgType {
	return BlockFetchMessage
}

func (msg *blockFetchMsg) GetHeight() uint32 {
	return 0
}

func (msg *blockFetchMsg) Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error {
	// TODO
	return nil
}

func (msg *blockFetchMsg) Serialize(sink *common.ZeroCopySink) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	sink.WriteVarBytes(data)
	return nil
}

func (msg *blockFetchMsg) Deserialize(source *common.ZeroCopySource) error {
	data, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	return json.Unmarshal(data, msg)
}

type BlockFetchRespMsg struct {
	BlockNumber uint32         `json:"block_number"`
	BlockHash   common.Uint256 `json:"block_hash"`
	BlockData   *types.Block   `json:"block_data"`
}

func (msg *BlockFetchRespMsg) Type() MsgType {
	return BlockFetchRespMessage
}

func (msg *BlockFetchRespMsg) GetHeight() uint32 {
	return 0
}

func (msg *BlockFetchRespMsg) Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error {
	// TODO
	return nil
}

func (msg *BlockFetchRespMsg) Serialize(sink *common.ZeroCopySink) error {
	sink.WriteUint32(msg.BlockNumber)
	sink.WriteVarBytes(msg.BlockHash[:])
	sink.WriteBool(msg.BlockData != nil)
	if msg.BlockData != nil {
		sink := common.NewZeroCopySink(nil)
		msg.BlockData.Serialization(sink)
		sink.WriteVarBytes(sink.Bytes())
	}
	return nil
}

func (msg *BlockFetchRespMsg) Deserialize(source *common.ZeroCopySource) error {
	var eof bool
	msg.BlockNumber, eof = source.NextUint32()
	if eof {
		return io.ErrUnexpectedEOF
	}
	blkHash, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}

	var err error
	msg.BlockHash, err = common.Uint256ParseFromBytes(blkHash)
	if err != nil {
		return err
	}

	hasBlk, irg, eof := source.NextBool()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	if !hasBlk {
		return nil
	}

	blkData, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	blk := &types.Block{}
	if err := blk.Deserialization(common.NewZeroCopySource(blkData)); err != nil {
		return fmt.Errorf("unmarshal block: %s", err)
	}
	msg.BlockData = blk
	return nil
}

// proposal fetch msg is to fetch proposal when peer failed to get proposal locally
type proposalFetchMsg struct {
	BlockNum uint32 `json:"block_num"`
	View     uint32 `json:"view"`
	Sig      []byte `json:"sig"`
}

func (msg *proposalFetchMsg) Type() MsgType {
	return ProposalFetchMessage
}

func (msg *proposalFetchMsg) GetHeight() uint32 {
	return msg.BlockNum
}

func (msg *proposalFetchMsg) Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error {
	// TODO
	return nil
}

func (msg *proposalFetchMsg) Serialize(sink *common.ZeroCopySink) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	sink.WriteVarBytes(data)
	return nil
}

func (msg *proposalFetchMsg) Deserialize(source *common.ZeroCopySource) error {
	data, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	return json.Unmarshal(data, msg)
}

type proposalFetchRespMsg struct {
	BlockNum uint32       `json:"block_num"`
	View     uint32       `json:"view"`
	Proposal *ProposalMsg `json:"proposal"`
}

func (msg *proposalFetchRespMsg) Type() MsgType {
	return ProposalFetchRespMessage
}

func (msg *proposalFetchRespMsg) GetHeight() uint32 {
	return msg.BlockNum
}

func (msg *proposalFetchRespMsg) Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error {
	// TODO
	return nil
}

func (msg *proposalFetchRespMsg) Serialize(sink *common.ZeroCopySink) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	sink.WriteVarBytes(data)
	return nil
}

func (msg *proposalFetchRespMsg) Deserialize(source *common.ZeroCopySource) error {
	data, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	return json.Unmarshal(data, msg)
}

type blockSubmitMsg struct {
	BlockStateRoot common.Uint256 `json:"block_state_root"`
	BlockNum       uint32         `json:"block_num"`
	SubmitMsgSig   []byte         `json:"submit_msg_sig"`
}

func (msg *blockSubmitMsg) Type() MsgType {
	return BlockSubmitMessage
}

func (msg *blockSubmitMsg) GetHeight() uint32 {
	return msg.BlockNum
}

func (msg *blockSubmitMsg) Verify(peerID uint32, peers map[uint32]keypair.PublicKey) error {
	pk, _ := peers[peerID]
	if pk == nil {
		return fmt.Errorf("invalid blocksubmit msg from %d", peerID)
	}
	hash := msg.BlockStateRoot
	sig, err := signature.Deserialize(msg.SubmitMsgSig)
	if err != nil {
		return fmt.Errorf("deserialize submitmsg sig: %s", err)
	}
	if !signature.Verify(pk, hash[:], sig) {
		return fmt.Errorf("failed to verify submit sig")
	}
	return nil
}

func (msg *blockSubmitMsg) GetBlockNum() uint32 {
	return msg.BlockNum
}

func (msg *blockSubmitMsg) Serialize(sink *common.ZeroCopySink) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	sink.WriteVarBytes(data)
	return nil
}

func (msg *blockSubmitMsg) Deserialize(source *common.ZeroCopySource) error {
	data, _, irg, eof := source.NextVarBytes()
	if eof {
		return io.ErrUnexpectedEOF
	}
	if irg {
		return common.ErrIrregularData
	}
	return json.Unmarshal(data, msg)
}
