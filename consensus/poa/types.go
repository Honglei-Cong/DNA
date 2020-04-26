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
	"io"

	"github.com/DNAProject/DNA/common"
	"github.com/DNAProject/DNA/consensus/vbft/config"
	"github.com/DNAProject/DNA/core/types"
)

type Block struct {
	Block               *types.Block
	Info                *vconfig.VbftBlockInfo
	PrevBlockMerkleRoot common.Uint256
}

func (blk *Block) getProposer() uint32 {
	return blk.Info.Proposer
}

func (blk *Block) getBlockNum() uint32 {
	return blk.Block.Header.Height
}

func (blk *Block) getPrevBlockHash() common.Uint256 {
	return blk.Block.Header.PrevBlockHash
}

func (blk *Block) getLastConfigBlockNum() uint32 {
	return blk.Info.LastConfigBlockNum
}

func (blk *Block) getNewChainConfig() *vconfig.ChainConfig {
	return blk.Info.NewChainConfig
}

func (blk *Block) getPrevBlockMerkleRoot() common.Uint256 {
	return blk.PrevBlockMerkleRoot
}

//
// getVrfValue() is a helper function for participant selection.
//
func (blk *Block) getVrfValue() []byte {
	return blk.Info.VrfValue
}

func (blk *Block) getVrfProof() []byte {
	return blk.Info.VrfProof
}

func (blk *Block) Serialize() []byte {
	payload := common.NewZeroCopySink(nil)
	payload.WriteVarBytes(common.SerializeToBytes(blk.Block))

	payload.WriteBool(blk.EmptyBlock != nil)
	if blk.EmptyBlock != nil {
		payload.WriteVarBytes(common.SerializeToBytes(blk.EmptyBlock))
	}
	payload.WriteHash(blk.PrevBlockMerkleRoot)
	return payload.Bytes()
}

func (blk *Block) Deserialize(data []byte) error {
	source := common.NewZeroCopySource(data)
	//buf := bytes.NewBuffer(data)
	buf1, _, irregular, eof := source.NextVarBytes()
	if irregular {
		return common.ErrIrregularData
	}
	if eof {
		return io.ErrUnexpectedEOF
	}

	block, err := types.BlockFromRawBytes(buf1)
	if err != nil {
		return fmt.Errorf("deserialize block: %s", err)
	}

	info := &vconfig.VbftBlockInfo{}
	if err := json.Unmarshal(block.Header.ConsensusPayload, info); err != nil {
		return fmt.Errorf("unmarshal vbft info: %s", err)
	}

	var emptyBlock *types.Block
	hasEmptyBlock, irr, eof := source.NextBool()
	if irr {
		return fmt.Errorf("read empty-block-bool: %s", common.ErrIrregularData)
	}
	if eof {
		return fmt.Errorf("read empty-block-bool: %s", io.ErrUnexpectedEOF)
	}
	if hasEmptyBlock {
		buf2, _, irregular, eof := source.NextVarBytes()
		if irregular || eof {
			return fmt.Errorf("read empty block failed: %v, %v", irregular, eof)
		}
		block2, err := types.BlockFromRawBytes(buf2)
		if err != nil {
			return fmt.Errorf("deserialize empty blk failed: %s", err)
		}
		emptyBlock = block2
	}

	var merkleRoot common.Uint256
	merkleRoot, eof = source.NextHash()
	if eof {
		return fmt.Errorf("block deserialize merkleRoot: %s", io.ErrUnexpectedEOF)
	}
	blk.Block = block
	blk.EmptyBlock = emptyBlock
	blk.Info = info
	blk.PrevBlockMerkleRoot = merkleRoot
	return nil
}

func getVbftInfo(block *types.Block) *vconfig.VbftBlockInfo {
	if block == nil {
		return nil
	}

	vbftInfo := &vconfig.VbftBlockInfo{}
	if err := json.Unmarshal(block.Header.ConsensusPayload, vbftInfo); err != nil {
		return nil
	}

	return vbftInfo
}
