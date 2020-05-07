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

	"github.com/DNAProject/DNA/common"
	"github.com/DNAProject/DNA/common/log"
	"github.com/DNAProject/DNA/core/ledger"
	"github.com/DNAProject/DNA/core/store"
	"github.com/DNAProject/DNA/core/store/overlaydb"
	"github.com/DNAProject/DNA/core/types"
	"github.com/DNAProject/DNA/events/message"
	"github.com/ontio/ontology-eventbus/actor"
)

type PendingBlock struct {
	block        *types.Block
	execResult   *store.ExecuteResult
	hasSubmitted bool
}
type ChainStore struct {
	db              *ledger.Ledger
	chainedBlockNum uint32
	pendingBlocks   map[uint32]*PendingBlock
	serverPid       *actor.PID
}

func OpenBlockStore(db *ledger.Ledger, serverPid *actor.PID) (*ChainStore, error) {
	chainstore := &ChainStore{
		db:              db,
		chainedBlockNum: db.GetCurrentBlockHeight(),
		pendingBlocks:   make(map[uint32]*PendingBlock),
		serverPid:       serverPid,
	}
	merkleRoot, err := db.GetStateMerkleRoot(chainstore.chainedBlockNum)
	if err != nil {
		log.Errorf("GetStateMerkleRoot blockNum:%d, error :%s", chainstore.chainedBlockNum, err)
		return nil, fmt.Errorf("GetStateMerkleRoot blockNum:%d, error :%s", chainstore.chainedBlockNum, err)
	}
	writeSet := overlaydb.NewMemDB(1, 1)
	block, err := chainstore.getBlock(chainstore.chainedBlockNum)
	if err != nil {
		return nil, err
	}
	log.Debugf("chainstore openblockstore pendingBlocks height:%d,", chainstore.chainedBlockNum)
	chainstore.pendingBlocks[chainstore.chainedBlockNum] = &PendingBlock{block: block, execResult: &store.ExecuteResult{WriteSet: writeSet, MerkleRoot: merkleRoot}, hasSubmitted: true}
	return chainstore, nil
}

func (self *ChainStore) close() {
	// TODO: any action on ledger actor??
}

func (self *ChainStore) GetChainedBlockNum() uint32 {
	return self.chainedBlockNum
}

func (self *ChainStore) getExecMerkleRoot(blkNum uint32) (common.Uint256, error) {
	if blk, present := self.pendingBlocks[blkNum]; blk != nil && present {
		return blk.execResult.MerkleRoot, nil
	}
	merkleRoot, err := self.db.GetStateMerkleRoot(blkNum)
	if err != nil {
		log.Infof("GetStateMerkleRoot blockNum:%d, error :%s", blkNum, err)
		return common.Uint256{}, fmt.Errorf("GetStateMerkleRoot blockNum:%d, error :%s", blkNum, err)
	} else {
		return merkleRoot, nil
	}

}

func (self *ChainStore) getExecWriteSet(blkNum uint32) *overlaydb.MemDB {
	if blk, present := self.pendingBlocks[blkNum]; blk != nil && present {
		return blk.execResult.WriteSet
	}
	return nil
}

func (self *ChainStore) ReloadFromLedger() {
	height := self.db.GetCurrentBlockHeight()
	if height > self.chainedBlockNum {
		// update chainstore height
		self.chainedBlockNum = height
		// remove persisted pending blocks
		newPending := make(map[uint32]*PendingBlock)
		for blkNum, blk := range self.pendingBlocks {
			if blkNum > height {
				newPending[blkNum] = blk
			}
		}
		log.Debug("chainstore ReloadFromLedger pendingBlocks")
		// update pending blocks
		self.pendingBlocks = newPending
	}
}

func (self *ChainStore) AddBlock(block *types.Block) error {
	if block == nil {
		return fmt.Errorf("try add nil block")
	}

	if block.Header.Height <= self.GetChainedBlockNum() {
		log.Warnf("chain store adding chained block(%d, %d)", block.Header.Height, self.GetChainedBlockNum())
		return nil
	}

	if block.Header == nil {
		panic("nil block header")
	}
	blkNum := self.GetChainedBlockNum() + 1
	err := self.submitBlock(blkNum - 1)
	if err != nil {
		log.Errorf("chainstore blkNum:%d, SubmitBlock: %s", blkNum-1, err)
	}
	execResult, err := self.db.ExecuteBlock(block)
	if err != nil {
		log.Errorf("chainstore AddBlock GetBlockExecResult: %s", err)
		return fmt.Errorf("chainstore AddBlock GetBlockExecResult: %s", err)
	}
	log.Debugf("chainstore addblock pendingBlocks height:%d,block height:%d", blkNum, block.Header.Height)
	self.pendingBlocks[blkNum] = &PendingBlock{block: block, execResult: &execResult, hasSubmitted: false}

	if self.serverPid != nil {
		self.serverPid.Tell(
			&message.BlockConsensusComplete{
				Block: block,
			})
	}
	self.chainedBlockNum = blkNum
	return nil
}

func (self *ChainStore) submitBlock(blkNum uint32) error {
	if blkNum == 0 {
		return nil
	}
	if submitBlk, present := self.pendingBlocks[blkNum]; submitBlk != nil && submitBlk.hasSubmitted == false && present {
		err := self.db.SubmitBlock(submitBlk.block, *submitBlk.execResult)
		if err != nil {
			return fmt.Errorf("ledger add submitBlk (%d, %d, %d) failed: %s", blkNum, self.GetChainedBlockNum(), self.db.GetCurrentBlockHeight(), err)
		}
		if _, present := self.pendingBlocks[blkNum-1]; present {
			log.Infof("chainstore submitBlock delete pendingBlocks height:%d", blkNum-1)
			delete(self.pendingBlocks, blkNum-1)
		}
		submitBlk.hasSubmitted = true
	}
	return nil
}

func (self *ChainStore) getBlock(blockNum uint32) (*types.Block, error) {
	if blk, present := self.pendingBlocks[blockNum]; present {
		return blk.block, nil
	}
	block, err := self.db.GetBlockByHeight(uint32(blockNum))
	if err != nil {
		return nil, err
	}
	return block, nil
	//prevMerkleRoot := common.Uint256{}
	//if blockNum > 1 {
	//	prevMerkleRoot, err = self.db.GetStateMerkleRoot(blockNum - 1)
	//	if err != nil {
	//		log.Errorf("GetStateMerkleRoot blockNum:%d, error :%s", blockNum, err)
	//		return nil, fmt.Errorf("GetStateMerkleRoot blockNum:%d, error :%s", blockNum, err)
	//	}
	//}
	//return initVbftBlock(block, prevMerkleRoot)
}