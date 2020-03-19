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
	"sync"

	"github.com/DNAProject/DNA/common"
)

var errDropFarFutureMsg = errors.New("msg pool dropped msg for far future")

type ConsensusRoundMsgs map[MsgType][]ConsensusMsg // indexed by MsgType (proposal, endorsement, ...)

type ConsensusRound struct {
	blockNum uint32
	msgHashs map[common.Uint256]interface{} // for msg-dup checking
}

func newConsensusRound(num uint32) *ConsensusRound {

	r := &ConsensusRound{
		blockNum: num,
		msgHashs: make(map[common.Uint256]interface{}),
	}

	return r
}

func (self *ConsensusRound) addMsg(msg ConsensusMsg, msgHash common.Uint256) {
	if _, present := self.msgHashs[msgHash]; present {
		return
	}

	self.msgHashs[msgHash] = msg
}

func (self *ConsensusRound) dropMsg(msg ConsensusMsg) {
	for hash, m := range self.msgHashs {
		if m == msg {
			delete(self.msgHashs, hash)
			return
		}
	}
}

func (self *ConsensusRound) hasMsg(msg ConsensusMsg, msgHash common.Uint256) bool {
	if _, present := self.msgHashs[msgHash]; present {
		return present
	}
	return false
}

type MsgPool struct {
	lock       sync.RWMutex
	server     *Server
	historyLen uint32
	rounds     map[uint32]*ConsensusRound // indexed by BlockNum
}

func newMsgPool(server *Server, historyLen uint32) *MsgPool {
	// TODO
	return &MsgPool{
		historyLen: historyLen,
		server:     server,
		rounds:     make(map[uint32]*ConsensusRound),
	}
}

func (pool *MsgPool) clean() {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	pool.rounds = make(map[uint32]*ConsensusRound)
}

func (pool *MsgPool) AddMsg(msg ConsensusMsg, msgHash common.Uint256) error {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	blkNum := msg.GetHeight()
	if blkNum > pool.server.GetCurrentBlockNo()+pool.historyLen {
		return errDropFarFutureMsg
	}

	if _, present := pool.rounds[blkNum]; !present {
		pool.rounds[blkNum] = newConsensusRound(blkNum)
	}

	// TODO: limit #history rounds to historyLen
	// Note: we accept msg for future rounds

	pool.rounds[blkNum].addMsg(msg, msgHash)
	return nil
}

func (pool *MsgPool) DropMsg(msg ConsensusMsg) {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	if roundMsgs, present := pool.rounds[msg.GetHeight()]; present {
		roundMsgs.dropMsg(msg)
	}
}

func (pool *MsgPool) HasMsg(msg ConsensusMsg, msgHash common.Uint256) bool {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	if roundMsgs, present := pool.rounds[msg.GetHeight()]; !present {
		return false
	} else {
		return roundMsgs.hasMsg(msg, msgHash)
	}

	return false
}

func (pool *MsgPool) onBlockSealed(blockNum uint32) {
	if blockNum <= pool.historyLen {
		return
	}
	pool.lock.Lock()
	defer pool.lock.Unlock()

	toFreeRound := make([]uint32, 0)
	for n := range pool.rounds {
		if n < blockNum-pool.historyLen {
			toFreeRound = append(toFreeRound, n)
		}
	}
	for _, n := range toFreeRound {
		delete(pool.rounds, n)
	}
}
