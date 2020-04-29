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
	"sync"
	"time"

	"github.com/DNAProject/DNA/common/log"
)

type TimerEventType int

const (
	EventPeerHeartbeat = iota
	EventTxPool
	EventViewTimeout
	EventMax
)

var (
	newviewTimeout       = 10 * time.Second
	peerHandshakeTimeout = 10 * time.Second
	txPooltimeout        = 1 * time.Second
)

type SendMsgEvent struct {
	ToPeer uint32 // peer index
	Msg    ConsensusMsg
}

type TimerEvent struct {
	evtType  TimerEventType
	blknum uint32
	View     uint32
	msg      ConsensusMsg
}

type perBlockTimer map[uint32]*time.Timer

type EventTimer struct {
	lock   sync.Mutex
	server *Server
	C      chan *TimerEvent
	//timerQueue TimerQueue

	// bft timers
	eventTimers map[TimerEventType]perBlockTimer

	// peer heartbeat tickers
	peerTickers map[uint32]*time.Timer
	// other timers
	normalTimers map[uint32]*time.Timer
}

func NewEventTimer(server *Server) *EventTimer {
	timer := &EventTimer{
		server:       server,
		C:            make(chan *TimerEvent, 64),
		eventTimers:  make(map[TimerEventType]perBlockTimer),
		peerTickers:  make(map[uint32]*time.Timer),
		normalTimers: make(map[uint32]*time.Timer),
	}

	for i := 0; i < int(EventMax); i++ {
		timer.eventTimers[TimerEventType(i)] = make(map[uint32]*time.Timer)
	}

	return timer
}

func stopAllTimers(timers map[uint32]*time.Timer) {
	for _, t := range timers {
		t.Stop()
	}
}

func (self *EventTimer) stop() {
	self.lock.Lock()
	defer self.lock.Unlock()

	// clear timers by event timer
	for i := 0; i < int(EventMax); i++ {
		stopAllTimers(self.eventTimers[TimerEventType(i)])
		self.eventTimers[TimerEventType(i)] = make(map[uint32]*time.Timer)
	}

	// clear normal timers
	stopAllTimers(self.normalTimers)
	self.normalTimers = make(map[uint32]*time.Timer)
}

func (self *EventTimer) StartTimer(Idx uint32, timeout time.Duration) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if t, present := self.normalTimers[Idx]; present {
		t.Stop()
		log.Infof("timer for %d got reset", Idx)
	}

	self.normalTimers[Idx] = time.AfterFunc(timeout, func() {
		// remove timer from map
		self.lock.Lock()
		defer self.lock.Unlock()
		delete(self.normalTimers, Idx)

		self.C <- &TimerEvent{
			evtType:  EventMax,
			blknum: Idx,
		}
	})
}

func (self *EventTimer) CancelTimer(idx uint32) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if t, present := self.normalTimers[idx]; present {
		t.Stop()
		delete(self.normalTimers, idx)
	}
}

func (self *EventTimer) getEventTimeout(evtType TimerEventType, view uint32) time.Duration {
	switch evtType {
	case EventPeerHeartbeat:
		return peerHandshakeTimeout
	case EventTxPool:
		return txPooltimeout
	case EventViewTimeout:
		return time.Duration(math.Pow(2, float64(view+1))-1) * newviewTimeout
	}

	return 0
}

//
// internal helper, should call with lock held
//
func (self *EventTimer) startEventTimer(evtType TimerEventType, blknum uint32, view uint32) error {
	timers := self.eventTimers[evtType]
	if t, present := timers[blknum]; present {
		t.Stop()
		delete(timers, blknum)
		log.Infof("timer (type: %d) for %d got reset", evtType, blknum)
	}

	timeout := self.getEventTimeout(evtType, view)
	if timeout == 0 {
		log.Errorf("invalid timeout for event %d, blkNum %d", evtType, blknum)
		return fmt.Errorf("invalid timeout for event %d, blkNum %d", evtType, blknum)
	}
	timers[blknum] = time.AfterFunc(timeout, func() {
		self.C <- &TimerEvent{
			evtType:  evtType,
			blknum: blknum,
		}
	})
	return nil
}

//
// internal helper, should call with lock held
//
func (self *EventTimer) cancelEventTimer(evtType TimerEventType, blknum uint32) {
	timers := self.eventTimers[evtType]

	if t, present := timers[blknum]; present {
		t.Stop()
		delete(timers, blknum)
	}
}

func (self *EventTimer) onBlockSealed(blknum uint32) {
	self.lock.Lock()
	defer self.lock.Unlock()

	// clear event timers
	for i := 0; i < int(EventMax); i++ {
		self.cancelEventTimer(TimerEventType(i), blknum)
	}
}

func (self *EventTimer) startPeerTicker(peerIdx uint32) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	if p, present := self.peerTickers[peerIdx]; present {
		p.Stop()
		log.Infof("ticker for %d got reset", peerIdx)
	}

	timeout := self.getEventTimeout(EventPeerHeartbeat, 0)
	self.peerTickers[peerIdx] = time.AfterFunc(timeout, func() {
		self.C <- &TimerEvent{
			evtType:  EventPeerHeartbeat,
			blknum: peerIdx,
		}
		self.peerTickers[peerIdx].Reset(timeout)
	})

	return nil
}

func (self *EventTimer) stopPeerTicker(peerIdx uint32) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	if p, present := self.peerTickers[peerIdx]; present {
		p.Stop()
		delete(self.peerTickers, peerIdx)
	}
	return nil
}

func (self *EventTimer) startTxTicker(blknum uint32) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.startEventTimer(EventTxPool, blknum, 0)
}

func (self *EventTimer) stopTxTicker(blknum uint32) {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.cancelEventTimer(EventTxPool, blknum)
}

func (self *EventTimer) startViewTimeout(blknum, view uint32) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.startEventTimer(EventViewTimeout, blknum, view)
}

func (self *EventTimer) stopViewTimeout(blknum, view uint32) {
	self.lock.Lock()
	defer self.lock.Unlock()

	// FIXME: indexed by (blknum, view)
	self.cancelEventTimer(EventViewTimeout, blknum)
}
