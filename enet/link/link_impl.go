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

package link

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"time"

	comm "github.com/DNAProject/DNA/common"
	"github.com/DNAProject/DNA/common/log"
	"github.com/DNAProject/DNA/enet/common"
	"github.com/DNAProject/DNA/enet/message/types"
)

//link used to establish
type linkImpl struct {
	id        uint64
	addr      string                 // The address of the node
	conn      net.Conn               // Connect socket with the peer node
	port      uint16                 // The server port of the node
	time      time.Time              // The latest time the node activity
	recvChan  chan *types.MsgPayload //msgpayload channel
	reqRecord map[string]int64       //Map RequestId to Timestamp, using for rejecting duplicate request in specific time
}

func NewLink() Link {
	link := &linkImpl{
		reqRecord: make(map[string]int64, 0),
	}
	return link
}

//SetID set peer id to link
func (this *linkImpl) SetID(id uint64) {
	this.id = id
}

//GetID return if from peer
func (this *linkImpl) GetID() uint64 {
	return this.id
}

//If there is connection return true
func (this *linkImpl) Valid() bool {
	return this.conn != nil
}

//set message channel for link layer
func (this *linkImpl) SetChan(msgchan chan *types.MsgPayload) {
	this.recvChan = msgchan
}

//get address
func (this *linkImpl) GetAddr() string {
	return this.addr
}

//set address
func (this *linkImpl) SetAddr(addr string) {
	this.addr = addr
}

//set port number
func (this *linkImpl) SetPort(p uint16) {
	this.port = p
}

//get port number
func (this *linkImpl) GetPort() uint16 {
	return this.port
}

//get connection
func (this *linkImpl) GetConn() net.Conn {
	return this.conn
}

//set connection
func (this *linkImpl) SetConn(conn net.Conn) {
	this.conn = conn
}

//record latest message time
func (this *linkImpl) UpdateRXTime(t time.Time) {
	this.time = t
}

//GetRXTime return the latest message time
func (this *linkImpl) GetRXTime() time.Time {
	return this.time
}

func (this *linkImpl) Rx() {
	conn := this.conn
	if conn == nil {
		return
	}

	reader := bufio.NewReaderSize(conn, common.MAX_BUF_LEN)

	for {
		msg, payloadSize, err := types.ReadMessage(reader)
		if err != nil {
			log.Infof("[p2p]error read from %s :%s", this.GetAddr(), err.Error())
			break
		}

		t := time.Now()
		this.UpdateRXTime(t)

		if !this.needSendMsg(msg) {
			log.Debugf("skip handle msgType:%s from:%d", msg.CmdType(), this.id)
			continue
		}

		this.addReqRecord(msg)
		this.recvChan <- &types.MsgPayload{
			Id:          this.id,
			Addr:        this.addr,
			PayloadSize: payloadSize,
			Payload:     msg,
		}

	}

	this.disconnectNotify()
}

//disconnectNotify push disconnect msg to channel
func (this *linkImpl) disconnectNotify() {
	log.Debugf("[p2p]call disconnectNotify for %s", this.GetAddr())
	this.CloseConn()

	msg, _ := types.MakeEmptyMessage(common.DISCONNECT_TYPE)
	discMsg := &types.MsgPayload{
		Id:      this.id,
		Addr:    this.addr,
		Payload: msg,
	}
	this.recvChan <- discMsg
}

//close connection
func (this *linkImpl) CloseConn() {
	if this.conn != nil {
		this.conn.Close()
		this.conn = nil
	}
}

func (this *linkImpl) Send(msg types.Message) error {
	sink := comm.NewZeroCopySink(nil)
	types.WriteMessage(sink, msg)

	return this.SendRaw(sink.Bytes())
}

func (this *linkImpl) SendRaw(rawPacket []byte) error {
	conn := this.conn
	if conn == nil {
		return errors.New("[p2p]tx link invalid")
	}

	nByteCnt := len(rawPacket)
	log.Tracef("[p2p]TX buf length: %d\n", nByteCnt)

	nCount := nByteCnt / common.PER_SEND_LEN
	if nCount == 0 {
		nCount = 1
	}
	conn.SetWriteDeadline(time.Now().Add(time.Duration(nCount*common.WRITE_DEADLINE) * time.Second))
	_, err := conn.Write(rawPacket)
	if err != nil {
		log.Infof("[p2p]error sending messge to %s :%s", this.GetAddr(), err.Error())
		this.disconnectNotify()
		return err
	}

	return nil
}

//needSendMsg check whether the msg is needed to push to channel
func (this *linkImpl) needSendMsg(msg types.Message) bool {
	if msg.CmdType() != common.GET_DATA_TYPE {
		return true
	}
	var dataReq = msg.(*types.DataReq)
	reqID := fmt.Sprintf("%x%s", dataReq.DataType, dataReq.Hash.ToHexString())
	now := time.Now().Unix()

	if t, ok := this.reqRecord[reqID]; ok {
		if int(now-t) < common.REQ_INTERVAL {
			return false
		}
	}
	return true
}

//addReqRecord add request record by removing outdated request records
func (this *linkImpl) addReqRecord(msg types.Message) {
	if msg.CmdType() != common.GET_DATA_TYPE {
		return
	}
	now := time.Now().Unix()
	if len(this.reqRecord) >= common.MAX_REQ_RECORD_SIZE-1 {
		for id := range this.reqRecord {
			t := this.reqRecord[id]
			if int(now-t) > common.REQ_INTERVAL {
				delete(this.reqRecord, id)
			}
		}
	}
	var dataReq = msg.(*types.DataReq)
	reqID := fmt.Sprintf("%x%s", dataReq.DataType, dataReq.Hash.ToHexString())
	this.reqRecord[reqID] = now
}
