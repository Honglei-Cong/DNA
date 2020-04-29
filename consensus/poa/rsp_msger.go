package poa

import (
	"math"
	"sync"
)

type RoundRspMsg struct {
	msgs map[uint32][]SubMsg // view -> []msg
}

type ResponseMsgPool struct {
	lock       sync.RWMutex
	server     *Server
	historyLen uint32
	msgs       map[uint32]*RoundRspMsg // height -> RoundRsp
}

func newResponseMsgPool(server *Server, historyLen uint32) *ResponseMsgPool {
	return &ResponseMsgPool{
		server:     server,
		historyLen: historyLen,
		msgs:       make(map[uint32]*RoundRspMsg),
	}
}

func (pool *ResponseMsgPool) addMsg(blknum, view uint32, msg SubMsg) {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	if _, present := pool.msgs[blknum]; !present {
		pool.msgs[blknum] = &RoundRspMsg{
			msgs: make(map[uint32][]SubMsg),
		}
	}
	roundmsgs := pool.msgs[blknum]
	if _, present := roundmsgs.msgs[view]; !present {
		roundmsgs.msgs[view] = make([]SubMsg, 0)
	}

	roundmsgs.msgs[view] = append(roundmsgs.msgs[view], msg)
}

func (pool *ResponseMsgPool) removeMsgs(blknum, view uint32) {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	if _, present := pool.msgs[blknum]; !present {
		return
	}
	roundmsgs := pool.msgs[blknum]
	if _, present := roundmsgs.msgs[view]; !present {
		return
	}
	delete(roundmsgs.msgs, view)
}

func (pool *ResponseMsgPool) getRoundResponse(blknum uint32) *RoundRspMsg {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	if _, present := pool.msgs[blknum]; !present {
		return nil
	}
	return pool.msgs[blknum]
}

func (pool *ResponseMsgPool) getFirstResponseHeight() uint32 {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	height := uint32(math.MaxUint32)
	for h := range pool.msgs {
		if h < height {
			height = h
		}
	}

	return height
}

func (pool *ResponseMsgPool) popAllMsgs() map[uint32]*RoundRspMsg {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	msgs := pool.msgs
	pool.msgs = make(map[uint32]*RoundRspMsg)
	return msgs
}
