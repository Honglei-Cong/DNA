package poa

import "sync"

type RoundRspMsg struct {
	msgs map[uint32][]SubMsg
}

type ResponseMsgPool struct {
	lock       sync.RWMutex
	server     *Server
	historyLen uint32
	msgs       map[uint32]*RoundRspMsg
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

func (pool *ResponseMsgPool) getMgs(blknum, view uint32) []SubMsg {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	if _, present := pool.msgs[blknum]; !present {
		return nil
	}
	roundmsgs := pool.msgs[blknum]
	return roundmsgs.msgs[view]
}
