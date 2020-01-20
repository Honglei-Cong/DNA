package link

import (
	"net"
	"time"

	"github.com/DNAProject/DNA/enet/message/types"
)

type Link interface {
	GetID() uint64
	SetID(id uint64)
	GetPort() uint16
	SetPort(p uint16)
	SetChan(msgchan chan *types.MsgPayload)
	GetAddr() string
	SetAddr(addr string)
	UpdateRXTime(t time.Time)
	GetRXTime() time.Time
	Valid() bool
	GetConn() net.Conn
	SetConn(conn net.Conn)

	Rx()
	Send(msg types.Message) error
	SendRaw(pkt []byte) error
}
