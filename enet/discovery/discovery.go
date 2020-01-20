
package discovery

import "github.com/DNAProject/DNA/enet/message/types"

type NetworkMember struct {
	Addr string
	Port uint16
	InternalAddr string
	InternalPort uint16
}

type CommService interface {
	Gossip (msg types.Message)
	SendToPeer (peer *NetworkMember)
	Ping(peer *NetworkMember)
	Accept() <-chan types.Message
	PresumeDead() <-chan types.Message
	CloseConn(peer *NetworkMember)
	Forward(msg types.Message)
}

type Discovery interface {
	Lookup(peerID uint64) *NetworkMember
	Self() NetworkMember
	Stop()

	GetMembership() []NetworkMember
	Connect(member NetworkMember)

	SyncWithNeighbors(peerNum int)
}
