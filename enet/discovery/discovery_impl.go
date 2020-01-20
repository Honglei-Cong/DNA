
package discovery

import "github.com/DNAProject/DNA/enet/message/types"

type discoveryImpl struct {
	self NetworkMember
	comm CommService
	exitChan chan bool

}

func NewDiscoveryService(self NetworkMember, comm CommService) Discovery {
	d := &discoveryImpl{
		self: self,
		comm: comm,
	}

	go d.periodicalSendAlive()
	go d.periodicalCheckAlive()
	go d.handleMsgs()
	go d.periodicalReconnectToDead()
	go d.handlePresumedDeadPeers()

	return d
}

func (d *discoveryImpl) Self() NetworkMember {
	return d.self
}

func (d *discoveryImpl) Lookup(peerID uint64) *NetworkMember {
	return nil
}

func (d *discoveryImpl) Connect(member NetworkMember) {

}

func (d *discoveryImpl) SyncWithNeighbors(peerNum int) {

}

func (d *discoveryImpl) GetMembership() []NetworkMember {
	return nil
}

func (d *discoveryImpl) Stop() {
}

func (d *discoveryImpl) handleMsgs() {
	in := d.comm.Accept()

	for {
		select {
		case <-d.exitChan:
			return
			case m := <-in:
				d.handleMsgFromComm(m)
		}
	}
}

func (d *discoveryImpl) handleMsgFromComm(msg types.Message) {

}

func (d *discoveryImpl) periodicalSendAlive() {

}

func (d *discoveryImpl) periodicalCheckAlive() {

}

func (d *discoveryImpl) periodicalReconnectToDead() {

}

func (d *discoveryImpl) handlePresumedDeadPeers() {

}
