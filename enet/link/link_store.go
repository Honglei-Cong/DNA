
package link

import "sync"

type linkCreator interface {
	newLink(addr string, port uint64) (Link, error)
}

type linkStore struct {
	sync.RWMutex
	creator linkCreator
	id2Link map[uint64]Link
}

func newLinkStore(linkCreator linkCreator) *linkStore {
	return &linkStore{
		creator: linkCreator,
		id2Link: make(map[uint64]Link),
	}
}


