package poa

import (
	"bytes"
	"fmt"
	"github.com/DNAProject/DNA/common"
	"github.com/DNAProject/DNA/common/log"
)

func (pool *BlockPool) processNewView(blknum, view uint32) error {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	nodes := pool.candidateBlocks[blknum]
	if nodes == nil {
		// should not happended
		log.Errorf("pool failed to find candidate nodes when processNewView (%d,%d)", blknum, view)
		return nil
	}
	// check if view is higher than active-view
	for _, n := range nodes {
		if n.View >= view && n.NodeState != PENDING {
			return nil
		}
	}

	oldViewNode := pool.getBlockNodeLocked(blknum, view)
	if oldViewNode == nil {
		log.Errorf("pool failed to find blocknode when processNewView(%d,%d)", blknum, view)
		return nil
	}
	if len(oldViewNode.ChangeView) < int(pool.server.GetQuorum(blknum)) {
		return nil
	}

	// fail all previous view, activate new one
	for _, n := range nodes {
		if n.View <= view {
			if n.NodeState == LOCKED {
				n.NodeState = LOCK_FAILED
			} else {
				n.NodeState = FAILED
			}
		} else if n.View == view+1 && n.NodeState == PENDING {
			// active view+1
			n.NodeState = READY
		}
	}

	return nil
}

func (pool *BlockPool) processNewExecMerkle(blknum, view uint32) error {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	// find block node
	var node *BlockNode
	if nodes := pool.candidateBlocks[blknum]; nodes != nil {
		for _, n := range nodes {
			if n.View == view {
				node = n
				break
			}
		}
	}
	if node == nil {
		log.Infof("skip processing merkle (%d,%d), not found blocknode", blknum, view)
		return nil
	}

	// check quorum
	sealed := false
	var sealedMerkle common.Uint256
	q := pool.server.GetQuorum(blknum)
	results := make(map[common.Uint256]uint32)
	for _, r := range node.Result {
		results[r] = results[r] + 1
		if results[r] >= q {
			sealed = true
			sealedMerkle = r
		}
	}

	if !sealed {
		return nil
	}

	if bytes.Compare(node.ExecMerkleRoot[:], sealedMerkle[:]) != 0 {
		panic(fmt.Sprintf("seal block with unmatched merkle root %v vs %v", node.ExecMerkleRoot, sealedMerkle))
	}
	// block is ready to be sealed
	// get all parent blocks which have not been sealed
	nodes := make([]*BlockNode, 0)
	nodes = append(nodes, node) // all un-finalized parent blocks, in reverse order

	n := node
	for {
		n2 := pool.nodePool[n.PrevBlockHash]
		if n2.NodeState == FAILED || n2.NodeState == LOCK_FAILED {
			panic(fmt.Sprintf("finalizing child block of failed block (%d,%d)", blknum, view))
		}
		if n2 == nil || n2.NodeState == FINALIZED {
			break
		}
		nodes = append(nodes, n2)
		n = n2
	}
	// seal all blocks
	for i := len(nodes) - 1; i >= 0; i-- {
		if err := pool.finalizeBlockNodeLocked(nodes[i]); err != nil {
			return err
		}
		pool.server.makeFinalized(nodes[i].Height, nodes[i].Proposal, nodes[i].ExecMerkleRoot)
	}

	return nil
}

func (pool *BlockPool) processCommit(blknum, view uint32) error {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	// find block node
	var node *BlockNode
	if nodes := pool.candidateBlocks[blknum]; nodes != nil {
		for _, n := range nodes {
			if n.View == view {
				node = n
				break
			}
		}
	}
	if node == nil {
		log.Infof("skip processing commit (%d,%d), not found blocknode", blknum, view)
		return nil
	}

	// get all parent blocks which have not been sealed
	nodes := make([]*BlockNode, 0)
	nodes = append(nodes, node) // all un-sealed parent blocks, in reverse order
	for {
		n2 := pool.nodePool[node.PrevBlockHash]
		if n2.NodeState == FAILED || n2.NodeState == LOCK_FAILED {
			log.Errorf("received committing child block of failed block (%d,%d)", blknum, view)
			return nil
		}
		if n2 == nil || n2.NodeState == SEALED {
			break
		}
		nodes = append(nodes, n2)
		node = n2
	}

	// try sealing all blocks, loop in reverse
	for i := len(nodes) - 1; i >= 0; i-- {
		n := nodes[i]
		// check quorum
		// block is ready to be sealed
		if uint32(len(n.Commit)) < pool.server.GetQuorum(n.Height) {
			break
		}

		if err := pool.sealBlockNodeLocked(n); err != nil {
			return err
		}
		if err := pool.server.makeSealed(n.Height, n.View, n.Proposal); err != nil {
			log.Errorf("try sealing block (%d,%d): %s", n.Height, n.View, err)
		}
	}

	return nil
}

func (pool *BlockPool) processPrepare(blknum, view uint32) error {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	// find block node
	var node *BlockNode
	if nodes := pool.candidateBlocks[blknum]; nodes != nil {
		for _, n := range nodes {
			if n.View == view {
				node = n
				break
			}
		}
	}
	if node == nil {
		log.Infof("skip processing prepare (%d,%d), not found blocknode", blknum, view)
		return nil
	}

	// get all parent blocks which have not been locked
	nodes := make([]*BlockNode, 0)
	nodes = append(nodes, node) // all un-sealed parent blocks, in reverse order
	for {
		n2 := pool.nodePool[node.PrevBlockHash]
		if n2.NodeState == FAILED || n2.NodeState == LOCK_FAILED {
			log.Errorf("received preparing child block of failed block (%d,%d)", blknum, view)
			return nil
		}
		if n2 == nil || n2.NodeState == LOCKED {
			break
		}
		nodes = append(nodes, n2)
		node = n2
	}

	// try committing all blocks, loop in reverse
	for i := len(nodes) - 1; i >= 0; i-- {
		n := nodes[i]
		if pool.hasCommittedOnForksLocked(n.Height, n.View) {
			// if locked for other view
			return nil
		}
		// prepare for the node
		if err := pool.prepareForBlockNodeLocked(n); err != nil {
			log.Error(err)
		}
		if uint32(len(n.Prepare)) < pool.server.GetQuorum(n.Height) {
			continue
		}
		// safe prepared
		if err := pool.commitBlockNodeLocked(n); err == nil {
			pool.server.makeCommitment(n.Height, n.View)
		} else {
			log.Errorf("failed to commit node (%d,%d): %s", n.Height, n.View, err)
		}
	}
	return nil
}

func (pool *BlockPool) processProposal(blknum, view uint32) error {
	// TODO:
	// verify blockhash/prev-hash in proposal
	// verify chainconfig-blocknum
	// verify block-merkle-root
	// verify block-timestamp
	// verify vrf
	// verify tx-list
	// take proposal as one prepare

	return pool.processPrepare(blknum, view)
}

func (pool *BlockPool) prepareForBlockNodeLocked(n *BlockNode) error {
	if n.hasPreparedBy(pool.server.Index) {
		return nil
	}
	if n.isProactor(pool.server.Index) {
		// proactor, make prepare anyway
		if err := pool.prepareBlockNodeLocked(n); err == nil {
			pool.server.makePrepare(n.Height, n.View)
		} else {
			return fmt.Errorf("proactor failed to prepare node (%d,%d): %s", n.Height, n.View, err)
		}
	} else {
		// check quorum of proactors
		if pool.isProPreparedLocked(n.Height, n.View) {
			// make prepare if proactor quorum-prepared
			if err := pool.prepareBlockNodeLocked(n); err == nil {
				pool.server.makePrepare(n.Height, n.View)
			} else {
				return fmt.Errorf("failed to prepare node (%d,%d): %s", n.Height, n.View, err)
			}
		}
	}

	return nil
}
