package poa

import (
	"bytes"
	"fmt"
	"time"

	"github.com/DNAProject/DNA/common"
	"github.com/DNAProject/DNA/common/log"
	actor2 "github.com/ontio/ontology-eventbus/actor"
)

func (pool *BlockPool) processNewView(blknum, view uint32) {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	nodes := pool.candidateBlocks[blknum]
	if nodes == nil {
		// should not happended
		log.Errorf("pool failed to find candidate nodes when processNewView (%d,%d)", blknum, view)
		return
	}
	// check if view is higher than active-view
	for _, n := range nodes {
		if n.View >= view && n.NodeState != PENDING {
			return
		}
	}

	oldViewNode := pool.getBlockNodeLocked(blknum, view)
	if oldViewNode == nil {
		log.Errorf("pool failed to find blocknode when processNewView(%d,%d)", blknum, view)
		return
	}
	if len(oldViewNode.ChangeView) < int(pool.server.GetQuorum(blknum)) {
		return
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
	pool.lock.Lock()
	defer pool.lock.Unlock()

	// verify blockhash/prev-hash in proposal
	node := pool.getBlockNodeLocked(blknum, view)
	if node == nil || node.Proposal == nil {
		return fmt.Errorf("failed to get blockNode when processing proposal(%d,%d): %v", blknum, view, node)
	}
	if node.ParticipantConfig == nil {
		return fmt.Errorf("failed to process proposal (%d,%d), nil participant config", blknum, view)
	}

	prevHash := node.Proposal.Header.PrevBlockHash
	prevNode := pool.nodePool[prevHash]
	if prevNode == nil || prevNode.Proposal == nil {
		log.Infof("failed to get prevBlockNode when processing proposal(%d,%d): %s", blknum, view, prevHash.ToHexString())
		return nil
	}
	if prevNode.NodeState == PENDING || prevNode.NodeState > MAX_SUCCESS_STATE {
		log.Infof("stop processing proposal (%d,%d) with prev block state: %d", blknum, view, prevNode.NodeState)
		return nil
	}

	// verify block-timestamp
	prevBlockTS := prevNode.Proposal.Header.Timestamp
	currentBlockTS := node.Proposal.Header.Timestamp
	if currentBlockTS < prevBlockTS || currentBlockTS > uint32(time.Now().Add(time.Minute*10).Unix()) {
		return fmt.Errorf("invalid proposal (%d,%d), prevBlockTS:%d, currentBlockTS:%d", blknum, view, prevBlockTS, currentBlockTS)
	}

	// verify vrf
	proposerPK := pool.server.GetPeerPublicKey(node.ParticipantConfig.Leader)
	prevVRF, _, err := getBlockVrfValue(prevNode.Proposal)
	if err != nil {
		return fmt.Errorf("failed to get prevBlock VRF: (%d,%d), %s", blknum, view, err)
	}
	proposalVRF, proposalVRFProof, err := getBlockVrfValue(node.Proposal)
	if err != nil {
		return fmt.Errorf("failed to get proposal VRF: (%d,%d), %s", blknum, view, err)
	}
	if err := verifyVrf(proposerPK, blknum, view, prevVRF, proposalVRF, proposalVRFProof); err != nil {
		return fmt.Errorf("failed to verfiy proposal VRF: (%d,%d): %s", blknum, view, err)
	}

	// verify tx-list
	txs := node.Proposal.Transactions
	if len(txs) > 0 && pool.server.nonSystxs(txs, blknum) {
		// get all txs in non-sealed block
		pendingTxs, err := pool.getPendingTxLocked(node)
		if err != nil {
			return fmt.Errorf("failed to get pending tx: %s", err)
		}
		for _, tx := range txs {
			if pendingTxs[tx.Hash()] {
				return fmt.Errorf("dup tx in proposal (%d,%d)", blknum, view)
			}
		}
		pendingTxs = nil

		// verify txs in tx-pool
		height := pool.getChainedBlockNumber()
		start, end := pool.server.incrValidator.BlockRange()
		validHeight := height
		if height+1 == end {
			validHeight = start
		} else {
			pool.server.incrValidator.Clean()
		}
		go func() {
			if err := pool.server.poolActor.VerifyBlock(txs, validHeight); err != nil && err != actor2.ErrTimeout {
				log.Errorf("verify proposal blk (%d,%d) failed, txs: %d, err: %s", blknum, view, len(txs), err)
			} else if err == actor2.ErrTimeout {
				log.Errorf("verify proposal blk(%d,%d) timeout, txs: %d, err: %s", blknum, view, len(txs))
			}

			pool.processCommit(blknum, view)
		}()
	}

	// TODO:
	// proposal has COMMITMENT semantic
	// if proprepared for (height, view), process as commit
	// otherwise, process as prepare
	return pool.processCommit(blknum, view)
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
