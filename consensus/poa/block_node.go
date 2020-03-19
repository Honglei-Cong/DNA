package poa

import (
	"fmt"
	"github.com/DNAProject/DNA/common"
	"github.com/DNAProject/DNA/core/types"
)

type NodeState uint32

const (
	PENDING NodeState = iota
	READY
	LOCKED
	SEALED
	FINALIZED
	MAX_SUCCESS_STATE
	FAILED
	LOCK_FAILED
)

type Vote struct {
	Height uint32
	Msg    *RoundVoteMsg
}

type BlockNode struct {
	Epoch         uint32
	Height        uint32
	View          uint32
	BlockHash     common.Uint256
	PrevBlockHash common.Uint256
	NodeState     NodeState

	ParticipantConfig *BlockParticipantConfig
	Proposal          *types.Block
	Txs               map[common.Uint256]struct{}
	ExecMerkleRoot    common.Uint256

	// justifies
	ChangeView map[uint32]*Vote // indexed by from-nodeID
	Prepare    map[uint32]*Vote
	Commit     map[uint32]*Vote
	Result     map[uint32]common.Uint256
}

func newPoolNode(epoch, height, view uint32, prevHash, blockhash common.Uint256) *BlockNode {
	return &BlockNode{
		Epoch:         epoch,
		Height:        height,
		View:          view,
		BlockHash:     blockhash,
		PrevBlockHash: prevHash,
		NodeState:     PENDING,
		Txs:           make(map[common.Uint256]struct{}),
		ChangeView:    make(map[uint32]*Vote),
		Prepare:       make(map[uint32]*Vote),
		Commit:        make(map[uint32]*Vote),
		Result:        make(map[uint32]common.Uint256),
	}
}

func (node *BlockNode) addChangeView(nodeFrom uint32, vote *Vote) {
	if _, present := node.ChangeView[nodeFrom]; !present {
		node.ChangeView[nodeFrom] = vote
	}
}

func (node *BlockNode) addResult(nodeID uint32, resultMsg *ExecMerkleMsg) error {
	if root, present := node.Result[nodeID]; present {
		if resultMsg.ExecMerkleRoot != root {
			return fmt.Errorf("execMerkleRoot unmatched sig")
		}
	} else {
		node.Result[nodeID] = resultMsg.ExecMerkleRoot
	}
	return nil
}

func (node *BlockNode) addCommitVote(nodeFrom uint32, vote *Vote) {
	if node.Height <= vote.Height && node.NodeState <= LOCKED {
		if _, present := node.Commit[nodeFrom]; !present {
			node.Commit[nodeFrom] = vote
		}
	}
}

func (node *BlockNode) addPrepareVote(nodeFrom uint32, vote *Vote) {
	if node.Height <= vote.Height && node.NodeState <= LOCKED {
		if _, present := node.Prepare[nodeFrom]; !present {
			node.Prepare[nodeFrom] = vote
		}
	}
}

func (node *BlockNode) addProposal(blk *types.Block) {
	node.Proposal = blk
}

func (node *BlockNode) isLeader(peerIdx uint32) bool {
	if node.ParticipantConfig == nil {
		return false
	}

	return node.ParticipantConfig.Leader == peerIdx
}

func (node *BlockNode) isProactor(peerIdx uint32) bool {
	if node.ParticipantConfig == nil {
		return false
	}

	for _, idx := range node.ParticipantConfig.Proactors {
		if idx == peerIdx {
			return true
		}
	}
	return false
}

func (node *BlockNode) hasPreparedBy(peerIdx uint32) bool {
	_, prepared := node.Prepare[peerIdx]
	_, committed := node.Commit[peerIdx]
	return prepared || committed
}

func (node *BlockNode) isProprepared() bool {
	// check if proactor of node has prepared
	return false
}

func (node *BlockNode) getCommitJustifies() *Justify {
	return nil
}
