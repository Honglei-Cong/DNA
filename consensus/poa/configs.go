package poa

import (
	"encoding/json"
	"fmt"
	"github.com/DNAProject/DNA/consensus/vbft/config"
	"math"
	"sync"
	"time"
)

type ConfigPool struct {
	lock       sync.RWMutex
	server     *Server
	pool       *BlockPool
	historyLen int

	epoches      map[uint32]uint32               // chaincfg-update-blk# -> epoch#
	chainConfigs map[uint32]*vconfig.ChainConfig // epoch# -> chain config
}

func newConfigPool(server *Server, pool *BlockPool) (*ConfigPool, error) {
	if server == nil || pool == nil {
		return nil, fmt.Errorf("nil server/store on init config pool")
	}

	cfgs := &ConfigPool{
		server:       server,
		pool:         pool,
		historyLen:   16,
		epoches:      make(map[uint32]uint32),
		chainConfigs: make(map[uint32]*vconfig.ChainConfig),
	}

	if err := cfgs.updateChainConfig(); err != nil {
		return nil, err
	}
	return cfgs, nil
}

func (cfgs *ConfigPool) getChainConfig(blknum uint32) *vconfig.ChainConfig {
	epoch := cfgs._getEpoch(blknum)
	if epoch == UNKNOWN_EPOCH {
		return nil
	}

	cfgs.lock.RLock()
	defer cfgs.lock.RUnlock()

	return cfgs.chainConfigs[epoch]
}

func (cfgs *ConfigPool) _getEpoch(blknum uint32) uint32 {
	if blknum > cfgs.pool.getSealedBlockNum() {
		return UNKNOWN_EPOCH
	}

	cfgs.lock.RLock()
	defer cfgs.lock.RUnlock()

	lastEpochStart := uint32(0)
	maxEpoch := uint32(0)
	epoch := uint32(UNKNOWN_EPOCH)
	for epStart, ep := range cfgs.epoches {
		if epStart == blknum {
			epoch = ep
			break
		} else if epStart > lastEpochStart {
			lastEpochStart = epStart
			epoch = ep
		}
		if ep > maxEpoch {
			maxEpoch = ep
		}
	}

	if epoch == UNKNOWN_EPOCH {
		// in latest epoch
		// FIXME: getting epoch of block when its epoch-switching-block not finalized
		if maxEpoch > 1 {
			maxEpoch -= 1
		}
		return maxEpoch
	}

	if epoch <= 2 {
		return 1
	}
	if epStart, present := cfgs.epoches[epoch-1]; present {
		if blknum > epStart {
			return epoch - 2
		}
	}
	return UNKNOWN_EPOCH
}

func (cfgs *ConfigPool) updateChainConfig() error {
	cfgs.lock.Lock()
	defer cfgs.lock.Unlock()

	startBlkNum := cfgs.pool.getSealedBlockNum()
	for len(cfgs.chainConfigs) < cfgs.historyLen+1 { // +1 to make new-chainconfig accepted
		// get block
		blk, err := cfgs.pool.getSealedBlock(startBlkNum)
		if err != nil {
			return fmt.Errorf("failed to sealed block, height: %d: %s", startBlkNum, err)
		}

		// parse consensus payload
		blkInfo := &vconfig.VbftBlockInfo{}
		if err := json.Unmarshal(blk.Header.ConsensusPayload, blkInfo); err != nil {
			return fmt.Errorf("failed to parse blockinfo, height: %d: %s", startBlkNum, err)
		}

		if blkInfo.NewChainConfig != nil {
			// add config to pool
			cfg := *blkInfo.NewChainConfig
			cfgs.epoches[startBlkNum] = cfg.View
			cfgs.chainConfigs[cfg.View] = &cfg

			if startBlkNum == 0 {
				break
			}
			startBlkNum -= 1 // continue to previous epoch
		} else {
			// if lastConfigBlock has been in pool, stop looping
			if _, present := cfgs.epoches[blkInfo.LastConfigBlockNum]; present {
				break
			}
			if blkInfo.LastConfigBlockNum == math.MaxUint32 {
				break
			}
			startBlkNum = blkInfo.LastConfigBlockNum
		}
	}

	// if config-pool contains more than History-Len, remove old configs
	for len(cfgs.chainConfigs) > cfgs.historyLen {
		// remove oldest config
		lowestCfgHeight := uint32(math.MaxUint32)
		for h := range cfgs.epoches {
			if h < lowestCfgHeight {
				lowestCfgHeight = h
			}
		}
		epoch := cfgs.epoches[lowestCfgHeight]
		delete(cfgs.chainConfigs, epoch)
		delete(cfgs.epoches, lowestCfgHeight)
	}

	return nil
}

func (cfgs *ConfigPool) GetCurrentBlockDelay() time.Duration {
	chaincfg := cfgs.getChainConfig(cfgs.pool.getSealedBlockNum())
	if chaincfg != nil {
		return chaincfg.BlockMsgDelay
	}

	return time.Second * 10 // DEFAULT BLOCK DELAY
}
