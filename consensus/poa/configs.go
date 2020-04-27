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
	lock         sync.RWMutex
	server       *Server
	pool         *BlockPool
	HistoryLen   int
	ChainConfigs map[uint32]*vconfig.ChainConfig // indexed by chaincfg-update-blknum
}

func newConfigPool(server *Server, pool *BlockPool) (*ConfigPool, error) {
	if server == nil || pool == nil {
		return nil, fmt.Errorf("nil server/store on init config pool")
	}

	cfgs := &ConfigPool{
		server:       server,
		pool:         pool,
		HistoryLen:   16,
		ChainConfigs: make(map[uint32]*vconfig.ChainConfig),
	}

	if err := cfgs.updateChainConfig(); err != nil {
		return nil, err
	}
	return cfgs, nil
}

// FIXME: get last-epoch chain config
func (cfgs *ConfigPool) getChainConfig(blknum uint32) *vconfig.ChainConfig {
	if blknum > cfgs.pool.getSealedBlockNum() {
		return nil
	}

	cfgs.lock.RLock()
	defer cfgs.lock.RUnlock()

	lastEpochStart := uint32(0)
	var lastEpochCfg *vconfig.ChainConfig
	for epochStart, cfg := range cfgs.ChainConfigs {
		if epochStart > blknum {
			continue
		}
		if epochStart == blknum {
			return cfg
		}
		if epochStart > lastEpochStart {
			lastEpochStart = epochStart
			lastEpochCfg = cfg
		}
	}

	return lastEpochCfg
}

func (cfgs *ConfigPool) GetEpoch(blknum uint32) uint32 {
	if blknum > cfgs.pool.getSealedBlockNum() {
		return UNKNOWN_EPOCH
	}

	cfgs.lock.RLock()
	defer cfgs.lock.RUnlock()

	lastEpochStart := uint32(0)
	lastEpoch := uint32(UNKNOWN_EPOCH)
	for epochStart, cfg := range cfgs.ChainConfigs {
		if epochStart > blknum {
			continue
		}
		if epochStart == blknum {
			return cfg.View
		}
		if epochStart > lastEpochStart {
			lastEpochStart = epochStart
			lastEpoch = cfg.View
		}
	}

	return lastEpoch
}

func (cfgs *ConfigPool) updateChainConfig() error {
	cfgs.lock.Lock()
	defer cfgs.lock.Unlock()

	startBlkNum := cfgs.pool.getSealedBlockNum()
	for len(cfgs.ChainConfigs) < cfgs.HistoryLen+1 {  // +1 to make new-chainconfig accepted
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
			cfgs.ChainConfigs[startBlkNum] = &cfg

			if startBlkNum == 0 {
				break
			}
			startBlkNum -= 1 // continue to previous epoch
		} else {
			// if lastConfigBlock has been in pool, stop looping
			if _, present := cfgs.ChainConfigs[blkInfo.LastConfigBlockNum]; present {
				break
			}
			if blkInfo.LastConfigBlockNum == math.MaxUint32 {
				break
			}
			startBlkNum = blkInfo.LastConfigBlockNum
		}
	}

	// if config-pool contains more than History-Len, remove old configs
	for len(cfgs.ChainConfigs) > cfgs.HistoryLen {
		// remove oldest config
		lowestCfgHeight := uint32(math.MaxUint32)
		for h := range cfgs.ChainConfigs {
			if h < lowestCfgHeight {
				lowestCfgHeight = h
			}
		}
		delete(cfgs.ChainConfigs, lowestCfgHeight)
	}

	return nil
}

func (cfgs *ConfigPool) GetCurrentBlockDelay() time.Duration {
	cfgs.lock.RLock()
	defer cfgs.lock.RUnlock()

	defaultDelay := time.Second * 10
	epoch := uint32(0)
	for ep, cfg := range cfgs.ChainConfigs {
		if ep > epoch {
			epoch = ep
			defaultDelay = cfg.BlockMsgDelay
		}
	}

	return defaultDelay
}
