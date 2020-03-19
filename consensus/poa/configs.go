package poa

import (
	"encoding/json"
	"fmt"
	"github.com/DNAProject/DNA/consensus/vbft/config"
	"math"
	"sync"
)

type ConfigPool struct {
	lock         sync.RWMutex
	server       *Server
	pool         *BlockPool
	ChainConfigs map[uint32]*vconfig.ChainConfig // indexed by chaincfg-update-blknum
}

func newConfigPool(server *Server, pool *BlockPool) (*ConfigPool, error) {
	if server == nil || pool == nil {
		return nil, fmt.Errorf("nil server/store on init config pool")
	}

	cfgs := &ConfigPool{
		server:       server,
		pool:         pool,
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

func (cfgs *ConfigPool) updateChainConfig() error {
	cfgs.lock.Lock()
	defer cfgs.lock.Unlock()

	cfgBlkNum := cfgs.pool.getSealedBlockNum()
	blk, err := cfgs.pool.getSealedBlock(cfgBlkNum)
	if err != nil {
		return fmt.Errorf("failed to sealed block, height: %d: %s", cfgBlkNum, err)
	}

	blkInfo := &vconfig.VbftBlockInfo{}
	if err := json.Unmarshal(blk.Header.ConsensusPayload, blkInfo); err != nil {
		return fmt.Errorf("failed to parse blockinfo, height: %d: %s", cfgBlkNum, err)
	}

	if blkInfo.NewChainConfig == nil {
		if _, present := cfgs.ChainConfigs[blkInfo.LastConfigBlockNum]; present {
			return nil
		}
		if blkInfo.LastConfigBlockNum != math.MaxUint32 {
			// load last-config-block
			blk, err := cfgs.pool.getSealedBlock(blkInfo.LastConfigBlockNum)
			if err != nil {
				return fmt.Errorf("failed to load config block %d: %s", blkInfo.LastConfigBlockNum, err)
			}
			// parse consensus-data of last-config-lock
			if err := json.Unmarshal(blk.Header.ConsensusPayload, blkInfo); err != nil {
				return fmt.Errorf("failed to parse lastconfig blockinfo, height: %d: %s", blkInfo.LastConfigBlockNum, err)
			}
		}
		if blkInfo.NewChainConfig == nil {
			return nil
		}
		cfgBlkNum = blkInfo.LastConfigBlockNum
	}

	cfg := *blkInfo.NewChainConfig
	cfgs.ChainConfigs[cfgBlkNum] = &cfg
	return nil
}
