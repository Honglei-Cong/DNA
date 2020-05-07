// SPDX-License-Identifier: LGPL-3.0-or-later
// Copyright 2019 DNA Dev team
//
/*
 * Copyright (C) 2018 The ontology Authors
 * This file is part of The ontology library.
 *
 * The ontology is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The ontology is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with The ontology.  If not, see <http://www.gnu.org/licenses/>.
 */

package poa

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/DNAProject/DNA/account"
	"github.com/DNAProject/DNA/common"
	"github.com/DNAProject/DNA/common/config"
	"github.com/DNAProject/DNA/consensus/vbft/config"
	"github.com/DNAProject/DNA/core/ledger"
	"github.com/DNAProject/DNA/core/signature"
	"github.com/DNAProject/DNA/core/states"
	scommon "github.com/DNAProject/DNA/core/store/common"
	"github.com/DNAProject/DNA/core/store/overlaydb"
	"github.com/DNAProject/DNA/core/types"
	gov "github.com/DNAProject/DNA/smartcontract/service/native/governance"
	nutils "github.com/DNAProject/DNA/smartcontract/service/native/utils"
	"github.com/ontio/ontology-crypto/keypair"
	"github.com/ontio/ontology-crypto/vrf"
)

func SignMsg(account *account.Account, msg ConsensusMsg) ([]byte, error) {
	sink := common.NewZeroCopySink(nil)
	if err := msg.Serialize(sink); err != nil {
		return nil, fmt.Errorf("failed to marshal msg when signing: %s", err)
	}

	return signature.Sign(account, sink.Bytes())
}

func hashData(data []byte) common.Uint256 {
	t := sha256.Sum256(data)
	f := sha256.Sum256(t[:])
	return common.Uint256(f)
}

func HashMsg(msg ConsensusMsg) (common.Uint256, error) {

	// FIXME: has to do marshal on each call

	data, err := SerializeConsensusMsg(msg)
	if err != nil {
		return common.Uint256{}, fmt.Errorf("failed to marshal block: %s", err)
	}

	return hashData(data), nil
}

type vrfData struct {
	BlockNum uint32 `json:"block_num"`
	View     uint32 `json:"view"`
	PrevVrf  []byte `json:"prev_vrf"`
}

func computeVrf(sk keypair.PrivateKey, blkNum, view uint32, prevVrf []byte) ([]byte, []byte, error) {
	data, err := json.Marshal(&vrfData{
		BlockNum: blkNum,
		View:     view,
		PrevVrf:  prevVrf,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("computeVrf failed to marshal vrfData: %s", err)
	}

	return vrf.Vrf(sk, data)
}

func verifyVrf(pk keypair.PublicKey, blkNum, view uint32, prevVrf, newVrf, proof []byte) error {
	data, err := json.Marshal(&vrfData{
		BlockNum: blkNum,
		View:     view,
		PrevVrf:  prevVrf,
	})
	if err != nil {
		return fmt.Errorf("verifyVrf failed to marshal vrfData: %s", err)
	}

	result, err := vrf.Verify(pk, data, newVrf, proof)
	if err != nil {
		return fmt.Errorf("verifyVrf failed: %s", err)
	}
	if !result {
		return fmt.Errorf("verifyVrf failed")
	}
	return nil
}

func isUpdate(memdb *overlaydb.MemDB, view uint32) (bool, error) {
	goveranceview, err := GetGovernanceView(memdb)
	if err != nil {
		return false, err
	}
	if goveranceview.View > view {
		return true, nil
	}
	return false, nil
}

func getRawStorageItemFromMemDb(memdb *overlaydb.MemDB, addr common.Address, key []byte) (value []byte, unkown bool) {
	rawKey := make([]byte, 0, 1+common.ADDR_LEN+len(key))
	rawKey = append(rawKey, byte(scommon.ST_STORAGE))
	rawKey = append(rawKey, addr[:]...)
	rawKey = append(rawKey, key...)
	return memdb.Get(rawKey)
}

func GetStorageValue(memdb *overlaydb.MemDB, backend *ledger.Ledger, addr common.Address, key []byte) (value []byte, err error) {
	if memdb == nil {
		return backend.GetStorageItem(addr, key)
	}
	rawValue, unknown := getRawStorageItemFromMemDb(memdb, addr, key)
	if unknown {
		return backend.GetStorageItem(addr, key)
	}
	if len(rawValue) == 0 {
		return nil, scommon.ErrNotFound
	}

	value, err = states.GetValueFromRawStorageItem(rawValue)
	return
}

func GetGovernanceView(memdb *overlaydb.MemDB) (*gov.GovernanceView, error) {
	value, err := GetStorageValue(memdb, ledger.DefLedger, nutils.GovernanceContractAddress, []byte(gov.GOVERNANCE_VIEW))
	if err != nil {
		return nil, err
	}
	governanceView := new(gov.GovernanceView)
	err = governanceView.Deserialize(bytes.NewBuffer(value))
	if err != nil {
		return nil, err
	}
	return governanceView, nil
}

type VrfSeedData struct {
	BlockNum uint32 `json:"block_num"`
	View     uint32 `json:"view"`
	VrfValue []byte `json:"vrf_value"`
}

func getBlockVrfValue(block *types.Block) ([]byte, []byte, error) {
	if block == nil {
		return nil, nil, fmt.Errorf("nil block in getBlockVrfValue")
	}

	blkInfo := vconfig.VbftBlockInfo{}
	if err := json.Unmarshal(block.Header.ConsensusPayload, blkInfo); err != nil {
		return nil, nil, fmt.Errorf("unmarshal blockinfo (%d): %s", block.Header.Height, err)
	}
	return blkInfo.VrfValue, blkInfo.VrfProof, nil
}

func getBlockLastConfigHeight(block *types.Block) (uint32, error) {
	if block == nil {
		return 0, fmt.Errorf("nil block in getBlockLastConfigHeight")
	}

	blkInfo := vconfig.VbftBlockInfo{}
	if err := json.Unmarshal(block.Header.ConsensusPayload, blkInfo); err != nil {
		return 0, fmt.Errorf("unmarshal blockinfo (%d): %s", block.Header.Height, err)
	}
	return blkInfo.LastConfigBlockNum, nil
}

func getBlockView(block *types.Block) (uint32, map[uint32][]byte, error) {
	if block == nil {
		return 0, nil, fmt.Errorf("nil block in getBlockView")
	}

	blkInfo := vconfig.VbftBlockInfo{}
	if err := json.Unmarshal(block.Header.ConsensusPayload, blkInfo); err != nil {
		return 0, nil, fmt.Errorf("unmarshal blockinfo (%d): %s", block.Header.Height, err)
	}
	if blkInfo.Round == nil {
		return 0, nil, nil
	}
	return blkInfo.Round.Round, blkInfo.Round.ProofSigs, nil
}
