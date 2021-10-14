package nv14

import (
	"container/list"
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	power5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v6/actors/util/smoothing"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
)

type powerMigrator struct{}

func (m powerMigrator) migratedCodeCID() cid.Cid {
	return builtin.StorageMarketActorCodeID
}

func (m powerMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState power5.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	//convert the state, i guess? do I really have to do this?
	outState := power.State{
		TotalRawBytePower:         inState.TotalRawBytePower,
		TotalBytesCommitted:       inState.TotalBytesCommitted,
		TotalQualityAdjPower:      inState.TotalQualityAdjPower,
		TotalQABytesCommitted:     inState.TotalQABytesCommitted,
		TotalPledgeCollateral:     inState.TotalPledgeCollateral,
		ThisEpochRawBytePower:     inState.ThisEpochRawBytePower,
		ThisEpochQualityAdjPower:  inState.ThisEpochQualityAdjPower,
		ThisEpochPledgeCollateral: inState.ThisEpochPledgeCollateral,
		ThisEpochQAPowerSmoothed:  smoothing.FilterEstimate(inState.ThisEpochQAPowerSmoothed),
		MinerCount:                inState.MinerCount,
		MinerAboveMinPowerCount:   inState.MinerAboveMinPowerCount,
		CronEventQueue:            inState.CronEventQueue,
		FirstCronEpoch:            inState.FirstCronEpoch,
		Claims:                    inState.Claims,
		ProofValidationBatch:      inState.ProofValidationBatch,
	}

	wrappedStore := adt.WrapStore(ctx, store)

	claims, err := adt.AsMap(wrappedStore, outState.Claims, builtin.DefaultHamtBitwidth)
	if err != nil {
		return nil, err
	}

	claimAddressesToRemove := list.New()
	var claim power.Claim
	err = claims.ForEach(&claim, func(key string) error {
		if isTestPostProofType(claim.WindowPoStProofType) {
			claimAddressesToRemove.PushBack(claim)
			// XXX: now how do i transfer any funds from miner to owner?
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// outState gets mutated in here- remove all the claimAddresses with deleteClaim
	for cAddr := claimAddressesToRemove.Front(); cAddr != nil; cAddr = cAddr.Next() {
		outState.DeleteClaim(claims, address.Address(cAddr.Value))
		outState.MinerCount--
	}

	newHead, err := store.Put(ctx, &outState)
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err
}

func isTestPostProofType(proofType abi.RegisteredPoStProof) bool {
	testPoStProofTypes := [6]abi.RegisteredPoStProof{abi.RegisteredPoStProof_StackedDrgWinning2KiBV1,
		abi.RegisteredPoStProof_StackedDrgWinning8MiBV1,
		abi.RegisteredPoStProof_StackedDrgWinning512MiBV1,
		abi.RegisteredPoStProof_StackedDrgWindow2KiBV1,
		abi.RegisteredPoStProof_StackedDrgWindow8MiBV1,
		abi.RegisteredPoStProof_StackedDrgWindow512MiBV1,
	}
	for i := 0; i < 6; i++ {
		if proofType == testPoStProofTypes[i] {
			return true
		}
	}
	return false
}

// An adt.Map key that just preserves the underlying string.
type StringKey string

func (k StringKey) Key() string {
	return string(k)
}
