package nv14

import (
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	power5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v6/actors/util/smoothing"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"
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

	var claim power.Claim
	err = claims.ForEach(&claim, func(key string) error {
		if isTestPostProofType(claim.WindowPoStProofType) {
			addr, err := address.NewFromString(key)
			if err != nil {
				return err
			}
			if claim.RawBytePower.GreaterThan(big.Zero()) {
				return xerrors.Errorf("nonzero RawBytePower on claim from miner with test proof size. This is not good.")
			}
			if claim.QualityAdjPower.GreaterThan(big.Zero()) {
				return xerrors.Errorf("nonzero QualityAdjPower on claim from miner with test proof size. This is not good.")
			}
			if builtin.ConsensusMinerMinPower(claim.WindowPoStProofType).LessThanEqual(big.Zero()) {
			
			outState.DeleteClaim(claims, addr)
			outState.MinerCount--

			// XXX: assert that they have not committed anything, no power, no locked funds at all
			// XXX: because that would be a biiiiig problem

			// XXX: are you SURE that these weird miner types will be in claims? make sure
			// should not need to worry about mineraboveminpowercount, but make sure!
			// make SURE they were only added to minercount and not mineraboveminpowercount
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	newHead, err := store.Put(ctx, &outState)
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err

	// XXX: what happens if someone sends these addresses some funds??? no good.

	// to test: add one of each type of miner, maybe add some sectors, make a complex enough state and check some invariants???
	// give some some fees, give some no fees, etc, etc, etc

	// XXX: should I loop through and check that the minpowercount and the minercount are correct after this?
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
