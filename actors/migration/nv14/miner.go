package nv14

import (
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	miner5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"
)

type minerMigrator struct{}

func (m minerMigrator) migratedCodeCID() cid.Cid {
	return builtin.StorageMinerActorCodeID
}

func (m minerMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState miner5.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}
	wrappedStore := adt.WrapStore(ctx, store)
	minerInfo, err := inState.GetInfo(wrappedStore)
	if err != nil {
		return nil, err
	}
	if isTestPostProofType(minerInfo.WindowPoStProofType) {
		if inState.PreCommitDeposits.GreaterThan(big.Zero()) {
			return nil, xerrors.Errorf("test type miner has nonzero PreCommitDeposits at address %v.", in.address)
		}
		if inState.LockedFunds.GreaterThan(big.Zero()) {
			return nil, xerrors.Errorf("test type miner has nonzero LockedFunds at address %v.", in.address)
		}
		if inState.FeeDebt.GreaterThan(big.Zero()) {
			return nil, xerrors.Errorf("test type miner has nonzero FeeDebt at address %v.", in.address)
		}
		if inState.InitialPledge.GreaterThan(big.Zero()) {
			return nil, xerrors.Errorf("test type miner has nonzero InitialPledge at address %v.", in.address)
		}
		sectors, err := adt.AsArray(wrappedStore, inState.Sectors, builtin.DefaultHamtBitwidth)
		if err != nil {
			return nil, err
		}
		if sectors.Length() != 0 {
			return nil, xerrors.Errorf("test type miner has nonzero length of Sectors at address %v.", in.address)
		}
		precommittedSectors, err := adt.AsArray(wrappedStore, inState.PreCommittedSectors, builtin.DefaultHamtBitwidth)
		if err != nil {
			return nil, err
		}
		if precommittedSectors.Length() != 0 {
			return nil, xerrors.Errorf("test type miner has nonzero length of PrecommittedSectors at address %v.", in.address)
		}

		return &actorMigrationResult{
			newCodeCID:                     m.migratedCodeCID(),
			newHead:                        in.head,
			minerTypeMigrationShouldDelete: true,
			minerTypeMigrationBalanceTransferInfo: struct {
				address.Address
				big.Int
			}{minerInfo.Owner, in.balance},
		}, nil
	}

	return &actorMigrationResult{
		newCodeCID:                     m.migratedCodeCID(),
		newHead:                        in.head,
		minerTypeMigrationShouldDelete: false,
	}, nil
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


		return &actorMigrationResult{
			newCodeCID:                     m.migratedCodeCID(),
			newHead:                        in.head,
			minerTypeMigrationShouldDelete: true,
			minerTypeMigrationBalanceTransferInfo: struct {
				address.Address
				big.Int
			}{minerInfo.Owner, in.balance},
		}, nil
	}

	return &actorMigrationResult{
		newCodeCID:                     m.migratedCodeCID(),
		newHead:                        in.head,
		minerTypeMigrationShouldDelete: false,
	}, nil
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
