package agent_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
	"github.com/filecoin-project/specs-actors/v2/support/agent"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
	vm_test "github.com/filecoin-project/specs-actors/v2/support/vm"
)

func TestCreate20Miners(t *testing.T) {
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1000), big.NewInt(1e18))
	minerCount := 20

	rnd := rand.New(rand.NewSource(42))

	sim := agent.NewSim(ctx, t, newBlockStore, agent.SimConfig{Seed: rnd.Int63()})
	accounts := vm_test.CreateAccounts(ctx, t, sim.GetVM(), minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		accounts,
		agent.MinerAgentConfig{
			PrecommitRate:    2.5,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  initialBalance,
			MinMarketBalance: big.Zero(),
			MaxMarketBalance: big.Zero(),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	// give it twice the number of ticks to account for variation in the rate
	for i := 0; i < 2*minerCount; i++ {
		require.NoError(t, sim.Tick())
	}

	// add 1 agent for miner generator
	assert.Equal(t, minerCount+1, len(sim.Agents))

	for _, a := range sim.Agents {
		miner, ok := a.(*agent.MinerAgent)
		if ok {
			actor, found, err := sim.GetVM().GetActor(miner.IDAddress)
			require.NoError(t, err)
			require.True(t, found)

			// demonstrate actor is created and has correct balance
			assert.Equal(t, initialBalance, actor.Balance)
		}
	}
}

func TestCommitPowerAndCheckInvariants(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1e9), big.NewInt(1e18))
	minerCount := 1

	rnd := rand.New(rand.NewSource(42))
	sim := agent.NewSim(ctx, t, newBlockStore, agent.SimConfig{Seed: rnd.Int63()})
	accounts := vm_test.CreateAccounts(ctx, t, sim.GetVM(), minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		accounts,
		agent.MinerAgentConfig{
			PrecommitRate:    0.1,
			FaultRate:        0.00001,
			RecoveryRate:     0.0001,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  initialBalance,
			MinMarketBalance: big.Zero(),
			MaxMarketBalance: big.Zero(),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	var pwrSt power.State
	for i := 0; i < 100_000; i++ {
		require.NoError(t, sim.Tick())

		epoch := sim.GetVM().GetEpoch()
		if epoch%100 == 0 {
			stateTree, err := sim.GetVM().GetStateTree()
			require.NoError(t, err)

			totalBalance, err := sim.GetVM().GetTotalActorBalance()
			require.NoError(t, err)

			acc, err := states.CheckStateInvariants(stateTree, totalBalance, sim.GetVM().GetEpoch()-1)
			require.NoError(t, err)
			require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))

			// assume each sector is 32Gb
			sectorCount := big.Div(pwrSt.TotalBytesCommitted, big.NewInt(32<<30))

			fmt.Printf("Power at %d: raw: %v  cmtRaw: %v  cmtSecs: %d  cnsMnrs: %d avgWins: %.3f  msgs: %d\n",
				epoch, pwrSt.TotalRawBytePower, pwrSt.TotalBytesCommitted, sectorCount.Uint64(),
				pwrSt.MinerAboveMinPowerCount, float64(sim.WinCount)/float64(epoch), sim.MessageCount)
		}
	}
}

func TestCommitAndCheckReadWriteStats(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1e8), big.NewInt(1e18))
	cumulativeStats := make(vm_test.StatsByCall)
	minerCount := 10
	clientCount := 9

	// set up sim
	rnd := rand.New(rand.NewSource(42))
	sim := agent.NewSim(ctx, t, newBlockStore, agent.SimConfig{
		Seed:             rnd.Int63(),
		CheckpointEpochs: 1000,
	})

	// create miners
	workerAccounts := vm_test.CreateAccounts(ctx, t, sim.GetVM(), minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		workerAccounts,
		agent.MinerAgentConfig{
			PrecommitRate:    2.0,
			FaultRate:        0.00001,
			RecoveryRate:     0.0001,
			UpgradeSectors:   true,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  big.Div(initialBalance, big.NewInt(2)),
			MinMarketBalance: big.NewInt(1e18),
			MaxMarketBalance: big.NewInt(2e18),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	clientAccounts := vm_test.CreateAccounts(ctx, t, sim.GetVM(), clientCount, initialBalance, rnd.Int63())
	dealAgents := agent.AddDealClientsForAccounts(sim, clientAccounts, rnd.Int63(), agent.DealClientConfig{
		DealRate:         .01,
		MinPieceSize:     1 << 29,
		MaxPieceSize:     32 << 30,
		MinStoragePrice:  big.Zero(),
		MaxStoragePrice:  abi.NewTokenAmount(200_000_000),
		MinMarketBalance: big.NewInt(1e18),
		MaxMarketBalance: big.NewInt(2e18),
	})

	var pwrSt power.State
	for i := 0; i < 50_000; i++ {
		require.NoError(t, sim.Tick())

		epoch := sim.GetVM().GetEpoch()
		if epoch%100 == 0 {
			// compute number of deals
			deals := 0
			for _, da := range dealAgents {
				deals += da.DealCount
			}

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))

			// assume each sector is 32Gb
			sectorCount := big.Div(pwrSt.TotalBytesCommitted, big.NewInt(32<<30))

			fmt.Printf("Power at %d: raw: %v  cmtRaw: %v  cmtSecs: %d  msgs: %d  deals: %d  gets: %d  puts: %d  write bytes: %d  read bytes: %d\n",
				epoch, pwrSt.TotalRawBytePower, pwrSt.TotalBytesCommitted, sectorCount.Uint64(),
				sim.MessageCount, deals, sim.GetVM().StoreReads(), sim.GetVM().StoreWrites(),
				sim.GetVM().StoreReadBytes(), sim.GetVM().StoreWriteBytes())
		}

		cumulativeStats.MergeAllStats(sim.GetCallStats())

		if sim.GetVM().GetEpoch()%1000 == 0 {
			for method, stats := range cumulativeStats {
				printCallStats(method, stats, "")
			}
			cumulativeStats = make(vm_test.StatsByCall)
		}
	}
}

func TestCreateDeals(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1e9), big.NewInt(1e18))
	minerCount := 3
	clientCount := 9

	// set up sim
	rnd := rand.New(rand.NewSource(42))
	sim := agent.NewSim(ctx, t, newBlockStore, agent.SimConfig{Seed: rnd.Int63()})

	// create miners
	workerAccounts := vm_test.CreateAccounts(ctx, t, sim.GetVM(), minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		workerAccounts,
		agent.MinerAgentConfig{
			PrecommitRate:    0.1,
			FaultRate:        0.0001,
			RecoveryRate:     0.0001,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  big.Div(initialBalance, big.NewInt(2)),
			MinMarketBalance: big.NewInt(1e18),
			MaxMarketBalance: big.NewInt(2e18),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	clientAccounts := vm_test.CreateAccounts(ctx, t, sim.GetVM(), clientCount, initialBalance, rnd.Int63())
	dealAgents := agent.AddDealClientsForAccounts(sim, clientAccounts, rnd.Int63(), agent.DealClientConfig{
		DealRate:         .01,
		MinPieceSize:     1 << 29,
		MaxPieceSize:     32 << 30,
		MinStoragePrice:  big.Zero(),
		MaxStoragePrice:  abi.NewTokenAmount(200_000_000),
		MinMarketBalance: big.NewInt(1e18),
		MaxMarketBalance: big.NewInt(2e18),
	})

	var pwrSt power.State
	for i := 0; i < 100_000; i++ {
		require.NoError(t, sim.Tick())

		epoch := sim.GetVM().GetEpoch()
		if epoch%100 == 0 {
			stateTree, err := sim.GetVM().GetStateTree()
			require.NoError(t, err)

			totalBalance, err := sim.GetVM().GetTotalActorBalance()
			require.NoError(t, err)

			acc, err := states.CheckStateInvariants(stateTree, totalBalance, sim.GetVM().GetEpoch()-1)
			require.NoError(t, err)
			require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))

			// assume each sector is 32Gb
			sectorCount := big.Div(pwrSt.TotalBytesCommitted, big.NewInt(32<<30))

			// compute number of deals
			deals := 0
			for _, da := range dealAgents {
				deals += da.DealCount
			}

			fmt.Printf("Power at %d: raw: %v  cmtRaw: %v  cmtSecs: %d  cnsMnrs: %d avgWins: %.3f  msgs: %d  deals: %d\n",
				epoch, pwrSt.TotalRawBytePower, pwrSt.TotalBytesCommitted, sectorCount.Uint64(),
				pwrSt.MinerAboveMinPowerCount, float64(sim.WinCount)/float64(epoch), sim.MessageCount, deals)
		}
	}
}

func TestCCUpgrades(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1e10), big.NewInt(1e18))
	minerCount := 10
	clientCount := 9

	// set up sim
	rnd := rand.New(rand.NewSource(42))
	sim := agent.NewSim(ctx, t, newBlockStore, agent.SimConfig{Seed: rnd.Int63()})

	// create miners
	workerAccounts := vm_test.CreateAccounts(ctx, t, sim.GetVM(), minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		workerAccounts,
		agent.MinerAgentConfig{
			PrecommitRate:    2.0,
			FaultRate:        0.00001,
			RecoveryRate:     0.0001,
			UpgradeSectors:   true,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  big.Div(initialBalance, big.NewInt(2)),
			MinMarketBalance: big.NewInt(1e18),
			MaxMarketBalance: big.NewInt(2e18),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	clientAccounts := vm_test.CreateAccounts(ctx, t, sim.GetVM(), clientCount, initialBalance, rnd.Int63())
	agent.AddDealClientsForAccounts(sim, clientAccounts, rnd.Int63(), agent.DealClientConfig{
		DealRate:         .01,
		MinPieceSize:     1 << 29,
		MaxPieceSize:     32 << 30,
		MinStoragePrice:  big.Zero(),
		MaxStoragePrice:  abi.NewTokenAmount(200_000_000),
		MinMarketBalance: big.NewInt(1e18),
		MaxMarketBalance: big.NewInt(2e18),
	})

	var pwrSt power.State
	for i := 0; i < 100_000; i++ {
		require.NoError(t, sim.Tick())

		epoch := sim.GetVM().GetEpoch()
		if epoch%100 == 0 {
			stateTree, err := sim.GetVM().GetStateTree()
			require.NoError(t, err)

			totalBalance, err := sim.GetVM().GetTotalActorBalance()
			require.NoError(t, err)

			acc, err := states.CheckStateInvariants(stateTree, totalBalance, sim.GetVM().GetEpoch()-1)
			require.NoError(t, err)
			require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))

			// assume each sector is 32Gb
			sectorCount := big.Div(pwrSt.TotalBytesCommitted, big.NewInt(32<<30))

			// compute stats
			deals := 0
			upgrades := uint64(0)
			for _, a := range sim.Agents {
				switch agnt := a.(type) {
				case *agent.MinerAgent:
					upgrades += agnt.UpgradedSectors
				case *agent.DealClientAgent:
					deals += agnt.DealCount
				}
			}

			// compute upgrades

			fmt.Printf("Power at %d: raw: %v  cmtRaw: %v  cmtSecs: %d  msgs: %d  deals: %d  upgrades: %d\n",
				epoch, pwrSt.TotalRawBytePower, pwrSt.TotalBytesCommitted, sectorCount.Uint64(),
				sim.MessageCount, deals, upgrades)
		}
	}
}

func printCallStats(method vm_test.MethodKey, stats *vm_test.CallStats, indent string) { // nolint:unused
	fmt.Printf("%s%v:%d: calls: %d  gets: %d  puts: %d  avg gets: %.2f, avg puts: %.2f\n",
		indent, builtin.ActorNameByCode(method.Code), method.Method, stats.Calls, stats.Reads, stats.Writes,
		float32(stats.Reads)/float32(stats.Calls), float32(stats.Writes)/float32(stats.Calls))

	if stats.SubStats == nil {
		return
	}

	for m, s := range stats.SubStats {
		printCallStats(m, s, indent+"  ")
	}
}

func newBlockStore() cbor.IpldBlockstore {
	return ipld.NewBlockStoreInMemory()
}
