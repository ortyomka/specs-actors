package nv14

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/rt"
	builtin5 "github.com/filecoin-project/specs-actors/v5/actors/builtin"
	states5 "github.com/filecoin-project/specs-actors/v5/actors/states"
	builtin6 "github.com/filecoin-project/specs-actors/v6/actors/builtin"
	states6 "github.com/filecoin-project/specs-actors/v6/actors/states"
	adt6 "github.com/filecoin-project/specs-actors/v6/actors/util/adt"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

// Config parameterizes a state tree migration
type Config struct {
	// Number of migration worker goroutines to run.
	// More workers enables higher CPU utilization doing migration computations (including state encoding)
	MaxWorkers uint
	// Capacity of the queue of jobs available to workers (zero for unbuffered).
	// A queue length of hundreds to thousands improves throughput at the cost of memory.
	JobQueueSize uint
	// Capacity of the queue receiving migration results from workers, for persisting (zero for unbuffered).
	// A queue length of tens to hundreds improves throughput at the cost of memory.
	ResultQueueSize uint
	// Time between progress logs to emit.
	// Zero (the default) results in no progress logs.
	ProgressLogPeriod time.Duration
}

type Logger interface {
	// This is the same logging interface provided by the Runtime
	Log(level rt.LogLevel, msg string, args ...interface{})
}

func ActorHeadKey(addr address.Address, head cid.Cid) string {
	return addr.String() + "-h-" + head.String()
}

// Migrates from v13 to v14
//
// This migration only updates the actor code CIDs in the state tree.
// MigrationCache stores and loads cached data. Its implementation must be threadsafe
type MigrationCache interface {
	Write(key string, newCid cid.Cid) error
	Read(key string) (bool, cid.Cid, error)
	Load(key string, loadFunc func() (cid.Cid, error)) (cid.Cid, error)
}

// Migrates the filecoin state tree starting from the global state tree and upgrading all actor state.
// The store must support concurrent writes (even if the configured worker count is 1).
func MigrateStateTree(ctx context.Context, store cbor.IpldStore, actorsRootIn cid.Cid, priorEpoch abi.ChainEpoch, cfg Config, log Logger, cache MigrationCache) (cid.Cid, error) {
	if cfg.MaxWorkers <= 0 {
		return cid.Undef, xerrors.Errorf("invalid migration config with %d workers", cfg.MaxWorkers)
	}

	// Maps prior version code CIDs to migration functions.
	var migrations = map[cid.Cid]actorMigration{
		builtin5.AccountActorCodeID:          nilMigrator{builtin6.AccountActorCodeID},
		builtin5.CronActorCodeID:             nilMigrator{builtin6.CronActorCodeID},
		builtin5.InitActorCodeID:             nilMigrator{builtin6.InitActorCodeID},
		builtin5.MultisigActorCodeID:         nilMigrator{builtin6.MultisigActorCodeID},
		builtin5.PaymentChannelActorCodeID:   nilMigrator{builtin6.PaymentChannelActorCodeID},
		builtin5.RewardActorCodeID:           nilMigrator{builtin6.RewardActorCodeID},
		builtin5.StorageMarketActorCodeID:    nilMigrator{builtin6.StorageMarketActorCodeID},
		builtin5.StorageMinerActorCodeID:     nilMigrator{builtin6.StorageMinerActorCodeID},
		builtin5.StoragePowerActorCodeID:     cachedMigration(cache, powerMigrator{}),
		builtin5.SystemActorCodeID:           nilMigrator{builtin6.SystemActorCodeID},
		builtin5.VerifiedRegistryActorCodeID: nilMigrator{builtin6.VerifiedRegistryActorCodeID},
	}

	// Set of prior version code CIDs for actors to defer during iteration, for explicit migration afterwards.
	var deferredCodeIDs = map[cid.Cid]struct{}{
		// None

		// XXX do the power actor last, after miner actor migration
	}

	if len(migrations)+len(deferredCodeIDs) != 11 {
		panic(fmt.Sprintf("incomplete migration specification with %d code CIDs", len(migrations)))
	}
	startTime := time.Now()

	// Load input and output state trees
	adtStore := adt6.WrapStore(ctx, store)
	actorsIn, err := states5.LoadTree(adtStore, actorsRootIn)
	if err != nil {
		return cid.Undef, err
	}
	actorsOut, err := states5.NewTree(adtStore)
	if err != nil {
		return cid.Undef, err
	}

	// Setup synchronization
	grp, ctx := errgroup.WithContext(ctx)
	// Input and output queues for workers.
	jobCh := make(chan *migrationJob, cfg.JobQueueSize)
	jobResultCh := make(chan *migrationJobResult, cfg.ResultQueueSize)
	// Atomically-modified counters for logging progress
	var jobCount uint32
	var doneCount uint32

	// Iterate all actors in old state root to create migration jobs for each non-deferred actor.
	grp.Go(func() error {
		defer close(jobCh)
		log.Log(rt.INFO, "Creating migration jobs for tree %s", actorsRootIn)
		if err = actorsIn.ForEach(func(addr address.Address, actorIn *states5.Actor) error {
			if _, ok := deferredCodeIDs[actorIn.Code]; ok {
				return nil // Deferred for explicit migration later.
			}
			migration, ok := migrations[actorIn.Code]
			if !ok {
				return xerrors.Errorf("actor with code %s has no registered migration function", actorIn.Code)
			}
			nextInput := &migrationJob{
				Address:        addr,
				Actor:          *actorIn, // Must take a copy, the pointer is not stable.
				cache:          cache,
				actorMigration: migration,
			}
			select {
			case jobCh <- nextInput:
			case <-ctx.Done():
				return ctx.Err()
			}
			atomic.AddUint32(&jobCount, 1)
			return nil
		}); err != nil {
			return err
		}
		log.Log(rt.INFO, "Done creating %d migration jobs for tree %s after %v", jobCount, actorsRootIn, time.Since(startTime))
		return nil
	})

	// Worker threads run jobs.
	var workerWg sync.WaitGroup
	for i := uint(0); i < cfg.MaxWorkers; i++ {
		workerWg.Add(1)
		workerId := i
		grp.Go(func() error {
			defer workerWg.Done()
			for job := range jobCh {
				result, err := job.run(ctx, store, priorEpoch)
				if err != nil {
					return err
				}
				select {
				case jobResultCh <- result:
				case <-ctx.Done():
					return ctx.Err()
				}
				atomic.AddUint32(&doneCount, 1)
			}
			log.Log(rt.INFO, "Worker %d done", workerId)
			return nil
		})
	}
	log.Log(rt.INFO, "Started %d workers", cfg.MaxWorkers)

	// Monitor the job queue. This non-critical goroutine is outside the errgroup and exits when
	// workersFinished is closed, or the context done.
	workersFinished := make(chan struct{}) // Closed when waitgroup is emptied.
	if cfg.ProgressLogPeriod > 0 {
		go func() {
			defer log.Log(rt.DEBUG, "Job queue monitor done")
			for {
				select {
				case <-time.After(cfg.ProgressLogPeriod):
					jobsNow := jobCount // Snapshot values to avoid incorrect-looking arithmetic if they change.
					doneNow := doneCount
					pendingNow := jobsNow - doneNow
					elapsed := time.Since(startTime)
					rate := float64(doneNow) / elapsed.Seconds()
					log.Log(rt.INFO, "%d jobs created, %d done, %d pending after %v (%.0f/s)",
						jobsNow, doneNow, pendingNow, elapsed, rate)
				case <-workersFinished:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Close result channel when workers are done sending to it.
	grp.Go(func() error {
		workerWg.Wait()
		close(jobResultCh)
		close(workersFinished)
		log.Log(rt.INFO, "All workers done after %v", time.Since(startTime))
		return nil
	})

	// building up a list of balance transfers.
	// this mutex will only get held like 30 times over a list of all actors, so it will have zero contention, but better safe than sorry!
	var balanceTransferListGuard = &sync.Mutex{}
	var balanceTransferList = list.New()
	// Insert migrated records in output state tree and accumulators.
	grp.Go(func() error {
		log.Log(rt.INFO, "Result writer started")
		resultCount := 0
		deletedActorCount := 0
		for result := range jobResultCh {
			if result.minerTypeMigrationShouldDelete {
				balanceTransferListGuard.Lock()
				balanceTransferList.PushBack(result.minerTypeMigrationBalanceTransferInfo)
				balanceTransferListGuard.Unlock()
				deletedActorCount++
			} else {
				if err := actorsOut.SetActor(result.address, &result.actor); err != nil {
					return err
				}
				resultCount++
			}
		}
		log.Log(rt.INFO, "Result writer wrote %d results to state tree and deleted %d actors after %v", resultCount, deletedActorCount, time.Since(startTime))
		return nil
	})

	if err := grp.Wait(); err != nil {
		return cid.Undef, err
	}

	// doing balance increments for owners of the deleted miners with test state tree types
	for e := balanceTransferList.Front(); e != nil; e = e.Next() {
		bTransfer := balanceTransferInfo(e.Value.(balanceTransferInfo))
		// check and make sure this is positive... just as a fun invariant, haha
		if !bTransfer.value.GreaterThanEqual(big.Zero()) {
			return cid.Undef, xerrors.Errorf("deleted test miner's balance was negative and we tried to send it to address %v", bTransfer.address)
		}
		incrementaddr := bTransfer.address
		actor, found, err := actorsOut.GetActor(bTransfer.address)
		if err != nil {
			return cid.Undef, err
		}
		// if you don't find the owner of the deleted miner, swap to sending funds to f099
		if !found {
			f099addr, err := address.NewFromString("f099")
			if err != nil {
				return cid.Undef, err
			}
			actor, found, err = actorsOut.GetActor(f099addr)
			incrementaddr = f099addr
			if err != nil {
				return cid.Undef, err
			}
			// if you don't find THAT one, you really messed up bad!
			if !found {
				return cid.Undef, xerrors.Errorf("could not find actor for the owner of the deleted miner, and then could not find f099 to send the funds to as a backup. something is very wrong here.")
			}
		}
		actor.Balance = big.Add(actor.Balance, bTransfer.value)
		err = actorsOut.SetActor(incrementaddr, actor)
		if err != nil {
			return cid.Undef, err
		}
	}

	elapsed := time.Since(startTime)
	rate := float64(doneCount) / elapsed.Seconds()
	log.Log(rt.INFO, "All %d done after %v (%.0f/s). Flushing state tree root.", doneCount, elapsed, rate)
	return actorsOut.Flush()
}

type actorMigrationInput struct {
	address    address.Address // actor's address
	balance    abi.TokenAmount // actor's balance
	head       cid.Cid         // actor's state head CID
	priorEpoch abi.ChainEpoch  // epoch of last state transition prior to migration
	cache      MigrationCache  // cache of existing cid -> cid migrations for this actor
}

type balanceTransferInfo struct {
	address address.Address
	value   big.Int
}

type actorMigrationResult struct {
	newCodeCID                            cid.Cid
	newHead                               cid.Cid
	minerTypeMigrationShouldDelete        bool
	minerTypeMigrationBalanceTransferInfo balanceTransferInfo
}

type actorMigration interface {
	// Loads an actor's state from an input store and writes new state to an output store.
	// Returns the new state head CID.
	migrateState(ctx context.Context, store cbor.IpldStore, input actorMigrationInput) (result *actorMigrationResult, err error)
	migratedCodeCID() cid.Cid
}

type migrationJob struct {
	address.Address
	states5.Actor
	actorMigration
	cache MigrationCache
}

type migrationJobResult struct {
	address                               address.Address
	actor                                 states6.Actor
	minerTypeMigrationShouldDelete        bool
	minerTypeMigrationBalanceTransferInfo balanceTransferInfo
}

func (job *migrationJob) run(ctx context.Context, store cbor.IpldStore, priorEpoch abi.ChainEpoch) (*migrationJobResult, error) {
	result, err := job.migrateState(ctx, store, actorMigrationInput{
		address:    job.Address,
		balance:    job.Actor.Balance,
		head:       job.Actor.Head,
		priorEpoch: priorEpoch,
		cache:      job.cache,
	})
	if err != nil {
		return nil, xerrors.Errorf("state migration failed for %s actor, addr %s: %w",
			builtin5.ActorNameByCode(job.Actor.Code), job.Address, err)
	}

	// Set up new actor record with the migrated state.
	// XXX: now how do i transfer any funds from miner to owner?
	// XXX: maybe add a TransferFrom field to this type to pass around transfers btwn actors
	// XXX: pair of transfer address and transfer amount
	// XXX: also a boolean for whether this miner should be deleted from the state tree
	// XXX: what is going on in power and market actors, also????
	//
	// XXX: to test: add one of each type of miner, maybe add some sectors, make a complex enough state and check some invariants???
	// XXX: give some some fees, give some no fees, etc, etc, etc
	// XXX: https://github.com/filecoin-project/specs-actors/blob/0fa32a654d910960306a0567d69f8d2ac1e66c67/actors/migration/nv4/top.go#L228
	return &migrationJobResult{
		minerTypeMigrationShouldDelete:        result.minerTypeMigrationShouldDelete,
		minerTypeMigrationBalanceTransferInfo: result.minerTypeMigrationBalanceTransferInfo,
		address:                               job.Address, // Unchanged
		actor: states6.Actor{
			Code:       result.newCodeCID,
			Head:       result.newHead,
			CallSeqNum: job.Actor.CallSeqNum, // Unchanged
			Balance:    job.Actor.Balance,    // Unchanged
		},
	}, nil
}

// Migrator which preserves the head CID and provides a fixed result code CID.
type nilMigrator struct {
	OutCodeCID cid.Cid
}

func (n nilMigrator) migrateState(_ context.Context, _ cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	return &actorMigrationResult{
		newCodeCID: n.OutCodeCID,
		newHead:    in.head,
	}, nil
}

func (n nilMigrator) migratedCodeCID() cid.Cid {
	return n.OutCodeCID
}

type cachedMigrator struct {
	cache MigrationCache
	actorMigration
}

func (c cachedMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	newHead, err := c.cache.Load(ActorHeadKey(in.address, in.head), func() (cid.Cid, error) {
		result, err := c.actorMigration.migrateState(ctx, store, in)
		if err != nil {
			return cid.Undef, err
		}
		return result.newHead, nil
	})
	if err != nil {
		return nil, err
	}
	return &actorMigrationResult{
		newCodeCID: c.migratedCodeCID(),
		newHead:    newHead,
	}, nil
}

func cachedMigration(cache MigrationCache, m actorMigration) actorMigration {
	return cachedMigrator{
		actorMigration: m,
		cache:          cache,
	}
}
