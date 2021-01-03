package solana

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"math"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type ingestor struct {
	log         *logrus.Entry
	name        string
	client      solana.Client
	tokenClient *token.Client
}

func New(name string, client solana.Client, t ed25519.PublicKey) ingestion.Ingestor {
	return &ingestor{
		log:         logrus.StandardLogger().WithField("type", "transaction/history/ingestion/solana"),
		name:        name,
		client:      client,
		tokenClient: token.NewClient(client, t),
	}
}

// Name implements ingestion.Ingestor.Name.
func (i *ingestor) Name() string {
	return i.name
}

// Ingest implements ingestion.Ingestor.Ingest.
func (i *ingestor) Ingest(ctx context.Context, w history.Writer, parent ingestion.Pointer) (ingestion.ResultQueue, error) {
	parentSlot, err := SlotFromPointer(parent)
	if err != nil {
		return nil, err
	}
	start := parentSlot + 1

	// todo(config): allow for a customizable buffer?
	queue := make(chan (<-chan ingestion.Result), 16)

	go func() {
		defer close(queue)

		_, err := retry.Retry(
			func() error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					// We can request a fairly aggressive range here, as to reduce
					// the number of requests to the RPC node.
					//
					// Note: the number here is currently arbitrary. The real limiter
					// is the number of concurrent processors (which perform an rpc call
					// for each block), which impacts the solana RPC node.
					blocks, err := i.client.GetConfirmedBlocksWithLimit(start, 1024)
					if err != nil {
						i.log.WithError(err).Info("failed to get confirmed blocks")
						return err
					}

					i.log.WithField("block_count", len(blocks)).Debug("processing blocks")

					for _, slot := range blocks {
						blockPtr := PointerFromSlot(slot)
						resultCh := make(chan ingestion.Result, 1)

						select {
						case queue <- resultCh:
						case <-ctx.Done():
							return ctx.Err()
						}

						result := ingestion.Result{
							Parent: parent,
							Block:  blockPtr,
						}
						parent = blockPtr

						go func(slot uint64) {
							if err := i.processSlot(slot, w); err != nil {
								result.Err = err
							}

							resultCh <- result
							close(resultCh)
						}(slot)

						i.log.WithField("slot", slot).Trace("processing slot")
						start = slot + 1
					}

					if len(blocks) == 0 {
						// todo(config): maybe this should be configurable? currently
						//               we ensure it's no faster than a second to alleviate load.
						time.Sleep(time.Duration(math.Max(float64(solana.PollRate), float64(time.Second))))
					}
				}
			},
			retry.NonRetriableErrors(context.Canceled),
			retry.BackoffWithJitter(backoff.BinaryExponential(time.Second), 30*time.Second, 0.1),
		)
		i.log.WithError(err).Info("ingestion stream closed")
	}()

	return queue, nil
}

func (i *ingestor) processSlot(slot uint64, w history.Writer) error {
	block, err := i.client.GetConfirmedBlock(slot)
	if err != nil {
		// todo: wtf? why was this return nil...
		return errors.Wrapf(err, "failed to get confirmed block")
	}

	// Not every slot has a block, so if we get no error, an empty
	// block is considered valid.
	//
	// todo(metrics): add meter here. should be close to zero in test/prod
	if block == nil {
		return nil
	}

	blockTime, err := i.client.GetBlockTime(slot)
	if err != nil {
		// Note: even in the solana.ErrBlockNotAvailable case, we _should_
		//       always have it available. It being not available indicates the
		//       underlying RPC node should be fixed.
		return errors.Wrap(err, "failed to get block time")
	}

	ts, err := ptypes.TimestampProto(blockTime)
	if err != nil {
		return errors.Wrap(err, "failed to marshal block time")
	}

	type shouldProcessFunc func(solana.BlockTransaction, int) (bool, error)
	checks := []shouldProcessFunc{
		i.containsInitialize,
		i.containsTransfer,
		i.containsSetAuthority,
	}

	for _, txn := range block.Transactions {
		for instr := range txn.Transaction.Message.Instructions {
			shouldProcess := false
			for _, check := range checks {
				shouldProcess, err = check(txn, instr)
				if err != nil {
					return err
				}
				if shouldProcess {
					break
				}
			}

			if !shouldProcess {
				continue
			}

			var txnErr []byte
			if txn.Err != nil {
				raw, err := txn.Err.JSONString()
				if err != nil {
					return errors.Wrap(err, "failed to marshal transaction error")
				}
				txnErr = []byte(raw)
			}

			entry := &model.Entry{
				Version: model.KinVersion_KIN4,
				Kind: &model.Entry_Solana{
					Solana: &model.SolanaEntry{
						Slot:             slot,
						Confirmed:        true,
						BlockTime:        ts,
						Transaction:      txn.Transaction.Marshal(),
						TransactionError: txnErr,
					},
				},
			}

			if err := w.Write(context.Background(), entry); err != nil {
				return errors.Wrap(err, "failed to write txn")
			}
		}
	}

	return nil
}

func (i *ingestor) containsInitialize(txn solana.BlockTransaction, index int) (bool, error) {
	decompiled, err := token.DecompileInitializeAccount(txn.Transaction.Message, index)
	if err != nil {
		return false, nil
	}

	if !bytes.Equal(decompiled.Mint, i.tokenClient.Token()) {
		return false, nil
	}

	return true, nil
}

func (i *ingestor) containsSetAuthority(txn solana.BlockTransaction, index int) (bool, error) {
	decompiled, err := token.DecompileSetAuthority(txn.Transaction.Message, index)
	if err != nil {
		return false, nil
	}

	info, err := i.tokenClient.GetAccount(decompiled.Account, solana.CommitmentSingle)
	if err == token.ErrInvalidTokenAccount {
		// The source account is either not a token account, or it's not for
		// our configured mint
		return false, nil
	} else if err != nil {
		// If we cannot retrieve the source account, _and_ there's a transaction failure,
		// then it is likely (but not guaranteed) that the transaction failed because the
		// of a bad 'currentAuthority' signature (i.e. someone's trying to steal).
		//
		// If, on the other hand, there is _no_ transaction error, then the we should be
		// able to retrieve the account info, as the it was referenced in a successful
		// token transfer instruction.
		//
		// In either case, we don't really have enough information to infer which mint
		// the transfer was for. We can only infer whether or not it was an RPC error, or
		// a transaction error.
		if txn.Err == nil {
			return false, errors.Wrapf(err, "failed to get account for non-failed transaction (set authority instruction) (account: %s)", base58.Encode(decompiled.Account))
		}

		return false, nil
	}

	if !bytes.Equal(info.Mint, i.tokenClient.Token()) {
		return false, nil
	}

	return true, nil
}

func (i *ingestor) containsTransfer(txn solana.BlockTransaction, index int) (bool, error) {
	decompiled, err := token.DecompileTransferAccount(txn.Transaction.Message, index)
	if err != nil {
		return false, nil
	}

	sourceInfo, err := i.tokenClient.GetAccount(decompiled.Source, solana.CommitmentSingle)
	if err == nil {
		if !bytes.Equal(sourceInfo.Mint, i.tokenClient.Token()) {
			return false, nil
		}

		return true, nil
	}

	// The source is clearly not a Kin token, we so ignore it.
	if err == token.ErrInvalidTokenAccount {
		return false, nil
	}

	// If the transaction failed, we don't really care enough to recover this information.
	if txn.Err != nil {
		return false, nil
	}

	destInfo, err := i.tokenClient.GetAccount(decompiled.Destination, solana.CommitmentSingle)
	if err == nil {
		if !bytes.Equal(destInfo.Mint, i.tokenClient.Token()) {
			return false, nil
		}

		return true, nil
	}

	// The dest is clearly not a Kin token, we so ignore it.
	if err == token.ErrInvalidTokenAccount {
		return false, nil
	}

	// We don't have any information about either account. This is likely the case
	// if both accounts were deleted before we were able to process the transaction.
	//
	// This can occur if the history system is lagging significantly behind. To avoid
	// loss, we process it anyway. This can create garbage, but won't polute the server
	// responses. On KRE ingestion, we can look for the InitializeAccount() instruction
	// to verify.
	return true, nil
}
