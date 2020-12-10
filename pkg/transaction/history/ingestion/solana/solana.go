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
	parentSlot, err := slotFromPointer(parent)
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
						return err
					}

					for _, slot := range blocks {
						blockPtr := pointerFromSlot(slot)
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
			return false, errors.Wrap(err, "failed to get account for non-failed transaction")
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

	info, err := i.tokenClient.GetAccount(decompiled.Source, solana.CommitmentSingle)
	if err == token.ErrInvalidTokenAccount {
		// The source account is either not a token account, or it's not for
		// our configured mint
		return false, nil
	} else if err != nil {
		// If we cannot retrieve the source account, _and_ there's a transaction failure,
		// then it is likely (but not guaranteed) that the transaction failed because the
		// source does not exist.
		//
		// If, on the other hand, there is _no_ transaction error, then the we should be
		// able to retrieve the account info, as the it was referenced in a successful
		// token transfer instruction.
		//
		// In either case, we don't really have enough information to infer which mint
		// the transfer was for. We can only infer whether or not it was an RPC error, or
		// a transaction error.
		//
		// Note: we could likely check the destination account as well, but it is generally
		// more likely that the source will be ok (rather than the dest), and we want to
		// avoid unnecessary API calls. The side effect here is that the failed transaction
		// will not be stored in our history.
		if txn.Err == nil {
			return false, errors.Wrap(err, "failed to get account for non-failed transaction")
		}

		return false, nil
	}

	if !bytes.Equal(info.Mint, i.tokenClient.Token()) {
		return false, nil
	}

	return true, nil
}
