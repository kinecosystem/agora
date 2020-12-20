package dedupe

import (
	"context"
	"time"

	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v4"
)

type Info struct {
	// The transaction signature.
	//
	// Note: currently unused, but kept just in case we want further
	//       debugging later.
	Signature []byte

	// If the transaction was successful, or final in a
	// 'positive' way (dupe sig, already submitted, etc),
	// then this is set.
	Response *transactionpb.SubmitTransactionResponse

	// Time of the submission
	SubmissionTime time.Time
}

// Deduper allows for the de-duplication of transactions at a higher level
// based on a de-dupe id.
//
// An empty/nil id should be handled gracefully by implementations, acting as
// if there was no existing info for the identifier.
type Deduper interface {
	// Dedupe attempts to attach the specified info to the provided id.
	//
	// If the id has already been claimed, the previous entry is returned.
	Dedupe(ctx context.Context, id []byte, info *Info) (prev *Info, err error)

	// Update sets the info for an id, regardless if there's state there.
	Update(ctx context.Context, id []byte, info *Info) error

	// Delete deletes the info for an id.
	//
	// Deletes are idempotent.
	Delete(ctx context.Context, id []byte) error
}
