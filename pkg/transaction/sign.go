package transaction

import (
	"github.com/kinecosystem/go/build"
	"github.com/kinecosystem/go/xdr"
)

// SignEnvelope whitelists a transaction envelope with the specified seed and network passphrase.
func SignEnvelope(envelope *xdr.TransactionEnvelope, network build.Network, seed string) (*xdr.TransactionEnvelope, error) {
	builder := build.TransactionEnvelopeBuilder{E: envelope}

	if err := builder.MutateTX(network); err != nil {
		return nil, err
	}

	if err := builder.Mutate(build.Sign{Seed: seed}); err != nil {
		return nil, err
	}

	return builder.E, nil
}
