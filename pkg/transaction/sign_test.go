package transaction

import (
	"testing"

	"github.com/kinecosystem/go/build"
	"github.com/kinecosystem/go/keypair"
	"github.com/kinecosystem/go/xdr"
	"github.com/stretchr/testify/require"
)

func TestSign(t *testing.T) {
	sender, err := keypair.Random()
	require.NoError(t, err)
	whitelister, err := keypair.Random()
	require.NoError(t, err)

	var emptyAcc xdr.Uint256
	e := &xdr.TransactionEnvelope{
		Tx: xdr.Transaction{
			SourceAccount: xdr.AccountId{
				Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
				Ed25519: &emptyAcc,
			},
			Operations: []xdr.Operation{
				{
					Body: xdr.OperationBody{
						Type: xdr.OperationTypePayment,
						PaymentOp: &xdr.PaymentOp{
							Amount: 10,
							Destination: xdr.AccountId{
								Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
								Ed25519: &emptyAcc,
							},
						},
					},
				},
			},
		},
	}

	b := build.TransactionEnvelopeBuilder{E: e}
	require.NoError(t, b.MutateTX(build.TestNetwork))
	require.NoError(t, b.Mutate(build.Sign{Seed: sender.Seed()}))

	signed, err := SignEnvelope(b.E, build.TestNetwork, whitelister.Seed())
	require.NoError(t, err)
	require.Len(t, signed.Signatures, 2)

	signed, err = SignEnvelope(b.E, build.TestNetwork, "")
	require.Nil(t, signed)
	require.NotNil(t, err)
}
