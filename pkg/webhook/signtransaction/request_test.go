package signtransaction

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestRequest(t *testing.T) {
	invoiceList := &commonpb.InvoiceList{
		Invoices: []*commonpb.Invoice{
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "1-lineitem1",
						Description: "1-desc1",
						Amount:      5,
						Sku:         []byte("1-sku1"),
					},
					{
						Title:       "1-lineitem2",
						Description: "1-desc2",
						Amount:      10,
						Sku:         []byte("1-sku2"),
					},
					{
						Title: "1-lineitem3",
					},
				},
			},
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "2-lineitem1",
						Description: "2-desc1",
						Amount:      15,
						Sku:         []byte("2-sku1"),
					},
					{
						Title:       "2-lineitem2",
						Description: "2-desc2",
						Amount:      20,
						Sku:         []byte("2-sku2"),
					},
					{
						Title: "2-lineitem3",
					},
				},
			},
		},
	}

	subsidizer := testutil.GenerateSolanaKeys(t, 1)[0]
	tx := solana.NewTransaction(subsidizer)
	actual, err := CreateSolanaRequest(tx, invoiceList)
	require.NoError(t, err)
	assert.EqualValues(t, 4, actual.KinVersion)
	assert.EqualValues(t, tx.Marshal(), actual.SolanaTransaction)

	actualProtoIL := &commonpb.InvoiceList{}
	err = proto.Unmarshal(actual.InvoiceList, actualProtoIL)
	require.NoError(t, err)
	require.True(t, proto.Equal(invoiceList, actualProtoIL))
}
