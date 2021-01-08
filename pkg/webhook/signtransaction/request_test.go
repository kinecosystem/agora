package signtransaction

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora-common/kin/version"
)

func TestRequest(t *testing.T) {
	envelopeXDR := []byte("somedata")
	req := &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeXDR,
		InvoiceList: &commonpb.InvoiceList{
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
		},
	}

	actual, err := CreateStellarRequest(version.KinVersion3, req)
	require.NoError(t, err)
	assert.EqualValues(t, 3, actual.KinVersion)
	assert.Equal(t, envelopeXDR, actual.EnvelopeXDR)

	actualProtoIL := &commonpb.InvoiceList{}
	err = proto.Unmarshal(actual.InvoiceList, actualProtoIL)
	require.NoError(t, err)
	require.True(t, proto.Equal(req.InvoiceList, actualProtoIL))
}
