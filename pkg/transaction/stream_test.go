package transaction

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/support/http/httptest"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Copied from github.com/stellar/go/clients/horizonclient/transaction_request_test.go
var txStreamResponse = `data: {"_links":{"self":{"href":"https://horizon-testnet.stellar.org/transactions/1534f6507420c6871b557cc2fc800c29fb1ed1e012e694993ffe7a39c824056e"},"account":{"href":"https://horizon-testnet.stellar.org/accounts/GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"},"ledger":{"href":"https://horizon-testnet.stellar.org/ledgers/607387"},"operations":{"href":"https://horizon-testnet.stellar.org/transactions/1534f6507420c6871b557cc2fc800c29fb1ed1e012e694993ffe7a39c824056e/operations{?cursor,limit,order}","templated":true},"effects":{"href":"https://horizon-testnet.stellar.org/transactions/1534f6507420c6871b557cc2fc800c29fb1ed1e012e694993ffe7a39c824056e/effects{?cursor,limit,order}","templated":true},"precedes":{"href":"https://horizon-testnet.stellar.org/transactions?order=asc\u0026cursor=2608707301036032"},"succeeds":{"href":"https://horizon-testnet.stellar.org/transactions?order=desc\u0026cursor=2608707301036032"}},"id":"1534f6507420c6871b557cc2fc800c29fb1ed1e012e694993ffe7a39c824056e","paging_token":"2608707301036032","successful":true,"hash":"1534f6507420c6871b557cc2fc800c29fb1ed1e012e694993ffe7a39c824056e","ledger":607387,"created_at":"2019-04-04T12:07:03Z","source_account":"GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR","source_account_sequence":"4660039930473","max_fee":100,"fee_charged":100,"operation_count":1,"envelope_xdr":"AAAAABB90WssODNIgi6BHveqzxTRmIpvAFRyVNM+Hm2GVuCcAAAAZAAABD0ABlJpAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAmLuzasXDMqsqgFK4xkbLxJLzmQQzkiCF2SnKPD+b1TsAAAAXSHboAAAAAAAAAAABhlbgnAAAAECqxhXduvtzs65keKuTzMtk76cts2WeVB2pZKYdlxlOb1EIbOpFhYizDSXVfQlAvvg18qV6oNRr7ls4nnEm2YIK","result_xdr":"AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAA=","result_meta_xdr":"AAAAAQAAAAIAAAADAAlEmwAAAAAAAAAAEH3Rayw4M0iCLoEe96rPFNGYim8AVHJU0z4ebYZW4JwBT3aiixBA2AAABD0ABlJoAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAlEmwAAAAAAAAAAEH3Rayw4M0iCLoEe96rPFNGYim8AVHJU0z4ebYZW4JwBT3aiixBA2AAABD0ABlJpAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAAAAwAAAAMACUSbAAAAAAAAAAAQfdFrLDgzSIIugR73qs8U0ZiKbwBUclTTPh5thlbgnAFPdqKLEEDYAAAEPQAGUmkAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEACUSbAAAAAAAAAAAQfdFrLDgzSIIugR73qs8U0ZiKbwBUclTTPh5thlbgnAFPdotCmVjYAAAEPQAGUmkAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAACUSbAAAAAAAAAACYu7NqxcMyqyqAUrjGRsvEkvOZBDOSIIXZKco8P5vVOwAAABdIdugAAAlEmwAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA==","fee_meta_xdr":"AAAAAgAAAAMACUSaAAAAAAAAAAAQfdFrLDgzSIIugR73qs8U0ZiKbwBUclTTPh5thlbgnAFPdqKLEEE8AAAEPQAGUmgAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEACUSbAAAAAAAAAAAQfdFrLDgzSIIugR73qs8U0ZiKbwBUclTTPh5thlbgnAFPdqKLEEDYAAAEPQAGUmgAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA==","memo_type":"none","signatures":["qsYV3br7c7OuZHirk8zLZO+nLbNlnlQdqWSmHZcZTm9RCGzqRYWIsw0l1X0JQL74NfKleqDUa+5bOJ5xJtmCCg=="]}
`

func TestOpenTransactionStream(t *testing.T) {
	hmock := httptest.NewClient()
	client := &horizonclient.Client{
		HorizonURL: "https://localhost/",
		HTTP:       hmock,
	}
	hmock.On(
		"GET",
		"https://localhost/transactions?cursor=now&order=asc",
	).ReturnString(200, txStreamResponse)

	ctx, cancel := context.WithCancel(context.Background())
	notifier := newTestNotifier(3, cancel)

	go func() {
		StreamTransactions(ctx, client, notifier)
	}()

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
	}

	assert.Equal(t, 3, len(notifier.receivedTxns))
	for _, data := range notifier.receivedTxns {
		actualAcc, err := data.e.Tx.SourceAccount.GetAddress()
		require.NoError(t, err)

		require.Equal(t, "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR", actualAcc)
		require.Equal(t, 100, int(data.e.Tx.Fee))
		require.Equal(t, 4660039930473, int(data.e.Tx.SeqNum))

		require.Equal(t, int32(1), data.m.V)
		require.Equal(t, 2, len(data.m.V1.TxChanges))
		require.Equal(t, 1, len(data.m.OperationsMeta()))
	}
}

type transactionData struct {
	e xdr.TransactionEnvelope
	m xdr.TransactionMeta
}

// testNotifier will call the provided CancelFunc after receiving the specified number of transactions.
type testNotifier struct {
	sync.Mutex
	receivedTxns []transactionData
	count        int
	cancel       context.CancelFunc
}

func newTestNotifier(count int, cancel context.CancelFunc) *testNotifier {
	return &testNotifier{
		count:  count,
		cancel: cancel,
	}
}

func (n *testNotifier) OnTransaction(e xdr.TransactionEnvelope, m xdr.TransactionMeta) {
	n.Lock()
	n.receivedTxns = append(n.receivedTxns, transactionData{
		e: e,
		m: m,
	})
	if len(n.receivedTxns) == n.count {
		n.cancel()
	}
	n.Unlock()
}
