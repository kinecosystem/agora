package client

import (
	"context"
	"crypto/sha256"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/go/xdr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"
)

type testServer struct {
	mu       sync.Mutex
	errors   []error
	accounts map[string]*accountpb.AccountInfo
	gets     map[string]transactionpb.GetTransactionResponse

	submits         []*transactionpb.SubmitTransactionRequest
	submitResponses []*transactionpb.SubmitTransactionResponse
}

func newTestServer() *testServer {
	return &testServer{
		accounts: make(map[string]*accountpb.AccountInfo),
		gets:     make(map[string]transactionpb.GetTransactionResponse),
	}
}

func (t *testServer) CreateAccount(ctx context.Context, req *accountpb.CreateAccountRequest) (*accountpb.CreateAccountResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := validateHeaders(ctx); err != nil {
		return nil, err
	}

	if err := t.getError(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if _, ok := t.accounts[req.AccountId.Value]; ok {
		return &accountpb.CreateAccountResponse{
			Result: accountpb.CreateAccountResponse_EXISTS,
		}, nil
	}

	t.accounts[req.AccountId.Value] = &accountpb.AccountInfo{
		AccountId:      proto.Clone(req.AccountId).(*commonpb.StellarAccountId),
		SequenceNumber: 1,
		Balance:        10,
	}

	return &accountpb.CreateAccountResponse{}, nil
}
func (t *testServer) GetAccountInfo(ctx context.Context, req *accountpb.GetAccountInfoRequest) (*accountpb.GetAccountInfoResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := validateHeaders(ctx); err != nil {
		return nil, err
	}

	accountInfo, ok := t.accounts[req.AccountId.Value]
	if !ok {
		return &accountpb.GetAccountInfoResponse{
			Result: accountpb.GetAccountInfoResponse_NOT_FOUND,
		}, nil
	}

	return &accountpb.GetAccountInfoResponse{
		AccountInfo: proto.Clone(accountInfo).(*accountpb.AccountInfo),
	}, nil
}
func (t *testServer) GetEvents(*accountpb.GetEventsRequest, accountpb.Account_GetEventsServer) error {
	return status.Error(codes.Unimplemented, "")
}

func (t *testServer) SubmitTransaction(ctx context.Context, req *transactionpb.SubmitTransactionRequest) (*transactionpb.SubmitTransactionResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := validateHeaders(ctx); err != nil {
		return nil, err
	}

	if err := t.getError(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var envelope xdr.TransactionEnvelope
	if err := envelope.UnmarshalBinary(req.EnvelopeXdr); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmarshal envelope: %v", err)
	}
	txBytes, err := envelope.Tx.MarshalBinary()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal tx: %v", err)
	}
	txHash := sha256.Sum256(txBytes)

	t.submits = append(t.submits, proto.Clone(req).(*transactionpb.SubmitTransactionRequest))
	if len(t.submitResponses) > 0 {
		r := t.submitResponses[0]
		t.submitResponses = t.submitResponses[1:]
		if r != nil {
			r.Hash = &commonpb.TransactionHash{
				Value: txHash[:],
			}
			return r, nil
		}
	}

	// Update the sequence number
	info := t.accounts[envelope.Tx.SourceAccount.Address()]
	if info == nil {
		info = &accountpb.AccountInfo{
			AccountId: &commonpb.StellarAccountId{
				Value: envelope.Tx.SourceAccount.Address(),
			},
		}
	}
	info.SequenceNumber++
	t.accounts[envelope.Tx.SourceAccount.Address()] = info

	resultXDR := xdr.TransactionResult{
		Result: xdr.TransactionResultResult{
			Code:    xdr.TransactionResultCodeTxSuccess,
			Results: &[]xdr.OperationResult{},
		},
	}
	resultBytes, err := resultXDR.MarshalBinary()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal result: %v", err)
	}

	return &transactionpb.SubmitTransactionResponse{
		Hash: &commonpb.TransactionHash{
			Value: txHash[:],
		},
		Ledger:    1,
		ResultXdr: resultBytes,
	}, nil
}

func (t *testServer) GetTransaction(ctx context.Context, req *transactionpb.GetTransactionRequest) (*transactionpb.GetTransactionResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := validateHeaders(ctx); err != nil {
		return nil, err
	}

	if err := t.getError(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if resp, ok := t.gets[string(req.TransactionHash.Value)]; ok {
		return &resp, nil
	}

	return &transactionpb.GetTransactionResponse{
		State: transactionpb.GetTransactionResponse_UNKNOWN,
	}, nil
}

func (t *testServer) GetHistory(context.Context, *transactionpb.GetHistoryRequest) (*transactionpb.GetHistoryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (t *testServer) setError(err error, n int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.errors = make([]error, n)
	for i := 0; i < n; i++ {
		t.errors[i] = err
	}
}

func (t *testServer) getError() error {
	if len(t.errors) == 0 {
		return nil
	}

	err := t.errors[0]
	t.errors = t.errors[1:]

	return err
}

func validateHeaders(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Internal, "failed to parse metadata")
	}

	vals := md.Get(userAgentHeader)
	for _, v := range vals {
		if v == userAgent {
			return nil
		}
	}

	return status.Error(codes.InvalidArgument, "missing user-agent")
}
