package testserver

import (
	"bytes"
	"context"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-api/genproto/airdrop/v4"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/mr-tron/base58"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	accountpbv4 "github.com/kinecosystem/agora-api/genproto/account/v4"
	airdroppbv4 "github.com/kinecosystem/agora-api/genproto/airdrop/v4"
	commonpbv4 "github.com/kinecosystem/agora-api/genproto/common/v4"
	transactionpbv4 "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/version"
)

var RecentBlockhash = bytes.Repeat([]byte{1}, 32)
var MinBalanceForRentException = uint64(1234567)
var MaxAirdrop = uint64(100000)

type V4Server struct {
	Mux    sync.Mutex
	Errors []error

	Creates       []*accountpbv4.CreateAccountRequest
	Accounts      map[string]*accountpbv4.AccountInfo
	TokenAccounts map[string][]*commonpbv4.SolanaAccountId

	ServiceConfigReqs []*transactionpbv4.GetServiceConfigRequest
	ServiceConfig     *transactionpbv4.GetServiceConfigResponse

	Gets            map[string]transactionpbv4.GetTransactionResponse
	Submits         []*transactionpbv4.SubmitTransactionRequest
	SubmitResponses []*transactionpbv4.SubmitTransactionResponse
}

func NewV4Server() *V4Server {
	return &V4Server{
		Accounts:      make(map[string]*accountpbv4.AccountInfo),
		TokenAccounts: make(map[string][]*commonpbv4.SolanaAccountId),
		Gets:          make(map[string]transactionpbv4.GetTransactionResponse),
	}
}

func (t *V4Server) CreateAccount(ctx context.Context, req *accountpbv4.CreateAccountRequest) (*accountpbv4.CreateAccountResponse, error) {
	t.Mux.Lock()
	defer t.Mux.Unlock()

	if err := validateV4Headers(ctx); err != nil {
		return nil, err
	}

	if err := t.GetError(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var tx solana.Transaction
	if err := tx.Unmarshal(req.Transaction.Value); err != nil {
		return nil, status.Error(codes.InvalidArgument, "bad transaction encoding")
	}

	t.Creates = append(t.Creates, proto.Clone(req).(*accountpbv4.CreateAccountRequest))

	sysCreate, err := system.DecompileCreateAccount(tx.Message, 0)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Sys::CreateAccount instruction")
	}

	accountID := base58.Encode(sysCreate.Address)
	if info, ok := t.Accounts[accountID]; ok {
		return &accountpbv4.CreateAccountResponse{
			Result:      accountpbv4.CreateAccountResponse_EXISTS,
			AccountInfo: proto.Clone(info).(*accountpbv4.AccountInfo),
		}, nil
	}

	accountInfo := &accountpbv4.AccountInfo{
		AccountId: &commonpbv4.SolanaAccountId{Value: sysCreate.Address},
		Balance:   10,
	}
	t.Accounts[accountID] = accountInfo
	return &accountpbv4.CreateAccountResponse{
		AccountInfo: accountInfo,
	}, nil
}

func (t *V4Server) GetAccountInfo(ctx context.Context, req *accountpbv4.GetAccountInfoRequest) (*accountpbv4.GetAccountInfoResponse, error) {
	t.Mux.Lock()
	defer t.Mux.Unlock()

	if err := validateV4Headers(ctx); err != nil {
		return nil, err
	}

	accountInfo, ok := t.Accounts[base58.Encode(req.AccountId.Value)]
	if !ok {
		return &accountpbv4.GetAccountInfoResponse{
			Result: accountpbv4.GetAccountInfoResponse_NOT_FOUND,
		}, nil
	}

	return &accountpbv4.GetAccountInfoResponse{
		AccountInfo: proto.Clone(accountInfo).(*accountpbv4.AccountInfo),
	}, nil
}

func (t *V4Server) ResolveTokenAccounts(ctx context.Context, req *accountpbv4.ResolveTokenAccountsRequest) (*accountpbv4.ResolveTokenAccountsResponse, error) {
	t.Mux.Lock()
	defer t.Mux.Unlock()

	if err := validateV4Headers(ctx); err != nil {
		return nil, err
	}

	accounts, ok := t.TokenAccounts[base58.Encode(req.AccountId.Value)]
	if !ok {
		return &accountpbv4.ResolveTokenAccountsResponse{}, nil
	}

	tokenAccounts := make([]*commonpbv4.SolanaAccountId, len(accounts))
	for i, a := range accounts {
		tokenAccounts[i] = proto.Clone(a).(*commonpbv4.SolanaAccountId)
	}
	return &accountpbv4.ResolveTokenAccountsResponse{
		TokenAccounts: tokenAccounts,
	}, nil
}

func (t *V4Server) GetEvents(*accountpbv4.GetEventsRequest, accountpbv4.Account_GetEventsServer) error {
	return status.Error(codes.Unimplemented, "")
}

func (t *V4Server) GetServiceConfig(ctx context.Context, req *transactionpbv4.GetServiceConfigRequest) (*transactionpbv4.GetServiceConfigResponse, error) {
	t.Mux.Lock()
	defer t.Mux.Unlock()

	if err := validateV4Headers(ctx); err != nil {
		return nil, err
	}

	t.ServiceConfigReqs = append(t.ServiceConfigReqs, req)
	return t.ServiceConfig, nil
}

func (t *V4Server) GetMinimumKinVersion(ctx context.Context, req *transactionpbv4.GetMinimumKinVersionRequest) (*transactionpbv4.GetMinimumKinVersionResponse, error) {
	if err := validateV4Headers(ctx); err != nil {
		return nil, err
	}

	desiredKinVersion, err := version.GetCtxDesiredVersion(ctx)
	if err == nil {
		return &transactionpbv4.GetMinimumKinVersionResponse{Version: uint32(desiredKinVersion)}, nil
	}

	return &transactionpbv4.GetMinimumKinVersionResponse{Version: 3}, nil
}

func (t *V4Server) GetRecentBlockhash(ctx context.Context, req *transactionpbv4.GetRecentBlockhashRequest) (*transactionpbv4.GetRecentBlockhashResponse, error) {
	if err := validateV4Headers(ctx); err != nil {
		return nil, err
	}

	return &transactionpbv4.GetRecentBlockhashResponse{Blockhash: &commonpbv4.Blockhash{Value: RecentBlockhash}}, nil
}

func (t *V4Server) GetMinimumBalanceForRentExemption(ctx context.Context, req *transactionpbv4.GetMinimumBalanceForRentExemptionRequest) (*transactionpbv4.GetMinimumBalanceForRentExemptionResponse, error) {
	if err := validateV4Headers(ctx); err != nil {
		return nil, err
	}

	return &transactionpbv4.GetMinimumBalanceForRentExemptionResponse{Lamports: MinBalanceForRentException}, nil
}

func (t *V4Server) GetHistory(context.Context, *transactionpbv4.GetHistoryRequest) (*transactionpbv4.GetHistoryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (t *V4Server) SubmitTransaction(ctx context.Context, req *transactionpbv4.SubmitTransactionRequest) (*transactionpbv4.SubmitTransactionResponse, error) {
	t.Mux.Lock()
	defer t.Mux.Unlock()

	if err := validateV4Headers(ctx); err != nil {
		return nil, err
	}

	if err := t.GetError(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	tx := solana.Transaction{}
	if err := tx.Unmarshal(req.Transaction.Value); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmarshal tx: %v", err)
	}

	t.Submits = append(t.Submits, proto.Clone(req).(*transactionpbv4.SubmitTransactionRequest))
	if len(t.SubmitResponses) > 0 {
		r := t.SubmitResponses[0]
		t.SubmitResponses = t.SubmitResponses[1:]
		if r != nil {
			r.Signature = &commonpbv4.TransactionSignature{
				Value: tx.Signature(),
			}
			return r, nil
		}
	}

	return &transactionpbv4.SubmitTransactionResponse{
		Signature: &commonpbv4.TransactionSignature{
			Value: tx.Signature(),
		},
	}, nil
}

func (t *V4Server) GetTransaction(ctx context.Context, req *transactionpbv4.GetTransactionRequest) (*transactionpbv4.GetTransactionResponse, error) {
	t.Mux.Lock()
	defer t.Mux.Unlock()

	if err := validateV4Headers(ctx); err != nil {
		return nil, err
	}

	if err := t.GetError(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if resp, ok := t.Gets[string(req.TransactionId.Value)]; ok {
		return &resp, nil
	}

	return &transactionpbv4.GetTransactionResponse{
		State: transactionpbv4.GetTransactionResponse_UNKNOWN,
	}, nil
}

func (t *V4Server) RequestAirdrop(ctx context.Context, req *airdrop.RequestAirdropRequest) (*airdrop.RequestAirdropResponse, error) {
	t.Mux.Lock()
	defer t.Mux.Unlock()

	if err := validateV4Headers(ctx); err != nil {
		return nil, err
	}

	if err := t.GetError(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	_, ok := t.Accounts[base58.Encode(req.AccountId.Value)]
	if !ok {
		return &airdroppbv4.RequestAirdropResponse{Result: airdroppbv4.RequestAirdropResponse_NOT_FOUND}, nil
	}

	if req.Quarks > MaxAirdrop {
		return &airdroppbv4.RequestAirdropResponse{Result: airdroppbv4.RequestAirdropResponse_INSUFFICIENT_KIN}, nil
	}

	return &airdroppbv4.RequestAirdropResponse{
		Signature: &commonpbv4.TransactionSignature{
			Value: make([]byte, 64),
		},
	}, nil
}

func (t *V4Server) SetError(err error, n int) {
	t.Mux.Lock()
	defer t.Mux.Unlock()

	t.Errors = make([]error, n)
	for i := 0; i < n; i++ {
		t.Errors[i] = err
	}
}

func (t *V4Server) GetError() error {
	if len(t.Errors) == 0 {
		return nil
	}

	err := t.Errors[0]
	t.Errors = t.Errors[1:]

	return err
}

func validateV4Headers(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Internal, "failed to parse metadata")
	}

	vals := md.Get("kin-user-agent")
	for _, v := range vals {
		if strings.Contains(v, "KinSDK") {
			return nil
		}
	}

	return status.Error(codes.InvalidArgument, "missing user-agent")
}
