package solana

import (
	"context"

	"github.com/kinecosystem/agora-api/genproto/transaction/v4"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type noopServer struct{}

func NewNoopServer() transactionpb.TransactionServer {
	return &noopServer{}
}

func (s *noopServer) GetServiceConfig(_ context.Context, _ *transaction.GetServiceConfigRequest) (*transaction.GetServiceConfigResponse, error) {
	return nil, status.Error(codes.Unimplemented, "kin4 is not enabled")
}

func (s *noopServer) GetMinimumKinVersion(_ context.Context, _ *transaction.GetMinimumKinVersionRequest) (*transaction.GetMinimumKinVersionResponse, error) {
	return &transactionpb.GetMinimumKinVersionResponse{
		Version: 3,
	}, nil
}

func (s *noopServer) GetRecentBlockhash(_ context.Context, _ *transaction.GetRecentBlockhashRequest) (*transaction.GetRecentBlockhashResponse, error) {
	return nil, status.Error(codes.Unimplemented, "kin4 is not enabled")
}

func (s *noopServer) GetMinimumBalanceForRentExemption(_ context.Context, _ *transaction.GetMinimumBalanceForRentExemptionRequest) (*transaction.GetMinimumBalanceForRentExemptionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "kin4 is not enabled")
}

func (s *noopServer) GetHistory(_ context.Context, _ *transaction.GetHistoryRequest) (*transaction.GetHistoryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "kin4 is not enabled")
}

func (s *noopServer) SubmitTransaction(_ context.Context, _ *transaction.SubmitTransactionRequest) (*transaction.SubmitTransactionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "kin4 is not enabled")
}

func (s *noopServer) GetTransaction(_ context.Context, _ *transaction.GetTransactionRequest) (*transaction.GetTransactionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "kin4 is not enabled")
}
