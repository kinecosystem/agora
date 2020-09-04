package server

import (
	"strconv"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/amount"
	hProtocol "github.com/kinecosystem/go/protocols/horizon"
	"github.com/stellar/go/xdr"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
)

func parseAccountInfo(horizonAccount hProtocol.Account) (info *accountpb.AccountInfo, err error) {
	strBalance, err := horizonAccount.GetNativeBalance()
	if err != nil {
		return nil, err
	}

	balance, err := amount.ParseInt64(strBalance)
	if err != nil {
		return nil, err
	}

	sequence, err := strconv.ParseInt(horizonAccount.Sequence, 10, 64)
	if err != nil {
		return nil, err
	}

	return &accountpb.AccountInfo{
		AccountId:      &commonpb.StellarAccountId{Value: horizonAccount.ID},
		SequenceNumber: sequence,
		Balance:        balance,
	}, nil
}

func parseKin2AccountInfo(horizonAccount hProtocol.Account, issuer string) (info *accountpb.AccountInfo, err error) {
	strBalance := horizonAccount.GetCreditBalance(kin.KinAssetCode, issuer)

	balance, err := amount.ParseInt64(strBalance)
	if err != nil {
		return nil, err
	}

	sequence, err := strconv.ParseInt(horizonAccount.Sequence, 10, 64)
	if err != nil {
		return nil, err
	}

	return &accountpb.AccountInfo{
		AccountId:      &commonpb.StellarAccountId{Value: horizonAccount.ID},
		SequenceNumber: sequence,
		Balance:        balance / 100, // the smallest amount on Kin 2 is 1e-7 instead of 1e-5, so we convert
	}, nil
}

func parseAccountInfoFromEntry(entry xdr.AccountEntry) (*accountpb.AccountInfo, error) {
	addr, err := entry.AccountId.GetAddress()
	if err != nil {
		return nil, err
	}

	return &accountpb.AccountInfo{
		AccountId:      &commonpb.StellarAccountId{Value: addr},
		SequenceNumber: int64(entry.SeqNum),
		Balance:        int64(entry.Balance),
	}, nil
}
