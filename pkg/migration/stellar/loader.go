package stellar

import (
	"crypto/ed25519"
	"net/http"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/amount"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/strkey"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/migration"
)

// compositeLoader loads the account information first from the kin3 chain,
// falling back to the kin2 chain.
type compositeLoader struct {
	kin2 migration.Loader
	kin3 migration.Loader
}

// NewCompositeLoader returns a loader that retrieves information from kin3,
// falling back to kin2 if no account exists.
func NewCompositeLoader(kin2, kin3 migration.Loader) migration.Loader {
	return &compositeLoader{
		kin2: kin2,
		kin3: kin3,
	}
}

// LoadAccount implements migration.Loader.Load.
func (l *compositeLoader) LoadAccount(account ed25519.PublicKey) (owner ed25519.PublicKey, balance uint64, err error) {
	owner, balance, err = l.kin3.LoadAccount(account)
	switch err {
	case nil, migration.ErrMultisig, migration.ErrBurned:
		return owner, balance, err
	case migration.ErrNotFound:
	default:
		return owner, balance, errors.Wrap(err, "failed to check kin3 balance")
	}

	return l.kin2.LoadAccount(account)
}

type kin2Loader struct {
	hc         horizon.ClientInterface
	kin2Issuer string
}

func NewKin2Loader(hc horizon.ClientInterface, kin2Issuer string) migration.Loader {
	return &kin2Loader{
		hc:         hc,
		kin2Issuer: kin2Issuer,
	}
}

// LoadAccount implements migration.Loader.Load.
func (k *kin2Loader) LoadAccount(account ed25519.PublicKey) (owner ed25519.PublicKey, balance uint64, err error) {
	address, err := strkey.Encode(strkey.VersionByteAccountID, account)
	if err != nil {
		return owner, balance, errors.Wrap(err, "failed to encode account as stellar address")
	}

	stellarAccount, err := k.hc.LoadAccount(address)
	if err != nil {
		if hErr, ok := err.(*horizon.Error); ok {
			switch hErr.Problem.Status {
			case http.StatusNotFound:
				return owner, balance, migration.ErrNotFound
			}
		}

		return owner, balance, errors.Wrap(err, "failed to check kin3 account status")
	}

	//
	// Extract balance
	//
	strBalance := stellarAccount.GetCreditBalance(kin.KinAssetCode, k.kin2Issuer)
	stellarBalance, err := amount.ParseInt64(strBalance)
	if err != nil {
		return owner, balance, errors.Wrap(err, "failed to parse balance")
	}
	if stellarBalance < 0 {
		return owner, balance, errors.Errorf("cannot migrate negative balance: %d", balance)
	}
	balance = uint64(stellarBalance)

	//
	// Extract owner
	//
	nonZeroSigners := make([]horizon.Signer, 0, len(stellarAccount.Signers))
	for _, s := range stellarAccount.Signers {
		if s.Weight > 0 {
			nonZeroSigners = append(nonZeroSigners, s)
		}
	}

	if len(nonZeroSigners) > 1 {
		return owner, balance, migration.ErrMultisig
	} else if len(nonZeroSigners) == 0 {
		return owner, balance, migration.ErrBurned
	}

	owner, err = strkey.Decode(strkey.VersionByteAccountID, nonZeroSigners[0].Key)
	if err != nil {
		return owner, balance, errors.Wrap(err, "failed to decode owner key")
	}

	return owner, balance, nil
}

type kin3Loader struct {
	hc horizon.ClientInterface
}

func NewKin3Loader(hc horizon.ClientInterface) migration.Loader {
	return &kin3Loader{
		hc: hc,
	}
}

// LoadAccount implements migration.Loader.Load.
func (k *kin3Loader) LoadAccount(account ed25519.PublicKey) (owner ed25519.PublicKey, balance uint64, err error) {
	address, err := strkey.Encode(strkey.VersionByteAccountID, account)
	if err != nil {
		return owner, balance, errors.Wrap(err, "failed to encode account as stellar address")
	}

	stellarAccount, err := k.hc.LoadAccount(address)
	if err != nil {
		if hErr, ok := err.(*horizon.Error); ok {
			switch hErr.Problem.Status {
			case http.StatusNotFound:
				return owner, balance, migration.ErrNotFound
			}
		}

		return owner, balance, errors.Wrap(err, "failed to check kin3 account status")
	}

	//
	// Extract balance
	//
	strBalance, err := stellarAccount.GetNativeBalance()
	if err != nil {
		return owner, balance, errors.Wrap(err, "failed to get native balance")
	}
	stellarBalance, err := amount.ParseInt64(strBalance)
	if err != nil {
		return owner, balance, errors.Wrap(err, "failed to parse balance")
	}
	if stellarBalance < 0 {
		return owner, balance, errors.Errorf("cannot migrate negative balance: %d", balance)
	}
	balance = uint64(stellarBalance)

	//
	// Extract owner
	//
	nonZeroSigners := make([]horizon.Signer, 0, len(stellarAccount.Signers))
	for _, s := range stellarAccount.Signers {
		if s.Weight > 0 {
			nonZeroSigners = append(nonZeroSigners, s)
		}
	}

	if len(nonZeroSigners) > 1 {
		return owner, balance, migration.ErrMultisig
	} else if len(nonZeroSigners) == 0 {
		return owner, balance, migration.ErrBurned
	}

	owner, err = strkey.Decode(strkey.VersionByteAccountID, nonZeroSigners[0].Key)
	if err != nil {
		return owner, balance, errors.Wrap(err, "failed to decode owner key")
	}

	return owner, balance, nil
}
