package stellar

import (
	"net/http"
	"testing"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/clients/horizon"
	hProtocol "github.com/kinecosystem/go/protocols/horizon"
	"github.com/kinecosystem/go/protocols/horizon/base"
	"github.com/kinecosystem/go/strkey"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/kinecosystem/agora/pkg/migration"
	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestCompositeLoader_Kin2(t *testing.T) {
	kin2 := &testLoader{accountState: make(map[string]*accountState)}
	kin3 := &testLoader{accountState: make(map[string]*accountState)}

	l := NewCompositeLoader(kin2, kin3)

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	accountOwner := testutil.GenerateSolanaKeys(t, 1)[0]

	owner, balance, err := l.LoadAccount(account)
	assert.Equal(t, migration.ErrNotFound, err)
	assert.EqualValues(t, 0, balance)
	assert.Nil(t, owner)

	kin2.accountState[string(account)] = &accountState{
		owner:   accountOwner,
		balance: 10,
	}

	for _, e := range []error{migration.ErrMultisig, migration.ErrBurned} {
		kin2.accountState[string(account)].err = e

		owner, balance, err = l.LoadAccount(account)
		assert.Equal(t, e, err)
		assert.EqualValues(t, 10, balance)
		assert.EqualValues(t, accountOwner, owner)
	}

	kin2.accountState[string(account)].err = nil
	owner, balance, err = l.LoadAccount(account)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, balance)
	assert.EqualValues(t, accountOwner, owner)
}

func TestCompositeLoader_Kin3(t *testing.T) {
	kin2 := &testLoader{accountState: make(map[string]*accountState)}
	kin3 := &testLoader{accountState: make(map[string]*accountState)}

	l := NewCompositeLoader(kin2, kin3)

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	accountOwner := testutil.GenerateSolanaKeys(t, 1)[0]

	owner, balance, err := l.LoadAccount(account)
	assert.Equal(t, migration.ErrNotFound, err)
	assert.EqualValues(t, 0, balance)
	assert.Nil(t, owner)

	kin3.accountState[string(account)] = &accountState{
		owner:   accountOwner,
		balance: 10,
	}

	for _, e := range []error{migration.ErrMultisig, migration.ErrBurned} {
		kin3.accountState[string(account)].err = e

		owner, balance, err = l.LoadAccount(account)
		assert.Equal(t, e, err)
		assert.EqualValues(t, 10, balance)
		assert.EqualValues(t, accountOwner, owner)
	}

	kin3.accountState[string(account)].err = nil
	owner, balance, err = l.LoadAccount(account)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, balance)
	assert.EqualValues(t, accountOwner, owner)
}

func TestCompositeLoader_Both(t *testing.T) {
	kin2 := &testLoader{accountState: make(map[string]*accountState)}
	kin3 := &testLoader{accountState: make(map[string]*accountState)}

	l := NewCompositeLoader(kin2, kin3)

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	accountOwner := testutil.GenerateSolanaKeys(t, 1)[0]

	owner, balance, err := l.LoadAccount(account)
	assert.Equal(t, migration.ErrNotFound, err)
	assert.EqualValues(t, 0, balance)
	assert.Nil(t, owner)

	kin2.accountState[string(account)] = &accountState{
		owner:   accountOwner,
		balance: 2,
	}
	kin3.accountState[string(account)] = &accountState{
		owner:   accountOwner,
		balance: 3,
	}

	// All errors except not found should just yield kin3 balance.
	for _, e := range []error{migration.ErrMultisig, migration.ErrBurned, errors.New("temporary")} {
		kin3.accountState[string(account)].err = e

		owner, balance, err = l.LoadAccount(account)
		assert.True(t, errors.Is(err, e))
		assert.EqualValues(t, 3, balance)
		assert.EqualValues(t, accountOwner, owner)
	}

	kin3.accountState[string(account)].err = nil
	owner, balance, err = l.LoadAccount(account)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, balance)
	assert.EqualValues(t, accountOwner, owner)

	delete(kin3.accountState, string(account))

	owner, balance, err = l.LoadAccount(account)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, balance)
	assert.EqualValues(t, accountOwner, owner)
}

func TestKin2Loader(t *testing.T) {
	hc := &horizon.MockClient{}
	l := NewKin2Loader(hc, kin.Kin2TestIssuer)

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	notFoundError := &horizon.Error{
		Problem: horizon.Problem{
			Status: http.StatusNotFound,
		},
	}

	//
	// Not Found
	//
	hc.On("LoadAccount", mock.Anything).Return(hProtocol.Account{}, notFoundError)
	_, _, err := l.LoadAccount(account)
	assert.Equal(t, migration.ErrNotFound, err)

	//
	// Found - negative balance
	//
	hAccount := hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "1",
			},
			{
				Asset: base.Asset{
					Code:   "KIN",
					Issuer: kin.Kin2TestIssuer,
				},
				Balance: "-1",
			},
		},
	}
	hc.ExpectedCalls = nil
	hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	_, _, err = l.LoadAccount(account)
	assert.Error(t, err)

	//
	// Found - burned
	//
	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 0,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "1",
			},
			{
				Asset: base.Asset{
					Code:   "KIN",
					Issuer: kin.Kin2TestIssuer,
				},
				Balance: "100",
			},
		},
	}
	hc.ExpectedCalls = nil
	hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	_, _, err = l.LoadAccount(account)
	assert.Equal(t, migration.ErrBurned, err)

	//
	// Found, multi-sig
	//
	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, testutil.GenerateSolanaKeys(t, 1)[0]),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "1",
			},
			{
				Asset: base.Asset{
					Code:   "KIN",
					Issuer: kin.Kin2TestIssuer,
				},
				Balance: "100",
			},
		},
	}
	hc.ExpectedCalls = nil
	hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	_, _, err = l.LoadAccount(account)
	assert.Error(t, err)

	//
	// Found
	//
	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "1",
			},
			{
				Asset: base.Asset{
					Code:   "KIN",
					Issuer: kin.Kin2TestIssuer,
				},
				Balance: "100",
			},
		},
	}
	hc.ExpectedCalls = nil
	hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	owner, balance, err := l.LoadAccount(account)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100*1e5), balance)
	assert.Equal(t, account, owner)

	// Found - different owner
	accountSigner := testutil.GenerateSolanaKeys(t, 1)[0]
	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, accountSigner),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "1",
			},
			{
				Asset: base.Asset{
					Code:   "KIN",
					Issuer: kin.Kin2TestIssuer,
				},
				Balance: "100",
			},
		},
	}
	hc.ExpectedCalls = nil
	hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	owner, balance, err = l.LoadAccount(account)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100*1e5), balance)
	assert.Equal(t, accountSigner, owner)
}

func TestKin3Loader(t *testing.T) {
	hc := &horizon.MockClient{}
	l := NewKin3Loader(hc)

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	notFoundError := &horizon.Error{
		Problem: horizon.Problem{
			Status: http.StatusNotFound,
		},
	}

	//
	// Not Found
	//
	hc.On("LoadAccount", mock.Anything).Return(hProtocol.Account{}, notFoundError)
	_, _, err := l.LoadAccount(account)
	assert.Equal(t, migration.ErrNotFound, err)

	//
	// Found - negative balance
	//
	hAccount := hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "-1",
			},
		},
	}
	hc.ExpectedCalls = nil
	hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	_, _, err = l.LoadAccount(account)
	assert.Error(t, err)

	//
	// Found - burned
	//
	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 0,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "100",
			},
		},
	}
	hc.ExpectedCalls = nil
	hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	_, _, err = l.LoadAccount(account)
	assert.Equal(t, migration.ErrBurned, err)

	//
	// Found, multi-sig
	//
	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, testutil.GenerateSolanaKeys(t, 1)[0]),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "100",
			},
		},
	}
	hc.ExpectedCalls = nil
	hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	_, _, err = l.LoadAccount(account)
	assert.Error(t, err)

	//
	// Found
	//
	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "100",
			},
		},
	}
	hc.ExpectedCalls = nil
	hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	owner, balance, err := l.LoadAccount(account)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100*1e5), balance)
	assert.Equal(t, account, owner)

	// Found - different owner
	accountSigner := testutil.GenerateSolanaKeys(t, 1)[0]
	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, accountSigner),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "100",
			},
		},
	}
	hc.ExpectedCalls = nil
	hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	owner, balance, err = l.LoadAccount(account)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100*1e5), balance)
	assert.Equal(t, accountSigner, owner)
}
