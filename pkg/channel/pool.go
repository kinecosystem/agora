package channel

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"

	"github.com/kinecosystem/go/keypair"
	"github.com/kinecosystem/go/strkey"
	"github.com/pkg/errors"
)

type Channel struct {
	KP   *keypair.Full
	Lock Lock
}

type Lock interface {
	// Unlock releases the lock.
	Unlock() error
}

type Pool interface {
	// GetChannel returns a full keypair available for use as a channel. The pool does not ensure that the account is
	// created or funded, therefore callers of this function must handle that logic.
	//
	// Callers are expected to call Unlock on the channel lock once finished using it.
	GetChannel() (channel *Channel, err error)
}

// GenerateChannelKeypair deterministically generates a keypair using the root account keypair, channel index, and
// configured channel salt.
func GenerateChannelKeypair(rootAccountKP *keypair.Full, channelIndex int, salt string) (*keypair.Full, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(channelIndex))
	rawSeed := strkey.MustDecode(strkey.VersionByteSeed, rootAccountKP.Seed())

	h := sha256.Sum256(append(append(b, rawSeed...), []byte(salt)...))
	return keypair.FromRawSeed(h)
}

// GenerateToken generates a random token for acquiring a channel lock.
func GenerateToken() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", errors.Wrap(err, "failed to generate token")
	}

	return base64.StdEncoding.EncodeToString(b), nil
}
