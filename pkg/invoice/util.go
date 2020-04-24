package invoice

import (
	"crypto/sha256"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// GetSHA224Hash returns the SHA-224 of the marshaled proto message.
func GetSHA224Hash(m proto.Message) ([]byte, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal invoice")
	}

	h := sha256.Sum224(b)
	return h[:], nil
}
