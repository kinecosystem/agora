package invoice

import (
	"crypto/sha256"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"
)

// GetHash returns the SHA-224 hash of the invoice, which can be used as the foreign key in an Agora transaction memo
func GetHash(inv *commonpb.Invoice) ([]byte, error) {
	b, err := proto.Marshal(inv)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal invoice")
	}

	h := sha256.Sum224(b)
	return h[:], nil
}
