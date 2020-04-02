package invoice

import (
	"crypto/sha256"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"
)

// GetHashPrefix returns the invoice hash prefix that can be used as a FK in an Agora transaction memo
func GetHashPrefix(inv *commonpb.Invoice) ([]byte, error) {
	b, err := proto.Marshal(inv)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal invoice")
	}

	h := sha256.Sum256(b)
	prefix := make([]byte, 29)
	for i := 0; i < 28; i++ {
		prefix[i] = h[i]
	}
	prefix[28] = h[28] & 0x3f

	return prefix, nil
}
