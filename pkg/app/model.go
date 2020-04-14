package app

import (
	"encoding/base64"
	"fmt"
	"net/url"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"
)

var (
	ErrURLNotSet = errors.New("requested URL not set")
)

type Config struct {
	AppName            string
	AgoraDataURL       *url.URL
	SignTransactionURL *url.URL
}

// GetAgoraDataURL returns the AgoraDataUrl for the provided memo, if the app has an agora data callback url set.
func (c *Config) GetAgoraDataURL(m kin.Memo) (*commonpb.AgoraDataUrl, error) {
	if c.AgoraDataURL == nil {
		return nil, ErrURLNotSet
	}

	return &commonpb.AgoraDataUrl{
		// todo: proper callback spec
		Value: fmt.Sprintf("https://%s/%s", c.AgoraDataURL, base64.URLEncoding.EncodeToString(m[:])),
	}, nil
}
