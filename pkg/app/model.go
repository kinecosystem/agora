package app

import (
	"net/url"

	"github.com/pkg/errors"
)

var (
	ErrURLNotSet = errors.New("requested URL not set")
)

type Config struct {
	AppName            string
	SignTransactionURL *url.URL
	EventsURL          *url.URL
	InvoicingEnabled   bool
	WebhookSecret      []byte
}
