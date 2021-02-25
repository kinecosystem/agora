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
	CreateAccountURL   *url.URL
	SignTransactionURL *url.URL
	EventsURL          *url.URL
	WebhookSecret      string
}
