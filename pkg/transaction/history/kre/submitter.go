package kre

import "context"

type Submitter interface {
	Submit(ctx context.Context, src interface{}) (err error)
}
