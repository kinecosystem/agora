package testutil

import (
	"context"

	"google.golang.org/grpc/metadata"
)

func GetKin2Context(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(
		ctx,
		metadata.Pairs(
			"kin-version", "2",
		),
	)
}
