package cmd

import (
	"context"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	migrationpb "github.com/kinecosystem/agora/pkg/migration/proto"
)

var setRateCmd = &cobra.Command{
	Use:   "set-limit",
	Short: "sets the ratelimit for migration",
	RunE:  setRateLimitRun,
	Args:  cobra.ExactArgs(1),
}
var setEnabledCmd = &cobra.Command{
	Use:   "set-enabled",
	Short: "sets the state for migration processor",
	RunE:  setEnabledRun,
	Args:  cobra.ExactArgs(1),
}

func init() {
	rootCmd.AddCommand(setRateCmd)
	rootCmd.AddCommand(setEnabledCmd)
}

func setRateLimitRun(_ *cobra.Command, args []string) error {
	limit, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid rate limit")
	}

	_, err = client.SetRateLimit(context.Background(), &migrationpb.SetRateLimitRequest{
		Rate: uint32(limit),
	})
	return err
}

func setEnabledRun(_ *cobra.Command, args []string) error {
	var s migrationpb.SetStateRequest_State

	switch strings.ToLower(args[0]) {
	case "true":
		s = migrationpb.SetStateRequest_RUNNING
	case "false":
		s = migrationpb.SetStateRequest_STOPPED
	default:
		return errors.Errorf("must be true or false")
	}
	_, err := client.SetState(context.Background(), &migrationpb.SetStateRequest{
		State: s,
	})
	return err
}
