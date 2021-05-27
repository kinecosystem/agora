package cmd

import (
	"context"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	gcpb "github.com/kinecosystem/agora/pkg/gc/proto"
)

var setRateCmd = &cobra.Command{
	Use:   "set-limit",
	Short: "sets the ratelimit for garbage collection",
	RunE:  setRateLimitRun,
	Args:  cobra.ExactArgs(1),
}
var setEnabledCmd = &cobra.Command{
	Use:   "set-enabled",
	Short: "sets the state for garbage collector",
	RunE:  setEnabledRun,
	Args:  cobra.ExactArgs(1),
}
var setHistoryCheck = &cobra.Command{
	Use:   "set-check-history",
	Short: "sets whether or not history should be checked before migrating",
	RunE:  setHistoryCheckRun,
	Args:  cobra.ExactArgs(1),
}

func init() {
	rootCmd.AddCommand(setRateCmd)
	rootCmd.AddCommand(setEnabledCmd)
	rootCmd.AddCommand(setHistoryCheck)
}

func setRateLimitRun(_ *cobra.Command, args []string) error {
	limit, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid rate limit")
	}

	_, err = client.SetRateLimit(context.Background(), &gcpb.SetRateLimitRequest{
		Rate: uint32(limit),
	})
	return err
}

func setEnabledRun(_ *cobra.Command, args []string) error {
	var s gcpb.SetStateRequest_State

	switch strings.ToLower(args[0]) {
	case "true":
		s = gcpb.SetStateRequest_RUNNING
	case "false":
		s = gcpb.SetStateRequest_STOPPED
	default:
		return errors.Errorf("must be true or false")
	}
	_, err := client.SetState(context.Background(), &gcpb.SetStateRequest{
		State: s,
	})
	return err
}

func setHistoryCheckRun(_ *cobra.Command, args []string) error {
	var enabled bool
	switch strings.ToLower(args[0]) {
	case "true":
		enabled = true
	case "false":
		enabled = false
	default:
		return errors.Errorf("must be true or false")
	}
	_, err := client.SetHistoryCheck(context.Background(), &gcpb.SetHistoryCheckRequest{
		Enabled: enabled,
	})
	return err
}
