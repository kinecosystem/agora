package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	migrationpb "github.com/kinecosystem/agora/pkg/migration/proto"
)

var (
	endpoint string

	cc     *grpc.ClientConn
	client migrationpb.AdminClient
)

var rootCmd = &cobra.Command{
	Use:                "borderctl",
	Short:              "Control the offline migration process from stellar to solana",
	PersistentPreRunE:  rootPreRun,
	PersistentPostRunE: rootPostRun,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&endpoint, "endpoint", "e", "localhost:8085", "endpoint")
}

func rootPreRun(_ *cobra.Command, _ []string) (err error) {
	cc, err = grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}

	client = migrationpb.NewAdminClient(cc)

	return nil
}

func rootPostRun(_ *cobra.Command, _ []string) error {
	return cc.Close()
}
