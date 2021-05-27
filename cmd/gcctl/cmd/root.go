package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	gcpb "github.com/kinecosystem/agora/pkg/gc/proto"
)

var (
	endpoint string

	cc     *grpc.ClientConn
	client gcpb.AdminClient
)

var rootCmd = &cobra.Command{
	Use:                "gcctl",
	Short:              "Control the offline garbage collector",
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

	client = gcpb.NewAdminClient(cc)

	return nil
}

func rootPostRun(_ *cobra.Command, _ []string) error {
	return cc.Close()
}
