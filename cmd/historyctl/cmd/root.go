package cmd

import (
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	logformat "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	awsConfig aws.Config
	level     string
)

var rootCmd = &cobra.Command{
	Use:               "historyctl",
	Short:             "Utility for inspecting history",
	PersistentPreRunE: rootPreRun,
	SilenceUsage:      true,
}

func Execute() {
	rootCmd.PersistentFlags().StringVar(&level, "log-level", "info", "")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func rootPreRun(_ *cobra.Command, _ []string) (err error) {
	awsConfig, err = external.LoadDefaultAWSConfig()
	if err != nil {
		return errors.Wrap(err, "failed to init v2 aws sdk")
	}

	// InitLogger configures the global logger.
	logger := log.StandardLogger()
	logger.Formatter = &logformat.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
	}

	switch level {
	case "debug":
		logger.Level = log.DebugLevel
	case "info":
		logger.Level = log.InfoLevel
	case "warn":
		logger.Level = log.WarnLevel
	case "error":
		logger.Level = log.ErrorLevel
	case "fatal":
		logger.Level = log.FatalLevel
	case "panic":
		logger.Level = log.PanicLevel
	default:
		logger.Level = log.DebugLevel
	}

	return nil
}
