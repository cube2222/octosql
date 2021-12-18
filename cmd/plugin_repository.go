package cmd

import (
	"github.com/spf13/cobra"
)

// pluginRepositoryCmd represents the plugin command
var pluginRepositoryCmd = &cobra.Command{
	Use:   "repository",
	Short: "",
	Long:  ``,
}

func init() {
	pluginCmd.AddCommand(pluginRepositoryCmd)
}
