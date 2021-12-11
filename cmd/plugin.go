package cmd

import (
	"github.com/spf13/cobra"
)

// pluginCmd represents the plugin command
var pluginCmd = &cobra.Command{
	Use:   "plugin",
	Short: "",
	Long:  ``,
}

func init() {
	rootCmd.AddCommand(pluginCmd)
}
