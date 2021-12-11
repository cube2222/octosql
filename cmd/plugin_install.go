package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

// pluginInstallCmd represents the plugin install command
var pluginInstallCmd = &cobra.Command{
	Use:   "install",
	Short: "",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Println("install plugin")
		return nil
	},
}

func init() {
	pluginCmd.AddCommand(pluginInstallCmd)
}
