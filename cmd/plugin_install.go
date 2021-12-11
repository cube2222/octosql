package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/cube2222/octosql/plugins/manager"
	"github.com/cube2222/octosql/plugins/repository"
)

// pluginInstallCmd represents the plugin install command
var pluginInstallCmd = &cobra.Command{
	Use:   "install",
	Short: "",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		repositories, err := repository.GetRepositories(context.Background())
		if err != nil {
			return fmt.Errorf("couldn't get repositories: %w", err)
		}
		pluginManager := &manager.PluginManager{
			Repositories: repositories,
		}

		return pluginManager.Install(context.Background(), args[0])
	},
}

func init() {
	pluginCmd.AddCommand(pluginInstallCmd)
}
