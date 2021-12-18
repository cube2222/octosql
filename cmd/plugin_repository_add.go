package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/cube2222/octosql/plugins/repository"
)

// pluginRepositoryAddCmd represents the plugin install command
var pluginRepositoryAddCmd = &cobra.Command{
	Use:   "add",
	Short: "",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		for _, arg := range args {
			if err := repository.AddRepository(context.Background(), arg); err != nil {
				return fmt.Errorf("couldn't add repository '%s': %s", arg, err)
			}
		}

		return nil
	},
}

func init() {
	pluginRepositoryCmd.AddCommand(pluginRepositoryAddCmd)
}
