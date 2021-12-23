package cmd

import (
	"fmt"

	"github.com/Masterminds/semver"
	"github.com/spf13/cobra"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/plugins/manager"
	"github.com/cube2222/octosql/plugins/repository"
)

// pluginInstallCmd represents the plugin install command
var pluginInstallCmd = &cobra.Command{
	Use:   "install",
	Short: "",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		repositories, err := repository.GetRepositories(ctx)
		if err != nil {
			return fmt.Errorf("couldn't get repositories: %w", err)
		}
		pluginManager := &manager.PluginManager{
			Repositories: repositories,
		}

		if len(args) > 0 {
			for _, arg := range args {
				if err := pluginManager.Install(ctx, arg, nil); err != nil {
					return err
				}
			}
			return nil
		}

		cfg, err := config.Read()
		if err != nil {
			return fmt.Errorf("couldn't read config: %w", err)
		}

	dbLoop:
		for i := range cfg.Databases {
			installedPlugins, err := pluginManager.ListInstalledPlugins()
			if err != nil {
				return fmt.Errorf("couldn't list installed plugins: %w", err)
			}

			if cfg.Databases[i].Version == nil {
				constraint, _ := semver.NewConstraint("*")
				cfg.Databases[i].Version = config.NewYamlUnmarshallableVersionConstraint(constraint)
			}
			for _, plugin := range installedPlugins {
				if plugin.Reference != cfg.Databases[i].Type {
					continue
				}
				for _, version := range plugin.Versions {
					if cfg.Databases[i].Version.Raw().Check(version.Number) {
						continue dbLoop
					}
				}
			}
			if err := pluginManager.Install(ctx, cfg.Databases[i].Type.String(), cfg.Databases[i].Version.Raw()); err != nil {
				return err
			}
		}

		return nil
	},
}

func init() {
	pluginCmd.AddCommand(pluginInstallCmd)
}
