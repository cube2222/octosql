package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type PluginManager struct {
}

type PluginMetadata struct {
	Name string
}

func (m *PluginManager) ListInstalledPlugins() ([]PluginMetadata, error) {
	pluginDirectories, err := os.ReadDir(getPluginDir())
	if os.IsNotExist(err) {
		return []PluginMetadata{}, nil
	} else if err != nil {
		return nil, fmt.Errorf("couldn't list plugins directory: %w", err)
	}

	out := make([]PluginMetadata, len(pluginDirectories))
	for i, dir := range pluginDirectories {
		firstDashIndex := strings.LastIndex(dir.Name(), "-")
		secondDashIndex := firstDashIndex + 1 + strings.LastIndex(dir.Name()[firstDashIndex+1:], "-")
		out[i].Name = dir.Name()[secondDashIndex+1:]
	}

	return out, nil
}

func (m *PluginManager) GetPluginBinaryPath(name string) (string, error) {
	fullName := fmt.Sprintf("octosql-plugin-%s", name)

	return filepath.Join(getPluginDir(), fullName, fullName), nil
}

func getPluginDir() string {
	out, ok := os.LookupEnv("OCTOSQL_PLUGIN_DIR")
	if !ok {
		out = "~/.octosql/plugins"
	}
	return out
}
