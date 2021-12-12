package manager

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/mholt/archiver"

	"github.com/cube2222/octosql/plugins/repository"
)

type PluginManager struct {
	Repositories []repository.Repository
}

type PluginMetadata struct {
	Name     string
	Versions []Version
}

type Version struct {
	Number *semver.Version
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

	for i := range out {
		curPluginDir := filepath.Join(getPluginDir(), pluginDirectories[i].Name())
		pluginVersions, err := os.ReadDir(curPluginDir)
		if err != nil {
			return nil, fmt.Errorf("couldn't list plugin directory: %w", err)
		}
		out[i].Versions = make([]Version, len(pluginVersions))
		for j, version := range pluginVersions {
			versionNumber, err := semver.NewVersion(version.Name())
			if err != nil {
				return nil, fmt.Errorf("couldn't parse plugin '%s' version number '%s': %w", pluginDirectories[i].Name(), version.Name(), err)
			}
			out[i].Versions[j] = Version{Number: versionNumber}
		}
		sort.Slice(out[i].Versions, func(j, k int) bool {
			return out[i].Versions[j].Number.GreaterThan(out[i].Versions[k].Number)
		})
	}

	return out, nil
}

func (m *PluginManager) GetPluginBinaryPath(name string, version *semver.Version) (string, error) {
	fullName := fmt.Sprintf("octosql-plugin-%s", name)

	binaryPath := filepath.Join(getPluginDir(), fullName, version.String(), fullName)

	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		return "", fmt.Errorf("plugin '%s' version '%s' is not installed", name, version.String())
	} else if err != nil {
		return "", fmt.Errorf("couldn't check if plugin '%s' version '%s' is installed: %w", name, version.String(), err)
	}

	return binaryPath, nil
}

func getPluginDir() string {
	out, ok := os.LookupEnv("OCTOSQL_PLUGIN_DIR")
	if !ok {
		out = "~/.octosql/plugins"
	}
	return out
}

func (m *PluginManager) Install(ctx context.Context, name string) error {
	if strings.Count(name, "@") > 1 {
		return fmt.Errorf("plugin name can contain only one '@' character: '%s'", name)
	}
	if strings.Count(name, "/") > 1 {
		return fmt.Errorf("plugin name can contain only one '/' character: '%s'", name)
	}

	var versionRequirement *semver.Version
	if i := strings.Index(name, "@"); i != -1 {
		var err error
		versionRequirement, err = semver.NewVersion(name[i+1:])
		if err != nil {
			return fmt.Errorf("couldn't parse version as semver: %w", err)
		}

		name = name[:i]
	}
	repoSlug := "core"
	if i := strings.Index(name, "/"); i != -1 {
		repoSlug = name[:i]
		name = name[i+1:]
	}

	var repo *repository.Repository
	for _, curRepo := range m.Repositories {
		if curRepo.Slug == repoSlug {
			repo = &curRepo
			break
		}
	}
	if repo == nil {
		return fmt.Errorf("repository '%s' not found", repoSlug)
	}

	var plugin *repository.Plugin
	for _, curPlugin := range repo.Plugins {
		if curPlugin.Name == name {
			plugin = &curPlugin
			break
		}
	}
	if plugin == nil {
		return fmt.Errorf("plugin '%s' not found", name)
	}

	manifest, err := repository.GetManifest(ctx, plugin.ManifestURL)
	if err != nil {
		return fmt.Errorf("couldn't get plugin manifest: %w", err)
	}

	var version *repository.Version
	for _, curVersion := range manifest.Versions {
		if versionRequirement != nil {
			if curVersion.Number.Equal(versionRequirement) {
				version = &curVersion
				break
			}
		} else if curVersion.Number.Prerelease() == "" {
			// If there's no specified version, we take the latest non-release version.
			version = &curVersion
			break
		}
	}
	if version == nil {
		return fmt.Errorf("version not found")
	}

	fmt.Printf("Downloading %s/%s@%s...\n", repoSlug, name, version.Number)

	url := manifest.GetBinaryDownloadURL(version.Number)

	newPluginDir := filepath.Join(getPluginDir(), fmt.Sprintf("octosql-plugin-%s", name), version.Number.String())

	if err := os.RemoveAll(newPluginDir); err != nil {
		return fmt.Errorf("couldn't remove old plugin directory: %w", err)
	}

	if err := os.MkdirAll(newPluginDir, os.ModePerm); err != nil {
		return fmt.Errorf("couldn't create plugins directory: %w", err)
	}
	archiveFilePath := filepath.Join(newPluginDir, "archive.tar.gz")

	// Anonymous function to take care of defers before we move forward.
	err = func() error {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("couldn't create request to get plugin: %w", err)
		}
		req = req.WithContext(ctx)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("couldn't get plugin: %w", err)
		}
		defer res.Body.Close()

		f, err := os.Create(archiveFilePath)
		if err != nil {
			return fmt.Errorf("couldn't create plugin archive file: %w", err)
		}
		defer f.Close()

		if _, err := io.Copy(f, res.Body); err != nil {
			return fmt.Errorf("couldn't download plugin archive: %w", err)
		}
		return nil
	}()
	if err != nil {
		return err
	}

	if err := archiver.NewTarGz().Unarchive(archiveFilePath, newPluginDir); err != nil {
		return fmt.Errorf("couldn't unarchive plugin archive: %w", err)
	}

	if err := os.Remove(archiveFilePath); err != nil {
		return fmt.Errorf("couldn't remove plugin archive: %w", err)
	}

	return nil
}
