package manager

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/mholt/archiver"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/plugins/repository"
)

type PluginManager struct {
	Repositories []repository.Repository
}

type PluginMetadata struct {
	Reference config.PluginReference
	Versions  []Version
}

type Version struct {
	Number *semver.Version
}

func (m *PluginManager) ListInstalledPlugins() ([]PluginMetadata, error) {
	repositoryDirectories, err := os.ReadDir(getPluginDir())
	if os.IsNotExist(err) {
		return []PluginMetadata{}, nil
	} else if err != nil {
		return nil, fmt.Errorf("couldn't list plugins directory: %w", err)
	}

	var out []PluginMetadata
	for _, repoDirName := range repositoryDirectories {
		repoDir := filepath.Join(getPluginDir(), repoDirName.Name())

		pluginDirectories, err := os.ReadDir(repoDir)
		if err != nil {
			return nil, fmt.Errorf("couldn't list plugins directory: %w", err)
		}

		curOut := make([]PluginMetadata, len(pluginDirectories))
		for i, dir := range pluginDirectories {
			firstDashIndex := strings.LastIndex(dir.Name(), "-")
			secondDashIndex := firstDashIndex + 1 + strings.LastIndex(dir.Name()[firstDashIndex+1:], "-")
			curOut[i].Reference = config.PluginReference{
				Name:       dir.Name()[secondDashIndex+1:],
				Repository: repoDirName.Name(),
			}
		}

		for i := range curOut {
			curPluginDir := filepath.Join(repoDir, pluginDirectories[i].Name())
			pluginVersions, err := os.ReadDir(curPluginDir)
			if err != nil {
				return nil, fmt.Errorf("couldn't list plugin directory: %w", err)
			}
			curOut[i].Versions = make([]Version, len(pluginVersions))
			for j, version := range pluginVersions {
				versionNumber, err := semver.NewVersion(version.Name())
				if err != nil {
					return nil, fmt.Errorf("couldn't parse plugin '%s' version number '%s': %w", curOut[i].Reference.String(), version.Name(), err)
				}
				curOut[i].Versions[j] = Version{Number: versionNumber}
			}
			sort.Slice(curOut[i].Versions, func(j, k int) bool {
				return curOut[i].Versions[j].Number.GreaterThan(curOut[i].Versions[k].Number)
			})
		}
		out = append(out, curOut...)
	}

	return out, nil
}

func (m *PluginManager) GetPluginBinaryPath(ref config.PluginReference, version *semver.Version) (string, error) {
	fullName := fmt.Sprintf("octosql-plugin-%s", ref.Name)

	binaryPath := filepath.Join(getPluginDir(), ref.Repository, fullName, version.String(), fullName)
	if runtime.GOOS == "windows" {
		binaryPath += ".exe"
	}

	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		return "", fmt.Errorf("plugin '%s' version '%s' is not installed", ref.String(), version.String())
	} else if err != nil {
		return "", fmt.Errorf("couldn't check if plugin '%s' version '%s' is installed: %w", ref.String(), version.String(), err)
	}

	return binaryPath, nil
}

func getPluginDir() string {
	out, ok := os.LookupEnv("OCTOSQL_PLUGIN_DIR")
	if ok {
		return out
	}

	return filepath.Join(config.OctosqlDataDir, "plugins")
}

func (m *PluginManager) Install(ctx context.Context, name string, constraint *semver.Constraints) error {
	if strings.Count(name, "@") > 1 {
		return fmt.Errorf("plugin name can contain only one '@' character: '%s'", name)
	}
	if strings.Count(name, "/") > 1 {
		return fmt.Errorf("plugin name can contain only one '/' character: '%s'", name)
	}

	if i := strings.Index(name, "@"); i != -1 {
		if constraint != nil {
			log.Fatalf("BUG: plugin '%s' can't have both inline constraint and config constraint", name)
		}

		var err error
		constraint, err = semver.NewConstraint(name[i+1:])
		if err != nil {
			return fmt.Errorf("couldn't parse version constraint: %w", err)
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
		if constraint != nil {
			if constraint.Check(curVersion.Number) {
				version = &curVersion
				break
			}
		} else if curVersion.Number.Prerelease() == "" {
			// If there's nothing specified, we take the latest non-prerelase version
			version = &curVersion
			break
		}
	}
	if version == nil {
		return fmt.Errorf("version not found")
	}

	fmt.Printf("Downloading %s/%s@%s...\n", repoSlug, name, version.Number)

	url := manifest.GetBinaryDownloadURL(version.Number)

	newPluginDir := filepath.Join(getPluginDir(), repoSlug, fmt.Sprintf("octosql-plugin-%s", name), version.Number.String())

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

	if err := registerFileExtensions(plugin.Name, plugin.FileExtensions); err != nil {
		return fmt.Errorf("couldn't register file extensions: %w", err)
	}

	return nil
}
