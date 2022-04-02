package repository

import (
	"context"
	"encoding/json"
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
	"github.com/mitchellh/go-homedir"
)

var repositoriesDir = func() string {
	dir, err := homedir.Dir()
	if err != nil {
		log.Fatalf("couldn't get user home directory: %s", err)
	}
	return filepath.Join(dir, ".octosql/repositories")
}()

var officialPluginRepositoryURL = func() string {
	if url, ok := os.LookupEnv("OCTOSQL_PLUGIN_REPOSITORY_OFFICIAL_URL"); ok {
		return url
	}
	return "https://raw.githubusercontent.com/cube2222/octosql/master/plugin_repository.json"
}()

type RepositoryEntry struct {
	URL string `json:"url"`
}

func getAdditionalPluginRepositoryURLs() ([]string, error) {
	entries, err := os.ReadDir(repositoriesDir)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("couldn't read plugin repositories directory: %w", err)
	}

	out := make([]string, len(entries))
	for i := range entries {
		data, err := os.ReadFile(filepath.Join(repositoriesDir, entries[i].Name()))
		if err != nil {
			return nil, fmt.Errorf("couldn't read plugin repository file: %w", err)
		}
		var entry RepositoryEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, fmt.Errorf("couldn't decode plugin repository file: %w", err)
		}
		out[i] = entry.URL
	}

	return out, nil
}

func AddRepository(ctx context.Context, url string) error {
	repo, err := GetRepository(ctx, url)
	if err != nil {
		return fmt.Errorf("couldn't get repository: %w", err)
	}
	entry := RepositoryEntry{
		URL: url,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("couldn't encode repository entry: %w", err)
	}
	if err := os.MkdirAll(repositoriesDir, 0755); err != nil {
		return fmt.Errorf("couldn't create plugin repositories directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(repositoriesDir, repo.Slug), data, 0644); err != nil {
		return fmt.Errorf("couldn't write repository entry: %w", err)
	}

	return nil
}

func GetRepositories(ctx context.Context) ([]Repository, error) {
	officialRepository, err := GetRepository(ctx, officialPluginRepositoryURL)
	if err != nil {
		return nil, fmt.Errorf("couldn't get official repository: %w", err)
	}
	additionalPluginRepositoryURLs, err := getAdditionalPluginRepositoryURLs()
	if err != nil {
		return nil, fmt.Errorf("couldn't get addional repository URLs: %w", err)
	}
	additionalRepositories := make([]Repository, len(additionalPluginRepositoryURLs))
	for i := range additionalPluginRepositoryURLs {
		repository, err := GetRepository(ctx, additionalPluginRepositoryURLs[i])
		if err != nil {
			return nil, fmt.Errorf("couldn't get additional repository from '%s': %w", additionalPluginRepositoryURLs[i], err)
		}
		if repository.Slug == "" {
			return nil, fmt.Errorf("repository from '%s' doesn't have a slug", additionalPluginRepositoryURLs[i])
		}
		// TODO: Check for duplicate slugs.
		additionalRepositories[i] = repository
	}

	return append([]Repository{officialRepository}, additionalRepositories...), nil
}

type Repository struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Slug        string   `json:"slug"`
	Plugins     []Plugin `json:"plugins"`
}

func GetRepository(ctx context.Context, url string) (Repository, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return Repository{}, fmt.Errorf("couldn't create request to get plugin repository contents: %w", err)
	}
	req = req.WithContext(ctx)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return Repository{}, fmt.Errorf("couldn't get plugin repository contents: %w", err)
	}
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return Repository{}, fmt.Errorf("couldn't read plugin repository contents: %w", err)
	}

	var out Repository
	if err := json.Unmarshal(data, &out); err != nil {
		return Repository{}, fmt.Errorf("couldn't decode plugin repository contents '%s': %w", string(data), err)
	}

	return out, nil
}

type Plugin struct {
	Name string `json:"name"`

	// FileExtensions supported by the plugin.
	FileExtensions []string `json:"file_extensions"`

	// Description is a *short* description of the plugin.
	Description  string `json:"description"`
	Website      string `json:"website"`
	ContactEmail string `json:"contact_email"`
	License      string `json:"license"`
	ReadmeURL    string `json:"readme_url"`
	ManifestURL  string `json:"manifest_url"`
}

type Manifest struct {
	BinaryDownloadURLPattern string `json:"binary_download_url_pattern"`

	// Version are sorted descending.
	Versions []Version `json:"versions"`
}

func GetManifest(ctx context.Context, url string) (Manifest, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return Manifest{}, fmt.Errorf("couldn't create request to get plugin manifest: %w", err)
	}
	req = req.WithContext(ctx)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return Manifest{}, fmt.Errorf("couldn't get plugin manifest: %w", err)
	}
	defer res.Body.Close()

	var out Manifest
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return Manifest{}, fmt.Errorf("couldn't decode plugin manifest: %w", err)
	}

	sort.Slice(out.Versions, func(i, j int) bool {
		return out.Versions[i].Number.GreaterThan(out.Versions[j].Number)
	})

	return out, nil
}

func (m *Manifest) GetBinaryDownloadURL(version *semver.Version) string {
	return strings.NewReplacer(
		"{{os}}", runtime.GOOS,
		"{{arch}}", runtime.GOARCH,
		"{{version}}", version.String(),
	).Replace(m.BinaryDownloadURLPattern)
}

type Version struct {
	Number *semver.Version `json:"number"`
}
