package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/adrg/xdg"
	"github.com/mitchellh/go-homedir"
	"gopkg.in/yaml.v3"
)

const ApplicationName = "octosql"

var octosqlHomeDir = func() string {
	dir, err := homedir.Dir()
	if err != nil {
		log.Fatalf("couldn't get user home directory: %s", err)
	}
	octosqlHomeDir := filepath.Join(dir, ".octosql")
	if _, err := os.Stat(octosqlHomeDir); os.IsNotExist(err) {
		if err = os.MkdirAll(octosqlHomeDir, 0755); err != nil {
			log.Fatalf("couldn't create ~/.octosql home directory: %s", err)
		}
	}
	return octosqlHomeDir
}()

var OctosqlConfigDir = func() string {
	configDirPath, err := xdg.SearchConfigFile(ApplicationName)
	if err != nil {
		return octosqlHomeDir
	}
	return configDirPath
}()

var OctosqlCacheDir = func() string {
	cacheDirPath, err := xdg.SearchCacheFile(ApplicationName)
	if err != nil {
		return octosqlHomeDir
	}
	return cacheDirPath
}()

var OctosqlDataDir = func() string {
	dataDirPath, err := xdg.SearchDataFile(ApplicationName)
	if err != nil {
		return octosqlHomeDir
	}
	return dataDirPath
}()

type Config struct {
	Databases []DatabaseConfig `yaml:"databases"`
	Files     FilesConfig      `yaml:"files"`
}

type DatabaseConfig struct {
	Name    string                               `yaml:"name"`
	Type    PluginReference                      `yaml:"type"`
	Version *YamlUnmarshallableVersionConstraint `yaml:"version"`
	Config  yaml.Node                            `yaml:"config"`
}

type FilesConfig struct {
	JSON            JSONConfig `yaml:"json"`
	BufferSizeBytes int        `yaml:"buffer_size_bytes"`
}

type JSONConfig struct {
	MaxLineSizeBytes int `yaml:"max_line_size_bytes"`
}

func Read() (*Config, error) {
	var config Config
	data, err := os.ReadFile(filepath.Join(OctosqlConfigDir, "octosql.yml"))
	if err != nil && os.IsNotExist(err) {
		config = Config{}
	} else if err != nil {
		return nil, fmt.Errorf("couldn't read config file: %w", err)
	} else {
		if err := yaml.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("couldn't unmarshal yaml configuration: %w", err)
		}
	}

	// TODO: Refactor to a sensibly structured default value system.
	if config.Files.BufferSizeBytes == 0 {
		config.Files.BufferSizeBytes = 4096 * 1024
	}
	if config.Files.JSON.MaxLineSizeBytes == 0 {
		config.Files.JSON.MaxLineSizeBytes = 1024 * 1024
	}

	return &config, nil
}

type YamlUnmarshallableVersionConstraint semver.Constraints

func (constraint *YamlUnmarshallableVersionConstraint) UnmarshalText(text []byte) error {
	v, err := semver.NewConstraint(string(text))
	if err != nil {
		return err
	}
	*constraint = YamlUnmarshallableVersionConstraint(*v)
	return nil
}

func (constraint *YamlUnmarshallableVersionConstraint) Raw() *semver.Constraints {
	return (*semver.Constraints)(constraint)
}

func NewYamlUnmarshallableVersionConstraint(v *semver.Constraints) *YamlUnmarshallableVersionConstraint {
	return (*YamlUnmarshallableVersionConstraint)(v)
}

type PluginReference struct {
	Name       string
	Repository string
}

func (r *PluginReference) UnmarshalText(text []byte) error {
	if i := strings.Index(string(text), "/"); i > 0 {
		r.Repository = string(text[:i])
		r.Name = string(text[i+1:])
	} else {
		r.Name = string(text)
		r.Repository = "core"
	}
	return nil
}

func (r *PluginReference) String() string {
	return fmt.Sprintf("%s/%s", r.Repository, r.Name)
}

// TODO: Using a custom context everywhere would be better.

type contextKey struct{}

func ContextWithConfig(ctx context.Context, config *Config) context.Context {
	return context.WithValue(ctx, contextKey{}, config)
}

func FromContext(ctx context.Context) *Config {
	return ctx.Value(contextKey{}).(*Config)
}
