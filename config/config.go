package config

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/Masterminds/semver"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases []DatabaseConfig `yaml:"databases"`
}

type YamlUnmarshallableVersion semver.Version

func (version *YamlUnmarshallableVersion) UnmarshalText(text []byte) error {
	v, err := semver.NewVersion(string(text))
	if err != nil {
		return err
	}
	*version = YamlUnmarshallableVersion(*v)
	return nil
}

func (version *YamlUnmarshallableVersion) Raw() *semver.Version {
	return (*semver.Version)(version)
}

func NewYamlUnmarshallableVersion(v *semver.Version) *YamlUnmarshallableVersion {
	return (*YamlUnmarshallableVersion)(v)
}

type DatabaseConfig struct {
	Name    string                     `yaml:"name"`
	Type    string                     `yaml:"type"`
	Version *YamlUnmarshallableVersion `yaml:"version"`
	Config  yaml.Node                  `yaml:"config"`
}

func Read() (*Config, error) {
	data, err := ioutil.ReadFile("octosql.yaml")
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{
				Databases: nil,
			}, nil
		}
		return nil, fmt.Errorf("couldn't read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("couldn't unmarshal yaml configuration: %w", err)
	}

	return &config, nil
}
