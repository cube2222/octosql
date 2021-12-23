package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/Masterminds/semver"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases []DatabaseConfig `yaml:"databases"`
}

type DatabaseConfig struct {
	Name    string                               `yaml:"name"`
	Type    PluginReference                      `yaml:"type"`
	Version *YamlUnmarshallableVersionConstraint `yaml:"version"`
	Config  yaml.Node                            `yaml:"config"`
}

func Read() (*Config, error) {
	data, err := ioutil.ReadFile("octosql.yml")
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
