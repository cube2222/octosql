package postgres

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/stdlib"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/physical"
)

type Config struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

func (c *Config) Validate() error {
	return nil
}

func connect(config *Config) (*sql.DB, error) {
	db, err := sql.Open("pgx", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", config.Host, config.Port, config.User, config.Password, config.Database))
	if err != nil {
		return nil, fmt.Errorf("couldn't open database: %w", err)
	}
	return db, nil
}

// This should then store creators as my-postgres.table-name
func Creator(configUntyped config.DatabaseSpecificConfig) (physical.Database, error) {
	cfg := configUntyped.(*Config)
	db, err := connect(cfg)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("couldn't ping database: %w", err)
	}
	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("couldn't close database: %w", err)
	}
	return &Database{
		Config: cfg,
	}, nil
}

type Database struct {
	Config *Config
}

func (d *Database) ListTables() ([]string, error) {
	panic("implement me")
}

func (d *Database) GetTable(name string) (physical.DatasourceImplementation, error) {
	panic("implement me")
}
