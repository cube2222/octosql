package postgres

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/octosql"
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

type testLogger struct {
}

func (t *testLogger) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	log.Printf("%s %s %+v", level, msg, data)
}

func connect(config *Config) (*pgx.Conn, error) {
	db, err := pgx.Connect(pgx.ConnConfig{
		Host:      config.Host,
		Port:      uint16(config.Port),
		User:      config.User,
		Database:  config.Database,
		Password:  config.Password,
		TLSConfig: nil,
		Logger:    &testLogger{},
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't open database: %w", err)
	}
	return db, nil
}

// This should then store creators as my-postgres.table-name
func Creator(ctx context.Context, configUntyped config.DatabaseSpecificConfig) (physical.Database, error) {
	cfg := configUntyped.(*Config)
	db, err := connect(cfg)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to database: %w", err)
	}
	if err := db.Ping(ctx); err != nil {
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

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	panic("implement me")
}

func (d *Database) GetTable(ctx context.Context, name string) (physical.DatasourceImplementation, error) {
	db, err := connect(d.Config)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to database: %w", err)
	}

	rows, err := db.QueryEx(ctx, "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", nil, name)
	if err != nil {
		return nil, fmt.Errorf("couldn't describe table: %w", err)
	}

	var descriptions [][]string
	for rows.Next() {
		desc := make([]string, 3)
		if err := rows.Scan(&desc[0], &desc[1], &desc[2]); err != nil {
			return nil, fmt.Errorf("couldn't scan table description: %w", err)
		}
		descriptions = append(descriptions, desc)
	}

	fields := make([]physical.SchemaField, len(descriptions))
	for i := range descriptions {
		var t octosql.Type
		switch descriptions[i][1] {
		case "integer", "smallint":
			t = octosql.Int
		case "text", "character":
			t = octosql.String
		case "real", "numeric":
			t = octosql.Float
		case "boolean":
			t = octosql.Boolean
		default:
			t = octosql.Null
			log.Printf("unsupported postgres type: %s", descriptions[i][1])

			// TODO: Support time.
			// TODO: Support more types.
		}
		if descriptions[i][2] == "YES" {
			t = octosql.TypeSum(t, octosql.Null)
		}
		fields[i] = physical.SchemaField{
			Name: descriptions[i][0],
			Type: t,
		}
	}

	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("couldn't close database: %w", err)
	}

	return &impl{
		config: d.Config,
		schema: physical.Schema{
			Fields:    fields,
			TimeField: -1,
		},
		table: name,
	}, nil
}
