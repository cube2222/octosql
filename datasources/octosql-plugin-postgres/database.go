package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins"
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

func connect(config *Config) (*pgx.ConnPool, error) {
	pgxConfig := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:      config.Host,
			Port:      uint16(config.Port),
			User:      config.User,
			Database:  config.Database,
			Password:  config.Password,
			TLSConfig: nil,
		},
		MaxConnections: 128,
	}
	if os.Getenv("OCTOSQL_POSTGRES_QUERY_LOGGING") == "1" {
		pgxConfig.ConnConfig.Logger = &testLogger{}
	}
	db, err := pgx.NewConnPool(pgxConfig)
	if err != nil {
		return nil, fmt.Errorf("couldn't open database: %w", err)
	}
	return db, nil
}

func Creator(ctx context.Context, configUntyped plugins.ConfigDecoder) (physical.Database, error) {
	var cfg Config
	if err := configUntyped.Decode(&cfg); err != nil {
		return nil, err
	}
	return &Database{
		Config: &cfg,
	}, nil
}

type Database struct {
	Config *Config
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	panic("implement me")
}

func (d *Database) GetTable(ctx context.Context, name string) (physical.DatasourceImplementation, physical.Schema, error) {
	db, err := connect(d.Config)
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't connect to database: %w", err)
	}

	rows, err := db.QueryEx(ctx, "SELECT column_name, udt_name, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", nil, name)
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't describe table: %w", err)
	}

	var descriptions [][]string
	for rows.Next() {
		desc := make([]string, 3)
		if err := rows.Scan(&desc[0], &desc[1], &desc[2]); err != nil {
			return nil, physical.Schema{}, fmt.Errorf("couldn't scan table description: %w", err)
		}
		descriptions = append(descriptions, desc)
	}
	if len(descriptions) == 0 {
		return nil, physical.Schema{}, fmt.Errorf("table %s does not exist", name)
	}

	fields := make([]physical.SchemaField, 0, len(descriptions))
	for i := range descriptions {
		t, ok := getOctoSQLType(descriptions[i][1])
		if !ok {
			continue
		}
		if descriptions[i][2] == "YES" {
			t = octosql.TypeSum(t, octosql.Null)
		}
		fields = append(fields, physical.SchemaField{
			Name: descriptions[i][0],
			Type: t,
		})
	}

	return &impl{
			config: d.Config,
			table:  name,
		},
		physical.Schema{
			Fields:    fields,
			TimeField: -1,
		},
		nil
}

func getOctoSQLType(typename string) (octosql.Type, bool) {
	if strings.HasPrefix(typename, "_") {
		elementType, ok := getOctoSQLType(typename[1:])
		if !ok {
			return octosql.Type{}, false
		}

		return octosql.Type{
			TypeID: octosql.TypeIDList,
			List: struct {
				Element *octosql.Type
			}{Element: &elementType},
		}, true
	}

	switch typename {
	case "int", "int2", "int4", "int8":
		return octosql.Int, true
	case "text", "character", "varchar", "bpchar":
		return octosql.String, true
	case "real", "numeric", "float4", "float8":
		return octosql.Float, true
	case "bool", "boolean":
		return octosql.Boolean, true
	case "timestamptz", "timestamp", "timetz", "time":
		return octosql.Time, true
	case "jsonb":
		// TODO: Handle me better.
		return octosql.String, true
	case "bytea":
		// TODO: Handle me better.
		return octosql.String, true
	default:
		log.Printf("unsupported postgres field type '%s' - ignoring column", typename)
		return octosql.Null, false

		// TODO: Support more types.
	}
}
