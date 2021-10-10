package postgres

import (
	"context"
	"fmt"
	"log"
	"strings"

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

func connect(config *Config) (*pgx.ConnPool, error) {
	db, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:      config.Host,
			Port:      uint16(config.Port),
			User:      config.User,
			Database:  config.Database,
			Password:  config.Password,
			TLSConfig: nil,
			// Logger:    &testLogger{},
		},
		MaxConnections: 128,
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
	conn, err := db.Acquire()
	if err != nil {
		return nil, fmt.Errorf("couldn't acquire connection from pool: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("couldn't ping database: %w", err)
	}
	db.Release(conn)
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

	rows, err := db.QueryEx(ctx, "SELECT column_name, udt_name, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", nil, name)
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
		t := getOctoSQLType(descriptions[i][1])
		if descriptions[i][2] == "YES" {
			t = octosql.TypeSum(t, octosql.Null)
		}
		fields[i] = physical.SchemaField{
			Name: descriptions[i][0],
			Type: t,
		}
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

func getOctoSQLType(typename string) octosql.Type {
	if strings.HasPrefix(typename, "_") {
		elementType := getOctoSQLType(typename[1:])

		return octosql.Type{
			TypeID: octosql.TypeIDList,
			List: struct {
				Element *octosql.Type
			}{Element: &elementType},
		}
	}

	switch typename {
	case "int", "int2", "int4", "int8":
		return octosql.Int
	case "text", "character", "varchar", "bpchar":
		return octosql.String
	case "real", "numeric", "float4", "float8":
		return octosql.Float
	case "bool", "boolean":
		return octosql.Boolean
	case "timestamptz", "timestamp", "timetz", "time":
		return octosql.Time
	case "jsonb":
		// TODO: Handle me better.
		return octosql.String
	case "bytea":
		// TODO: Handle me better.
		return octosql.String
	default:
		log.Fatalf("unsupported postgres field type: %s", typename)
		return octosql.Null

		// TODO: Support time.
		// TODO: Support more types.
	}
}
