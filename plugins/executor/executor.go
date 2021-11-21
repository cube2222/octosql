package executor

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"google.golang.org/grpc"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/internal/plugins"
)

type PluginExecutor struct {
	runningPlugins []*exec.Cmd
}

func (e *PluginExecutor) RunPlugin(ctx context.Context) (*Database, error) {
	tmpDir, err := os.MkdirTemp("./plugins/tmp", "octosql-")
	if err != nil {
		return nil, fmt.Errorf("couldn't create tempdir: %w", err)
	}

	socketLocation := filepath.Join(tmpDir, "root.sock")
	log.Printf("plugin socket: %s", socketLocation)

	cmd := exec.CommandContext(ctx, "plugins/plugin/plugin", socketLocation)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("couldn't start plugin: %w", err)
	}

	e.runningPlugins = append(e.runningPlugins, cmd)

	// TODO: Now we will do polling for the socket, change to fsnotify later.
	// watcher, err := fsnotify.NewWatcher()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// watcher.Add(tmpDir)
	// defer watcher.Close()

	for {
		_, err := os.Stat(socketLocation)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("couldn't check if plugin socket exists: %w", err)
		}
		break
	}

	conn, err := grpc.Dial(
		fmt.Sprintf("unix://%s", socketLocation),
		grpc.WithBlock(),
		grpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to plugin: %w", err)
	}
	cli := plugins.NewDatasourceClient(conn)

	return &Database{
		cli: cli,
	}, nil
}

type Database struct {
	cli plugins.DatasourceClient
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	panic("implement me")
}

func (d *Database) GetTable(ctx context.Context, name string) (physical.DatasourceImplementation, physical.Schema, error) {
	res, err := d.cli.GetTable(ctx, &plugins.GetTableRequest{
		TableContext: &plugins.TableContext{
			TableName: name,
		},
	})
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't get table from plugin: %w", err)
	}

	return &PhysicalDatasource{
			cli:   d.cli,
			table: name,
		},
		res.Schema.ToNativeSchema(),
		nil
}

type PhysicalDatasource struct {
	cli   plugins.DatasourceClient
	table string
}

func (p *PhysicalDatasource) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	panic("implement me")
}

func (p *PhysicalDatasource) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	panic("implement me")
}
