package executor

import (
	"context"
	"encoding/json"
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
	runningConns   []*grpc.ClientConn
	runningPlugins []*exec.Cmd
}

func (e *PluginExecutor) RunPlugin(ctx context.Context, name string) (*Database, error) {
	tmpDir, err := os.MkdirTemp("./plugins/tmp", "octosql-")
	if err != nil {
		return nil, fmt.Errorf("couldn't create tempdir: %w", err)
	}

	socketLocation := filepath.Join(tmpDir, "root.sock")
	log.Printf("plugin socket: %s", socketLocation)
	absolute, err := filepath.Abs(socketLocation)
	if err != nil {
		return nil, fmt.Errorf("couldn't get absolute path to plugin socket: %w", err)
	}
	log.Printf("absolute plugin socket: %s", absolute)

	cmd := exec.CommandContext(ctx, "plugins/plugin/plugin", absolute)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("couldn't start plugin: %w", err)
	}
	// cmd.Stdout = text.NewIndentWriter(os.Stdout, []byte(fmt.Sprintf("[%s] ", name)))
	// cmd.Stderr = text.NewIndentWriter(os.Stderr, []byte(fmt.Sprintf("[%s] ", name)))
	// TODO: Doesn't work.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

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
		fmt.Sprintf("unix://%s", absolute),
		grpc.WithBlock(),
		grpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to plugin: %w", err)
	}
	e.runningConns = append(e.runningConns, conn)
	cli := plugins.NewDatasourceClient(conn)

	return &Database{
		cli: cli,
	}, nil
}

func (e *PluginExecutor) Close() error {
	for i := range e.runningConns {
		if err := e.runningConns[i].Close(); err != nil {
			return fmt.Errorf("couldn't close connection: %w", err)
		}
	}
	for i := range e.runningPlugins {
		if err := e.runningPlugins[i].Process.Kill(); err != nil {
			return fmt.Errorf("couldn't close process: %w", err)
		}
	}
	return nil
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
	newPredicatesBytes, err := json.Marshal(&newPredicates)
	if err != nil {
		panic(fmt.Errorf("couldn't marshal new predicates to JSON: %w", err))
	}
	// TODO: Filter out predicates with subqueries.
	// TODO: In the future, if the subquery doesn't depend on the record, we could precalculate it and pass it down as a variable.
	pushedDownPredicatesBytes, err := json.Marshal(&pushedDownPredicates)
	if err != nil {
		panic(fmt.Errorf("couldn't marshal pushed down predicates to JSON: %w", err))
	}
	res, err := p.cli.PushDownPredicates(context.Background(), &plugins.PushDownPredicatesRequest{
		TableContext: &plugins.TableContext{
			TableName: p.table,
		},
		NewPredicates:        newPredicatesBytes,
		PushedDownPredicates: pushedDownPredicatesBytes,
	})
	if err != nil {
		panic(fmt.Errorf("couldn't push down predicates to plugin: %w", err))
	}
	var outRejected, outPushedDown []physical.Expression
	if err := json.Unmarshal(res.Rejected, &outRejected); err != nil {
		panic(fmt.Errorf("couldn't unmarshal rejected predicates from JSON: %w", err))
	}
	if err := json.Unmarshal(res.PushedDown, &outPushedDown); err != nil {
		panic(fmt.Errorf("couldn't unmarshal pushed down predicates from JSON: %w", err))
	}
	return outRejected, outPushedDown, res.Changed
}
