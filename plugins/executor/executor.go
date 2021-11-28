package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/kr/text"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/internal/plugins"
)

type Manager interface {
	GetBinaryPath(name string) (string, error)
}

type PluginExecutor struct {
	Manager Manager

	runningConns   []*grpc.ClientConn
	runningPlugins []*exec.Cmd
}

func (e *PluginExecutor) RunPlugin(ctx context.Context, pluginType, databaseName string, config yaml.Node) (*Database, error) {
	tmpRootDir := "/Users/jakub/.octosql/tmp/plugins"
	if err := os.MkdirAll(tmpRootDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("couldn't create tmp directory %s: %w", tmpRootDir, err)
	}

	tmpDir, err := os.MkdirTemp(tmpRootDir, fmt.Sprintf("%s", pluginType))
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

	input, err := json.Marshal(&plugins.PluginInput{
		Config: config,
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't encode plugin input to JSON: %w", err)
	}

	binaryPath, err := e.Manager.GetBinaryPath(pluginType)
	if err != nil {
		return nil, fmt.Errorf("couldn't find binary location for plugin %s: %w", pluginType, err)
	}

	cmd := exec.CommandContext(ctx, binaryPath, absolute)
	cmd.Stdin = bytes.NewReader(input)
	cmd.Stdout = text.NewIndentWriter(os.Stdout, []byte(fmt.Sprintf("[plugin][%s][%s] ", pluginType, databaseName)))
	cmd.Stderr = text.NewIndentWriter(os.Stderr, []byte(fmt.Sprintf("[plugin][%s][%s] ", pluginType, databaseName)))
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

func (p *PhysicalDatasource) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	pushedDownPredicatesBytes, err := json.Marshal(&pushedDownPredicates)
	if err != nil {
		return nil, fmt.Errorf("couldn't marshal pushed down predicates to JSON: %w", err)
	}
	res, err := p.cli.Materialize(ctx, &plugins.MaterializeRequest{
		TableContext: &plugins.TableContext{
			TableName: p.table,
		},
		Schema:               plugins.NativeSchemaToProto(schema),
		PushedDownPredicates: pushedDownPredicatesBytes,
		VariableContext:      plugins.NativePhysicalVariableContextToProto(env.VariableContext),
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't materialize plugin datasource: %w", err)
	}

	// TODO: Change to fsnotify.
	for {
		_, err := os.Stat(res.SocketPath)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("couldn't check if executing plugin socket exists: %w", err)
		}
		break
	}

	conn, err := grpc.Dial(
		fmt.Sprintf("unix://%s", res.SocketPath),
		grpc.WithBlock(),
		grpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to executing plugin: %w", err)
	}
	cli := plugins.NewExecutionDatasourceClient(conn)

	return &ExecutionDatasource{
		cli: cli,
	}, nil
}

type ExecutionDatasource struct {
	cli plugins.ExecutionDatasourceClient
}

func (e *ExecutionDatasource) Run(ctx execution.ExecutionContext, produce execution.ProduceFn, metaSend execution.MetaSendFn) error {
	msgStream, err := e.cli.Run(
		ctx.Context,
		&plugins.RunRequest{
			VariableContext: plugins.NativeExecutionVariableContextToProto(ctx.VariableContext),
		},
	)
	if err != nil {
		return fmt.Errorf("couldn't get message stream from plugin: %w", err)
	}
	produceCtx := execution.ProduceFromExecutionContext(ctx)
	for {
		msg, err := msgStream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("couldn't get next message from plugin message stream: %w", err)
		}
		if msg.Record != nil {
			if err := produce(produceCtx, msg.Record.ToNativeRecord()); err != nil {
				// TODO: Now we can't bubble up the error back to the plugin, which is different than with other streams. Is this a problem?
				return fmt.Errorf("couldn't produce record: %w", err)
			}
		} else {
			if err := metaSend(produceCtx, msg.Metadata.ToNativeMetadataMessage()); err != nil {
				return fmt.Errorf("couldn't produce metadata message: %w", err)
			}
		}
	}
	return nil
}
