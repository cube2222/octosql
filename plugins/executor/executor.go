package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/Masterminds/semver"
	"github.com/kr/text"
	"github.com/mitchellh/go-homedir"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logs"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/internal/plugins"
)

var tmpPluginsDir = func() string {
	if value, ok := os.LookupEnv("OCTOSQL_PLUGIN_TMP_DIR"); ok {
		return value
	}
	dir, err := homedir.Dir()
	if err != nil {
		log.Fatalf("couldn't get user home directory: %s", err)
	}
	return filepath.Join(dir, ".octosql/tmp/plugins")
}()

type Manager interface {
	GetPluginBinaryPath(name config.PluginReference, version *semver.Version) (string, error)
}

type PluginExecutor struct {
	Manager Manager

	runningConns   []*grpc.ClientConn
	runningPlugins []*exec.Cmd
	tmpDirs        []string
	done           []chan error
}

func (e *PluginExecutor) RunPlugin(ctx context.Context, pluginRef config.PluginReference, databaseName string, version *semver.Version, config yaml.Node) (*Database, error) {
	if err := os.MkdirAll(tmpPluginsDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("couldn't create tmp directory %s: %w", tmpPluginsDir, err)
	}

	tmpDir, err := os.MkdirTemp(tmpPluginsDir, pluginRef.Repository+"$"+pluginRef.Name)
	if err != nil {
		return nil, fmt.Errorf("couldn't create tempdir: %w", err)
	}

	socketLocation := filepath.Join(tmpDir, "root.sock")
	absolute, err := filepath.Abs(socketLocation)
	if err != nil {
		return nil, fmt.Errorf("couldn't get absolute path to plugin socket: %w", err)
	}

	input, err := json.Marshal(&plugins.PluginInput{
		Config: config,
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't encode plugin input to JSON: %w", err)
	}

	binaryPath, err := e.Manager.GetPluginBinaryPath(pluginRef, version)
	if err != nil {
		return nil, fmt.Errorf("couldn't find binary location for plugin %s: %w", pluginRef.String(), err)
	}

	cmd := exec.CommandContext(ctx, binaryPath, absolute)
	cmd.Stdin = bytes.NewReader(input)
	cmd.Stdout = text.NewIndentWriter(logs.Output, []byte(fmt.Sprintf("[plugin][%s][%s] ", pluginRef.String(), databaseName)))
	cmd.Stderr = text.NewIndentWriter(logs.Output, []byte(fmt.Sprintf("[plugin][%s][%s] ", pluginRef.String(), databaseName)))
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("couldn't start plugin: %w", err)
	}

	e.runningPlugins = append(e.runningPlugins, cmd)
	e.tmpDirs = append(e.tmpDirs, tmpDir)
	e.done = append(e.done, make(chan error, 1))

	go func(i int) {
		if err := cmd.Wait(); err != nil {
			e.done[i] <- err
		}
	}(len(e.done) - 1)

	for {
		_, err := os.Lstat(socketLocation)
		if os.IsNotExist(err) {
			select {
			case cmdOutErr := <-e.done[len(e.done)-1]:
				e.runningPlugins = e.runningPlugins[:len(e.runningPlugins)-1]
				e.tmpDirs = e.tmpDirs[:len(e.tmpDirs)-1]
				e.done = e.done[:len(e.done)-1]
				return nil, fmt.Errorf("plugin exited prematurely, error: %s", cmdOutErr)
			default:
			}
			time.Sleep(time.Millisecond)
			continue
		} else if err != nil {
			return nil, fmt.Errorf("couldn't check if plugin socket exists: %w", err)
		}
		break
	}

	conn, err := grpc.DialContext(
		ctx,
		fmt.Sprintf("unix:///%s", absolute),
		grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			// This is necessary for sockets to properly work on Windows.
			return (&net.Dialer{}).DialContext(ctx, "unix", absolute)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to plugin: %w", err)
	}
	e.runningConns = append(e.runningConns, conn)
	cli := plugins.NewDatasourceClient(conn)

	res, err := cli.Metadata(ctx, &plugins.MetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("couldn't get metadata from plugin: %w", err)
	}
	if res.ApiLevel < plugins.APILevel {
		return nil, fmt.Errorf("plugin API Level is %d, lower than current %d, this means the plugin was built for an older version of OctoSQL and is not supported anymore - please try installing a newer version by running `octosql plugin install %s`", res.ApiLevel, plugins.APILevel, pluginRef.String())
	}

	return &Database{
		ctx: ctx,
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
		if err := e.runningPlugins[i].Process.Kill(); err != nil && err != os.ErrProcessDone {
			return fmt.Errorf("couldn't close process: %w", err)
		}
	}
	for i := range e.done {
		<-e.done[i]
	}
	for i := range e.tmpDirs {
		if err := os.RemoveAll(e.tmpDirs[i]); err != nil {
			return fmt.Errorf("couldn't remove temporary directory '%s': %w", e.tmpDirs[i], err)
		}
	}
	return nil
}

type Database struct {
	ctx context.Context
	cli plugins.DatasourceClient
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	panic("implement me")
}

func (d *Database) GetTable(ctx context.Context, name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
	tableContext := &plugins.TableContext{
		TableName: name,
		Options:   options,
	}

	res, err := d.cli.GetTable(ctx, &plugins.GetTableRequest{
		TableContext: tableContext,
	})
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't get table from plugin: %w", err)
	}

	return &PhysicalDatasource{
			ctx:          d.ctx,
			cli:          d.cli,
			tableContext: tableContext,
		},
		res.Schema.ToNativeSchema(),
		nil
}

type PhysicalDatasource struct {
	ctx          context.Context
	cli          plugins.DatasourceClient
	tableContext *plugins.TableContext
}

func (p *PhysicalDatasource) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	var newPredicatesSerializable, newPredicatesNotSerializable []physical.Expression
	for i := range newPredicates {
		if !containsSubquery(newPredicates[i]) {
			newPredicatesSerializable = append(newPredicatesSerializable, newPredicates[i])
		} else {
			newPredicatesNotSerializable = append(newPredicatesNotSerializable, newPredicates[i])
		}
	}
	// TODO: If the subquery doesn't depend on the record, we could precalculate it and pass it down as a variable.

	newPredicatesBytes, err := json.Marshal(&newPredicatesSerializable)
	if err != nil {
		panic(fmt.Errorf("couldn't marshal new predicates to JSON: %w", err))
	}
	pushedDownPredicatesBytes, err := json.Marshal(&pushedDownPredicates)
	if err != nil {
		panic(fmt.Errorf("couldn't marshal pushed down predicates to JSON: %w", err))
	}
	res, err := p.cli.PushDownPredicates(p.ctx, &plugins.PushDownPredicatesRequest{
		TableContext:         p.tableContext,
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
	for i := range outRejected {
		var ok bool
		outRejected[i], ok = plugins.RepopulatePhysicalExpressionFunctions(outRejected[i])
		if !ok {
			panic(fmt.Errorf("received unknown function from plugin, please check logs for more"))
		}
	}
	for i := range outPushedDown {
		var ok bool
		outPushedDown[i], ok = plugins.RepopulatePhysicalExpressionFunctions(outPushedDown[i])
		if !ok {
			panic(fmt.Errorf("received unknown function from plugin, please check logs for more"))
		}
	}
	return append(outRejected, newPredicatesNotSerializable...), outPushedDown, res.Changed
}

func containsSubquery(expr physical.Expression) (out bool) {
	(&physical.Transformers{
		ExpressionTransformer: func(expr physical.Expression) physical.Expression {
			if expr.ExpressionType == physical.ExpressionTypeQueryExpression {
				out = true
			}
			return expr
		},
	}).TransformExpr(expr)
	return
}

func (p *PhysicalDatasource) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	pushedDownPredicatesBytes, err := json.Marshal(&pushedDownPredicates)
	if err != nil {
		return nil, fmt.Errorf("couldn't marshal pushed down predicates to JSON: %w", err)
	}
	res, err := p.cli.Materialize(ctx, &plugins.MaterializeRequest{
		TableContext:         p.tableContext,
		Schema:               plugins.NativeSchemaToProto(schema),
		PushedDownPredicates: pushedDownPredicatesBytes,
		VariableContext:      plugins.NativePhysicalVariableContextToProto(env.VariableContext),
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't materialize plugin datasource: %w", err)
	}

	// TODO: Change to fsnotify.
	for {
		_, err := os.Lstat(res.SocketPath)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("couldn't check if executing plugin socket exists: %w", err)
		}
		break
	}

	conn, err := grpc.Dial(
		fmt.Sprintf("unix:///%s", res.SocketPath),
		grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			// This is necessary for sockets to properly work on Windows.
			return (&net.Dialer{}).DialContext(ctx, "unix", res.SocketPath)
		}),
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
