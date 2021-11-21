package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/internal/plugins"
)

type Server struct {
	plugins.UnimplementedDatasourceServer
	database  physical.Database
	socketDir string
	wg        *sync.WaitGroup
	// TODO: Possible datasource implementation cache, table_name -> impl
}

func (s *Server) GetTable(ctx context.Context, request *plugins.GetTableRequest) (*plugins.GetTableResponse, error) {
	_, schema, err := s.database.GetTable(ctx, request.TableContext.TableName)
	if err != nil {
		return nil, fmt.Errorf("couldn't get table: %w", err)
	}
	return &plugins.GetTableResponse{
		Schema: plugins.NativeSchemaToProto(schema),
	}, nil
}

func (s *Server) PushDownPredicates(ctx context.Context, request *plugins.PushDownPredicatesRequest) (*plugins.PushDownPredicatesResponse, error) {
	impl, _, err := s.database.GetTable(ctx, request.TableContext.TableName)
	if err != nil {
		return nil, fmt.Errorf("couldn't get table: %w", err)
	}
	var newPredicates, pushedDownPredicates []physical.Expression
	if err := json.Unmarshal(request.NewPredicates, &newPredicates); err != nil {
		return nil, fmt.Errorf("couldn't unmarshal new predicates: %w", err)
	}
	if err := json.Unmarshal(request.PushedDownPredicates, &pushedDownPredicates); err != nil {
		return nil, fmt.Errorf("couldn't unmarshal pushed down predicates: %w", err)
	}
	rejected, newPushedDown, changed := impl.PushDownPredicates(newPredicates, pushedDownPredicates)
	rejectedData, err := json.Marshal(&rejected)
	if err != nil {
		return nil, fmt.Errorf("couldn't marshal rejected predicates: %w", err)
	}
	newPushedDownData, err := json.Marshal(&newPushedDown)
	if err != nil {
		return nil, fmt.Errorf("couldn't marshal new pushed down predicates: %w", err)
	}

	return &plugins.PushDownPredicatesResponse{
		Rejected:   rejectedData,
		PushedDown: newPushedDownData,
		Changed:    changed,
	}, nil
}

func (s *Server) Materialize(ctx context.Context, request *plugins.MaterializeRequest) (*plugins.MaterializeResponse, error) {
	impl, _, err := s.database.GetTable(ctx, request.TableContext.TableName)
	if err != nil {
		return nil, fmt.Errorf("couldn't get table: %w", err)
	}
	var pushedDownPredicates []physical.Expression
	if err := json.Unmarshal(request.PushedDownPredicates, &pushedDownPredicates); err != nil {
		return nil, fmt.Errorf("couldn't unmarshal pushed down predicates: %w", err)
	}
	// TODO: Repopulate function descriptors in pushed down predicates.
	log.Printf("Received schema: %s", spew.Sdump(request.Schema))

	node, err := impl.Materialize(
		ctx,
		physical.Environment{
			VariableContext: &physical.VariableContext{},
		},
		request.Schema.ToNativeSchema(),
		pushedDownPredicates,
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't materialize datasource: %w", err)
	}

	socketName := ulid.MustNew(ulid.Now(), rand.Reader).String() + ".sock"
	socketPath := filepath.Join(s.socketDir, socketName)
	log.Printf("Listening execution on %s", socketPath)
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatal(err)
	}
	execServer := &ExecutionServer{
		node: node,
	}
	s.wg.Add(1)

	grpcServer := grpc.NewServer()
	plugins.RegisterExecutionDatasourceServer(grpcServer, execServer)

	go func() {
		defer s.wg.Done()
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	return &plugins.MaterializeResponse{SocketPath: socketPath}, nil
}

type ExecutionServer struct {
	plugins.UnimplementedExecutionDatasourceServer

	node execution.Node
}

func (e *ExecutionServer) Run(request *plugins.RunRequest, stream plugins.ExecutionDatasource_RunServer) error {
	if err := e.node.Run(
		execution.ExecutionContext{
			// TODO: Pass through proto.
		},
		func(ctx execution.ProduceContext, record execution.Record) error {
			if err := stream.Send(&plugins.RunResponseMessage{
				Record: plugins.NativeRecordToProto(record),
			}); err != nil {
				return fmt.Errorf("couldn't send record to stream: %w", err)
			}
			return nil
		},
		func(ctx execution.ProduceContext, msg execution.MetadataMessage) error {
			if err := stream.Send(&plugins.RunResponseMessage{
				Metadata: plugins.NativeMetadataMessageToProto(msg),
			}); err != nil {
				return fmt.Errorf("couldn't send metadata message to stream: %w", err)
			}
			return nil
		},
	); err != nil {
		return fmt.Errorf("couldn't run node: %w", err)
	}

	return nil
}

type dbMock struct {
}

func (d *dbMock) Run(ctx execution.ExecutionContext, produce execution.ProduceFn, metaSend execution.MetaSendFn) error {
	record := execution.NewRecord(
		[]octosql.Value{
			octosql.NewInt(42),
			octosql.NewString("Kuba"),
			octosql.NewString("Warsaw"),
			octosql.NewInt(3),
			octosql.NewNull(),
			octosql.NewDuration(time.Second * 1000),
			octosql.NewTime(time.Now()),
		},
		false,
		time.Time{},
	)
	for i := 0; i < 10; i++ {
		if err := produce(execution.ProduceFromExecutionContext(ctx), record); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
	return nil
}

func (d *dbMock) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	return &dbMock{}, nil
}

func (d *dbMock) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, pushedDownPredicates, false
}

func (d *dbMock) ListTables(ctx context.Context) ([]string, error) {
	return []string{"table1", "table2"}, nil
}

func (d *dbMock) GetTable(ctx context.Context, name string) (physical.DatasourceImplementation, physical.Schema, error) {
	return d, physical.NewSchema([]physical.SchemaField{
		{
			Name: "age1",
			Type: octosql.Int,
		},
		{
			Name: "name1",
			Type: octosql.String,
		},
		{
			Name: "name2",
			Type: octosql.String,
		},
		{
			Name: "age2",
			Type: octosql.TypeSum(octosql.Int, octosql.String),
		},
		{
			Name: "name3",
			Type: octosql.TypeSum(octosql.String, octosql.Null),
		},
		{
			Name: "some_duration",
			Type: octosql.Duration,
		},
		{
			Name: "now",
			Type: octosql.Time,
		},
	}, -1), nil
}

// TODO: A może robić dodatkowy serwer per execution.Node i mieć tłumaczenie o wiele bardziej 1-1?
// To czyni wszystko bardziej stateful, ale w końcu optymalizujemy pod lokalne działanie, więc whatever.
// I każdy ze swoim unix socketem.

func main() {
	debug.SetGCPercent(800)
	log.Printf("I'm on!")

	log.Printf("Listening on %s", os.Args[1])
	lis, err := net.Listen("unix", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer lis.Close()

	s := grpc.NewServer()

	var wg sync.WaitGroup
	server := &Server{
		database:  &dbMock{},
		socketDir: filepath.Dir(os.Args[1]),
		wg:        &wg,
	}

	plugins.RegisterDatasourceServer(s, server)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()
	wg.Wait()

	log.Printf("I'm gone!")
}
