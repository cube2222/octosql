package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/debug"
	"sync"

	"google.golang.org/grpc"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/internal/plugins"
)

type Server struct {
	plugins.UnimplementedDatasourceServer
	database physical.Database
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
		return nil, fmt.Errorf("couldn't get table")
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

func (*Server) Materialize(context.Context, *plugins.MaterializeRequest) (*plugins.MaterializeResponse, error) {
	panic("implement me")
}

type dbMock struct {
}

func (d *dbMock) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	panic("can't materialize")
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
			Name: "name",
			Type: octosql.String,
		},
		{
			Name: "age",
			Type: octosql.TypeSum(octosql.Int, octosql.String),
		},
	}, -1), nil
}

func main() {
	debug.SetGCPercent(800)

	lis, err := net.Listen("unix", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer lis.Close()

	s := grpc.NewServer()

	server := &Server{
		database: &dbMock{},
	}

	plugins.RegisterDatasourceServer(s, server)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()
	wg.Wait()
}
