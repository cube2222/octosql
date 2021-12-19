package plugins

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

	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/functions"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/internal/plugins"
)

type physicalServer struct {
	plugins.UnimplementedDatasourceServer
	database  physical.Database
	socketDir string
	wg        *sync.WaitGroup
}

func (s *physicalServer) GetTable(ctx context.Context, request *plugins.GetTableRequest) (*plugins.GetTableResponse, error) {
	_, schema, err := s.database.GetTable(ctx, request.TableContext.TableName)
	if err != nil {
		return nil, fmt.Errorf("couldn't get table: %w", err)
	}
	return &plugins.GetTableResponse{
		Schema: plugins.NativeSchemaToProto(schema),
	}, nil
}

func (s *physicalServer) PushDownPredicates(ctx context.Context, request *plugins.PushDownPredicatesRequest) (*plugins.PushDownPredicatesResponse, error) {
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

	var knownFunctionsNewPredicates, unknownFunctionsNewPredicates []physical.Expression
	for i := range newPredicates {
		_, ok := repopulateFunctions(newPredicates[i])
		if !ok {
			unknownFunctionsNewPredicates = append(unknownFunctionsNewPredicates, newPredicates[i])
			continue
		}
		knownFunctionsNewPredicates = append(knownFunctionsNewPredicates, newPredicates[i])
	}

	rejected, newPushedDown, changed := impl.PushDownPredicates(knownFunctionsNewPredicates, pushedDownPredicates)
	rejected = append(rejected, unknownFunctionsNewPredicates...)

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

func (s *physicalServer) Materialize(ctx context.Context, request *plugins.MaterializeRequest) (*plugins.MaterializeResponse, error) {
	impl, _, err := s.database.GetTable(ctx, request.TableContext.TableName)
	if err != nil {
		return nil, fmt.Errorf("couldn't get table: %w", err)
	}
	var pushedDownPredicates []physical.Expression
	if err := json.Unmarshal(request.PushedDownPredicates, &pushedDownPredicates); err != nil {
		return nil, fmt.Errorf("couldn't unmarshal pushed down predicates: %w", err)
	}
	for i := range pushedDownPredicates {
		var ok bool
		pushedDownPredicates[i], ok = repopulateFunctions(pushedDownPredicates[i])
		if !ok {
			return nil, fmt.Errorf("received unknown function through predicate pushdown during materialization, this is a bug")
		}
	}

	node, err := impl.Materialize(
		ctx,
		physical.Environment{
			VariableContext: request.VariableContext.ToNativePhysicalVariableContext(),
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
	execServer := &executionServer{
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

func (s *physicalServer) Metadata(context.Context, *plugins.MetadataRequest) (*plugins.MetadataResponse, error) {
	return &plugins.MetadataResponse{
		ApiLevel: plugins.APILevel,
	}, nil
}

func repopulateFunctions(expr physical.Expression) (physical.Expression, bool) {
	funcMap := functions.FunctionMap()

	outOk := true
	out := (&physical.Transformers{
		ExpressionTransformer: func(expr physical.Expression) physical.Expression {
			if expr.ExpressionType != physical.ExpressionTypeFunctionCall {
				return expr
			}
			receivedDescriptor := expr.FunctionCall.FunctionDescriptor

			descriptors, ok := funcMap[expr.FunctionCall.Name]
			if !ok {
				log.Printf("Unknown function, rejecting predicate: %s", expr.FunctionCall.Name)
				outOk = false
				return expr
			}

		descriptorLoop:
			for _, descriptor := range descriptors {
				if len(descriptor.ArgumentTypes) != len(receivedDescriptor.ArgumentTypes) {
					continue descriptorLoop
				}
				if descriptor.Strict != receivedDescriptor.Strict {
					continue descriptorLoop
				}
				if !descriptor.OutputType.Equals(receivedDescriptor.OutputType) {
					continue descriptorLoop
				}
				for j := range descriptor.ArgumentTypes {
					if !descriptor.ArgumentTypes[j].Equals(receivedDescriptor.ArgumentTypes[j]) {
						continue descriptorLoop
					}
				}
				expr.FunctionCall.FunctionDescriptor.TypeFn = descriptor.TypeFn
				expr.FunctionCall.FunctionDescriptor.Function = descriptor.Function
				return expr
			}

			log.Printf("Unknown function signature, rejecting predicate: %s", expr.FunctionCall.Name)
			ok = false
			return expr
		},
	}).TransformExpr(expr)
	return out, outOk
}

type executionServer struct {
	plugins.UnimplementedExecutionDatasourceServer

	node execution.Node
}

func (e *executionServer) Run(request *plugins.RunRequest, stream plugins.ExecutionDatasource_RunServer) error {
	// TODO: Maybe run this asynchronously here, like in JOIN? This way the serialization overhead will be separate from what's underneath.
	if err := e.node.Run(
		execution.ExecutionContext{
			Context:         stream.Context(),
			VariableContext: request.VariableContext.ToNativeExecutionVariableContext(),
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

type ConfigDecoder interface {
	Decode(interface{}) error
}

func Run(dbCreator func(ctx context.Context, configDecoder ConfigDecoder) (physical.Database, error)) {
	debug.SetGCPercent(1000)
	log.Printf("Plugin started.")

	var input plugins.PluginInput
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		log.Fatal("couldn't decode plugin input from JSON: ", err)
	}

	db, err := dbCreator(context.Background(), &input.Config)
	if err != nil {
		log.Fatal("couldn't create database: ", err)
	}

	log.Printf("Listening on %s...", os.Args[1])
	lis, err := net.Listen("unix", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer lis.Close()

	s := grpc.NewServer()

	var wg sync.WaitGroup
	server := &physicalServer{
		database:  db,
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

	log.Printf("Plugin shut down.")
}
