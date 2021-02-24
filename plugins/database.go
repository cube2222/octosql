package plugins

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/types"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

type DatabasePlugin struct {
	db physical.Database
}

func (d *DatabasePlugin) GetPluginMetadata(ctx context.Context, request *GetPluginMetadataRequest) (*GetPluginMetadataResponse, error) {
	return &GetPluginMetadataResponse{
		SupportedFunctions:       nil,
		SupportedExpressionTypes: nil,
	}, nil
}

func (d *DatabasePlugin) GetSchema(ctx context.Context, request *GetSchemaRequest) (*Schema, error) {
	table, err := d.db.GetTable(ctx, request.Table)
	if err != nil {
		return nil, fmt.Errorf("couldn't get table '%s': %w", table, err)
	}
	schema, err := table.Schema()
	if err != nil {
		return nil, fmt.Errorf("couldn't get table '%s' schema: %w", table, err)
	}
	fields := make([]*SchemaField, len(schema.Fields))
	for i := range schema.Fields {
		fields[i] = &SchemaField{
			Name: schema.Fields[i].Name,
			Type: TypeToProto(schema.Fields[i].Type),
		}
	}
	return &Schema{
		Fields:    fields,
		TimeField: int64(schema.TimeField),
	}, nil
}

func (d *DatabasePlugin) Run(request *RunRequest, server Plugin_RunServer) error {
	ctx := server.Context()
	table, err := d.db.GetTable(ctx, request.Table)
	if err != nil {
		return fmt.Errorf("couldn't get table '%s': %w", table, err)
	}

	// TODO: Add variables and pushed down predicates.
	node, err := table.Materialize(ctx, physical.Environment{}, nil)
	if err != nil {
		return fmt.Errorf("couldn't materialize table '%s': %w", table, err)
	}

	return node.Run(execution.ExecutionContext{
		Context: ctx,
		// TODO: Add variables and pushed down predicates.
		VariableContext: nil,
	}, func(ctx execution.ProduceContext, record execution.Record) error {
		return server.Send(&RunMessage{
			Content: &RunMessage_Record{
				Record: RecordToProto(record),
			},
		})
	}, func(ctx execution.ProduceContext, msg execution.MetadataMessage) error {
		t, err := types.TimestampProto(msg.Watermark)
		if err != nil {
			panic(fmt.Errorf("couldn't convert timestamp to proto: %w", err))
		}

		return server.Send(&RunMessage{
			Content: &RunMessage_MetadataMessage{
				MetadataMessage: &MetadataMessage{
					MetadataMessage: &MetadataMessage_Watermark{
						Watermark: t,
					},
				},
			},
		})
	})
}
