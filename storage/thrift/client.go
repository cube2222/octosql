package thrift

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cube2222/octosql"
)

type ThriftConnection struct {
	connectionTransport thrift.TTransport
	client              *thrift.TStandardClient
}

var defaultCtx = context.Background()

// Return protocol factory for a given human readable protocol string
func GetProtocolFactory(protocol string) thrift.TProtocolFactory {
	switch protocol {
	case "binary":
		return thrift.NewTBinaryProtocolFactoryDefault()
	case "json":
		return thrift.NewTJSONProtocolFactory()
	case "debug":
		return thrift.NewTDebugProtocolFactory(thrift.NewTJSONProtocolFactory(), "debug")
	default:
		return thrift.NewTBinaryProtocolFactoryDefault()
	}
}

// Run thrift record query within the given RecordStream
// Note that thrift client and connection objects will be cached inside RecordStream to be used multiple times
// This is version of RunClientEx with Thrift settings extracted from the RecordStream and sensible defaults
func RunClient(rs *RecordStream) (CallResult, *ThriftClientError) {
	pr := rs.source.protocol
	protocolFactory := GetProtocolFactory(pr)
	transportFactory := thrift.NewTTransportFactory()
	addr := rs.source.thriftAddr
	return RunClientEx(transportFactory, protocolFactory, addr, rs)
}

// Close Thrift client
func CloseClient(rs *RecordStream) error {
	if rs.thriftConnection != nil {
		err := rs.thriftConnection.connectionTransport.Close()
		if err != nil {
			return err
		}
	}
	rs.thriftConnection = nil
	return nil
}

// Run thrift record query within the given RecordStream
// Note that thrift client and connection objects will be cached inside RecordStream to be used multiple times
func RunClientEx(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, rs *RecordStream) (CallResult, *ThriftClientError) {
	var r CallResult

	if rs.thriftConnection == nil {
		var connectionTransport thrift.TTransport
		var err error
		if rs.source.secure {
			cfg := new(tls.Config)
			cfg.InsecureSkipVerify = true
			connectionTransport, err = thrift.NewTSSLSocket(addr, cfg)
		} else {
			connectionTransport, err = thrift.NewTSocket(addr)
		}
		if err != nil {
			return r, ThriftWrapError(err)
		}
		if connectionTransport == nil {
			return r, ThriftClientNewError("Error opening socket, got nil transport. Is server available?")
		}
		connectionTransport, _ = transportFactory.GetTransport(connectionTransport)
		if connectionTransport == nil {
			return r, ThriftClientNewError("Error from transportFactory.GetTransport(), got nil transport. Is server available?")
		}

		err = connectionTransport.Open()
		if err != nil {
			return r, ThriftWrapError(err)
		}

		client := thrift.NewTStandardClient(protocolFactory.GetProtocol(connectionTransport), protocolFactory.GetProtocol(connectionTransport))
		rs.thriftConnection = &ThriftConnection{
			connectionTransport: connectionTransport,
			client: client,
		}
	}

	result := EmptyCallResult(rs, "")
	var args GetRecordCallArgs

	if !rs.isOpen {
		var openResult = EmptyCallResult(rs, "")
		var openArgs CallArgs
		err := rs.thriftConnection.client.Call(defaultCtx, "openRecords", &openArgs, &openResult)
		if err != nil {
			return EmptyCallResult(rs, ""), ThriftWrapError(err)
		}

		k := openResult.Fields[octosql.NewVariableName(fmt.Sprintf("%s.field_0", rs.alias))]
		rs.streamID = int32(k.GetInt())

		rs.isOpen = true
		rs.isDone = false
	}
	args.StreamID = rs.streamID

	result.CalledMethodName = "getRecord"
	err := rs.thriftConnection.client.Call(defaultCtx, "getRecord", &args, &result)

	if err != nil {
		return r, ThriftWrapError(err)
	}

	return result, nil
}