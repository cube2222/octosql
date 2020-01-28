package thrift

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cube2222/octosql"
	"time"
)

var defaultCtx = context.Background()

func NormalizeEntry(v interface{}) (interface{}) {
	if str, ok := v.(string); ok {
		parsed, err := time.Parse(time.RFC3339, str)
		if err == nil {
			v = parsed
		}
	}
	return v
}

type StandardClientWrapper struct {
	seqId        int32
	iprot, oprot thrift.TProtocol
}

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

func RunClient(rs *RecordStream) (CallResult, *ThriftClientError) {

	pr := rs.source.protocol
	protocolFactory := GetProtocolFactory(pr)
	transportFactory := thrift.NewTTransportFactory()
	addr := rs.source.thriftAddr

	return RunDataClient(transportFactory, protocolFactory, addr, rs)
}

func RunDataClient(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, rs *RecordStream) (CallResult, *ThriftClientError) {
	var r CallResult

	var transport thrift.TTransport
	var err error
	if rs.source.secure {
		cfg := new(tls.Config)
		cfg.InsecureSkipVerify = true
		transport, err = thrift.NewTSSLSocket(addr, cfg)
	} else {
		transport, err = thrift.NewTSocket(addr)
	}
	if err != nil {
		return r, ThriftWrapError(err)
	}
	if transport == nil {
		return r, ThriftClientNewError("Error opening socket, got nil transport. Is server available?")
	}
	transport, _ = transportFactory.GetTransport(transport)
	if transport == nil {
		return r, ThriftClientNewError("Error from transportFactory.GetTransport(), got nil transport. Is server available?")
	}

	err = transport.Open()
	if err != nil {
		return r, ThriftWrapError(err)
	}
	defer transport.Close()

	client := thrift.NewTStandardClient(protocolFactory.GetProtocol(transport), protocolFactory.GetProtocol(transport))

	result := EmptyCallResult(rs, -1)
	var args GetRecordCallArgs

	if !rs.isOpen {
		var openResult = EmptyCallResult(rs, -1)
		var openArgs CallArgs
		err = client.Call(defaultCtx, "openRecords", &openArgs, &openResult)
		if err != nil {
			return EmptyCallResult(rs, -1), ThriftWrapError(err)
		}

		k := openResult.Fields[octosql.NewVariableName(fmt.Sprintf("%s.field_0", rs.alias))]
		rs.streamID = int32(k.GetInt())

		rs.isOpen = true
		rs.isDone = false
	}
	args.StreamID = rs.streamID

	result.MetaResultStructID = rs.thriftMeta.GetMetaResultStructID("getRecord")
	err = client.Call(defaultCtx, "getRecord", &args, &result)

	if err != nil {
		return r, ThriftWrapError(err)
	}

	return result, nil
}