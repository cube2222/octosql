package thrift

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cube2222/octosql"
	"strconv"
	"time"
)

var defaultCtx = context.Background()

type CallResult struct {
	Fields map[octosql.VariableName]octosql.Value
	Alias string
}

type CallArgs struct {}


type GetRecordCallArgs struct {
	StreamID int32 `thrift:"streamID,1" db:"streamID" json:"streamID"`
}

func NewGetRecordCallArgs() *GetRecordCallArgs {
	return &GetRecordCallArgs{}
}

func (p *GetRecordCallArgs) GetStreamID() int32 {
	return p.StreamID
}
func (p *GetRecordCallArgs) Read(iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP { break; }
		switch fieldId {
		case 1:
			if fieldTypeId == thrift.I32 {
				if err := p.ReadField1(iprot); err != nil {
					return err
				}
			} else {
				if err := iprot.Skip(fieldTypeId); err != nil {
					return err
				}
			}
		default:
			if err := iprot.Skip(fieldTypeId); err != nil {
				return err
			}
		}
		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

func (p *GetRecordCallArgs)  ReadField1(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadI32(); err != nil {
		return thrift.PrependError("error reading field 1: ", err)
	} else {
		p.StreamID = v
	}
	return nil
}

func (p *GetRecordCallArgs) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("getRecord_args"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
	if p != nil {
		if err := p.writeField1(oprot); err != nil { return err }
	}
	if err := oprot.WriteFieldStop(); err != nil {
		return thrift.PrependError("write field stop error: ", err) }
	if err := oprot.WriteStructEnd(); err != nil {
		return thrift.PrependError("write struct stop error: ", err) }
	return nil
}

func (p *GetRecordCallArgs) writeField1(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("streamID", thrift.I32, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:streamID: ", p), err) }
	if err := oprot.WriteI32(int32(p.StreamID)); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T.streamID (1) field write error: ", p), err) }
	if err := oprot.WriteFieldEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:streamID: ", p), err) }
	return err
}

func (p *GetRecordCallArgs) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("GetRecordCallArgs(%+v)", *p)
}


func (p *CallResult) Read(iprot thrift.TProtocol) error {
	return p.ReadEx(iprot, true)
}

func NormalizeEntry(v interface{}) (interface{}) {
	if str, ok := v.(string); ok {
		parsed, err := time.Parse(time.RFC3339, str)
		if err == nil {
			v = parsed
		}
	}
	return v
}

func (p *CallResult) ReadEx(iprot thrift.TProtocol, addAliasPrefix bool) error {
	if _, err := iprot.ReadStructBegin(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	i := 0
	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP { break; }

		k := "field_" + strconv.Itoa(i)
		i = i + 1
		fieldName := ""

		if addAliasPrefix {
			fieldName = fmt.Sprintf("%s.%s", p.Alias, k)
		} else {
			fieldName = k
		}

		switch fieldTypeId {
		case thrift.STRUCT:
			var p2 CallResult = CallResult{Fields: map[octosql.VariableName]octosql.Value{}, Alias:p.Alias}
			p2.ReadEx(iprot, false)
			for k2, v2 := range p2.Fields {
				p.Fields[octosql.NewVariableName(fmt.Sprintf("%s.%s",fieldName, k2))] = octosql.NormalizeType(v2)
			}
		case thrift.BOOL:
			val, err := iprot.ReadBool()
			if err != nil {
				return err
			}
			p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
		case thrift.BYTE:
			val, err := iprot.ReadByte()
			if err != nil {
				return err
			}
			p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
		case thrift.DOUBLE:
			val, err := iprot.ReadDouble()
			if err != nil {
				return err
			}
			p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
		case thrift.I16:
			val, err := iprot.ReadI16()
			if err != nil {
				return err
			}
			p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
		case thrift.I32:
			val, err := iprot.ReadI32()
			if err != nil {
				return err
			}
			p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
		case thrift.I64:
			val, err := iprot.ReadI64()
			if err != nil {
				return err
			}
			p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
		case thrift.UTF8:
		case thrift.UTF16:
		case thrift.STRING:
			val, err := iprot.ReadString()
			if err != nil {
				return err
			}
			p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
		default:
			if err := iprot.Skip(fieldTypeId); err != nil {
				return err
			}
			break
		}

		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

func (p *CallResult) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("ping_args"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
	if p != nil {
	}
	if err := oprot.WriteFieldStop(); err != nil {
		return thrift.PrependError("write field stop error: ", err) }
	if err := oprot.WriteStructEnd(); err != nil {
		return thrift.PrependError("write struct stop error: ", err) }
	return nil
}


func (p *CallArgs) Read(iprot thrift.TProtocol) error {

	if _, err := iprot.ReadStructBegin(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP { break; }
		if err := iprot.Skip(fieldTypeId); err != nil {
			return err
		}
		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

func (p *CallArgs) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("ping_args"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
	if p != nil {
	}
	if err := oprot.WriteFieldStop(); err != nil {
		return thrift.PrependError("write field stop error: ", err) }
	if err := oprot.WriteStructEnd(); err != nil {
		return thrift.PrependError("write struct stop error: ", err) }
	return nil
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

func RunClient(rs *RecordStream) (CallResult, error) {

	pr := rs.source.protocol
	protocolFactory := GetProtocolFactory(pr)
	transportFactory := thrift.NewTTransportFactory()
	addr := rs.source.thriftAddr

	return RunDataClient(transportFactory, protocolFactory, addr, rs)
}

func RunDataClient(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, rs *RecordStream) (CallResult, error) {
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
		return r, err
	}
	if transport == nil {
		return r, fmt.Errorf("Error opening socket, got nil transport. Is server available?")
	}
	transport, _ = transportFactory.GetTransport(transport)
	if transport == nil {
		return r, fmt.Errorf("Error from transportFactory.GetTransport(), got nil transport. Is server available?")
	}

	err = transport.Open()
	if err != nil {
		return r, err
	}
	defer transport.Close()

	client := thrift.NewTStandardClient(protocolFactory.GetProtocol(transport), protocolFactory.GetProtocol(transport))

	var result CallResult = CallResult{Fields: map[octosql.VariableName]octosql.Value{}, Alias:rs.source.alias}
	var args GetRecordCallArgs

	if !rs.isOpen {
		var openResult CallResult = CallResult{Fields: map[octosql.VariableName]octosql.Value{}, Alias:rs.source.alias}
		var openArgs CallArgs
		err = client.Call(defaultCtx, "openRecords", &openArgs, &openResult)
		if err != nil {
			return CallResult{}, err
		}

		k := openResult.Fields[octosql.NewVariableName(fmt.Sprintf("%s.field_0", rs.alias))]
		var id int64 = 0
		id, err = strconv.ParseInt(k.String(), 10, 32)
		rs.streamID = int32(id)
		if err != nil {
			return CallResult{}, err
		}

		rs.isOpen = true
		rs.isDone = false
	}
	args.StreamID = rs.streamID

	err = client.Call(defaultCtx, "getRecord", &args, &result)

	if err != nil {
		return r, err
	}

	return result, nil
}