package thrift

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage/thrift/analyzer"
	"strconv"
)

type CallResult struct {
	Fields map[octosql.VariableName]octosql.Value
	Alias string
	RecordStream *RecordStream
	ThriftMeta *analyzer.ThriftMeta
	MetaResultStructID int
}

func EmptyCallResult(rs *RecordStream, MetaResultStructID int) CallResult {
	return CallResult{
		Fields: map[octosql.VariableName]octosql.Value{},
		Alias:  rs.source.alias,
		RecordStream: rs,
		ThriftMeta: rs.thriftMeta,
		MetaResultStructID: MetaResultStructID,
	}
}

func (p *CallResult) Read(iprot thrift.TProtocol) error {
	currentFieldID := -1
	return p.ReadEx(iprot, true, true, &currentFieldID, "")
}

func (p *CallResult) ReadEx(iprot thrift.TProtocol, addAliasPrefix bool, isRoot bool, currentFieldID *int, currentPrefix string) error {

	if isRoot && addAliasPrefix {
		currentPrefix = fmt.Sprintf("%s%s.", currentPrefix, p.Alias)
	}

	if _, err := iprot.ReadStructBegin(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	i := 0
	for {
		*currentFieldID++
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP { break; }

		k := "field_" + strconv.Itoa(i)
		i = i + 1
		fieldName := ""
		recPrefix := ""

		if p.MetaResultStructID > -1 && *currentFieldID >= 1 {
			fieldName = p.ThriftMeta.GetFieldName(p.MetaResultStructID, *currentFieldID-1)
			recPrefix = fmt.Sprintf("%s%s.",currentPrefix, fieldName)
		}

		if len(fieldName) == 0 {
			fieldName = k
			recPrefix = currentPrefix
		}

		fieldName = currentPrefix + fieldName

		switch fieldTypeId {
		case thrift.STRUCT:
			var p2 CallResult = EmptyCallResult(p.RecordStream, p.MetaResultStructID)
			p2.ReadEx(iprot, false, false, currentFieldID, recPrefix)
			for k2, v2 := range p2.Fields {
				p.Fields[k2] = octosql.NormalizeType(v2)
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